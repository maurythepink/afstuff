#! /usr/bin/env python


import argparse
import re
from pathlib import Path
import json
from enum import Enum, auto
from typing import Union, Optional, Iterator, Generator, Callable


EVIDENCE_OF_EXECUTION = "message contains 'Prefetch {' or message contains 'AppCompatCache' or \
message contains 'typed the following cmd' or \
message contains 'CMD typed' or \
message contains 'Last run' or \
message contains 'RunMRU' or \
message contains 'MUICache' or \
message contains 'UserAssist key' or \
message contains 'Time of Launch' or \
message contains 'Prefetch' or \
message contains 'SHIMCACHE' or \
message contains 'Scheduled' or \
message contains '.pf' or \
message contains 'was run' or \
message contains 'UEME_' or \
message contains '[PROCESS]'"


def regex_options_string(string_list: list[str]) -> str:
    return '|'.join((f'({_s})' for _s in string_list))


class FileType(Enum):
    json = auto()
    l2tcsv = auto()
    # xlsx = auto()
    # l2ttln = auto()
    # elastic = auto()
    #4n6time_sqlite = auto()
    # kml = auto()
    dynamic = auto()
    # rawpy = auto()
    # tln = auto()
    json_line = auto()

    @staticmethod
    def names_string() -> str:
        return f'{", ".join((_type.name for _type in FileType))}'

    @staticmethod
    def infer_form_file_name(path: Union[str, Path]):
        _file_name = path if type(path) is str else str(path)
        for _t in FileType:
            if _file_name[-(len(_t.name) + 1):] == f'.{_t.name}':
                return _t
        raise ValueError(f'Cannot recognize the file type. Use the -t option to specify one, or change the extension '
                         f'in one from: {FileType.names_string()}.')

    @staticmethod
    def from_string(type_name: str):
        for _t in FileType:
            if type_name == _t.name:
                return _t
        raise ValueError(f'Source type ({type_name}) not recognized. Choose one from: {FileType.names_string()}')

    def make_json_data(self, source_path: Path) -> Optional[Iterator[dict]]:
        with source_path.open('r') as source_file:
            if self is FileType.json:
                _row_json_data = json.loads(source_file.read())
                return (_value for _, _value in _row_json_data.items())
            elif self is FileType.l2tcsv or self is FileType.dynamic:
                _lines = source_file.readlines()
                _keys = _lines[0].split(',')
                for _line in _lines[1:]:
                    yield {_keys[_i]: _line.split(',')[_i] for _i in range(len(_keys))}
        return None


class DataSet:
    def __init__(self, source_path: Path, file_type: FileType):
        self.file_type = file_type
        self.source_path = source_path

    @property
    def json_data(self) -> Iterator[dict]:
        return self.file_type.make_json_data(self.source_path)

    @property
    def keys(self) -> list[str]:
        for _item in self.json_data:
            return list(_item.keys())


ap = argparse.ArgumentParser(prog='AF Stuff', usage='usage')
ap.add_argument('source',
                type=str,
                help=f'Source file. Available types: {FileType.names_string()}. Types can be '
                     f'defined naming the extensions as the chosen type (es: dump.json_line)',
                action='store')
ap.add_argument('-t --source-type',
                dest='source_type',
                type=str,
                help=f'Specify the source type ({FileType.names_string()})')
ap.add_argument('-k --include-keys',
                dest='include_keys',
                type=str,
                action='store',
                default='ALL',
                help='comma-separated keys to include in the output. Es: \'source,message,parser\'. If argument '
                     '\'LIST\' is passed, it returns the list of available keys for the passed file.')
ap.add_argument('--evidence-of-execution',
                action='store_true',
                dest='evidence_of_execution',
                help='made filter')
ap.add_argument('-f --filter',
                dest='filter_string',
                type=str,
                action='store',
                help='filter string, like \'message contains \"https:\"\', \'ANY contains "google"\'')


class ParsedArgs:
    def __init__(self):
        _parsed_args = ap.parse_args()
        _source_file = Path(_parsed_args.source)
        if not _source_file.exists():
            raise ValueError(f'File at path \'{_source_file}\' does not exist!')
        self.source_file = _source_file
        self._include_keys: str = _parsed_args.include_keys
        _source_type = _parsed_args.source_type
        if _source_type is not None:
            self.source_type = FileType.from_string(_source_type)
        else:
            self.source_type = FileType.infer_form_file_name(self.source_file)
        self.filter_string = _parsed_args.filter_string
        self.evidence_of_execution = _parsed_args.evidence_of_execution

    def included_keys(self, data_set: DataSet) -> list[str]:
        if self._include_keys == 'ALL':
            return data_set.keys
        else:
            _keys = self._include_keys.split(',')
            if not set(_keys).issubset(set(data_set.keys)):
                raise ValueError('Wrong list of keys!')
            return _keys


args = ParsedArgs()
data = DataSet(args.source_file, args.source_type)


class Operation(Enum):
    CONTAINS = auto()
    IS = auto()
    EQ = auto()
    REGEX = auto()
    IREGEX = auto()

    @property
    def symbol(self) -> str:
        if self is Operation.CONTAINS:
            return 'contains'
        elif self is Operation.IS:
            return 'is'
        elif self is Operation.EQ:
            return '=='
        elif self is Operation.REGEX:
            return 'regex'
        elif self is Operation.IREGEX:
            return 'iregex'
        else:
            raise ValueError('Missing!!')

    @staticmethod
    def from_symbol(symbol: str):
        for _op in Operation:
            if symbol == _op.symbol:
                return _op
        raise ValueError(f'The symbol \'{symbol}\' is not valid for operations. Valid symbols: '
                         f'{", ".join([_s for _s in Operation])}')

    def get_operation_function(self, test_string: str) -> Callable[[str], bool]:
        if self is Operation.CONTAINS:
            def _opr(_arg: str):
                return test_string in _arg
            return _opr
        elif self is Operation.IS:
            def _opr(_arg: str):
                return test_string is _arg
            return _opr
        elif self is Operation.EQ:
            def _opr(_arg: str):
                return test_string == _arg
            return _opr
        elif self is Operation.REGEX:
            def _opr(_arg: str):
                _re_pattern = re.compile(test_string)
                return _re_pattern.search(_arg) is not None
            return _opr
        elif self is Operation.IREGEX:
            def _opr(_arg: str):
                _re_pattern = re.compile(test_string, re.RegexFlag.I)
                return _re_pattern.search(_arg) is not None
            return _opr


class BaseFilter:
    def __init__(self, base_string: str):
        self.re_pattern = BaseFilter.re_pattern()
        _match_dict = self.re_pattern.search(base_string).groupdict()
        self.key = _match_dict.get('key')
        self.operation_on_value = Operation.from_symbol(_match_dict.get('op')).\
            get_operation_function(_match_dict.get('filter'))
        self.whole_filter = _match_dict.get('whole_filter')

    def match_on_dict(self, the_dict: dict) -> bool:
        for kk, vv in the_dict.items():
            if (kk == self.key or kk == 'ANY') and self.operation_on_value(vv):
                return True
        return False

    @staticmethod
    def re_pattern_string() -> str:
        return r'(?P<whole_filter>(?P<key>' + \
               regex_options_string(data.keys) + \
               r') (?P<op>' + \
               regex_options_string([v.symbol for v in Operation]) + \
               r') (?P<q>[\"\'])(?P<filter>[^\"\']+)(?P=q))'

    @staticmethod
    def re_pattern() -> re.Pattern:
        return re.compile(BaseFilter.re_pattern_string())


def phrase_filter_iterator(phrase_filter: str, data_set: DataSet) -> Iterator:
        q_string = phrase_filter
        re_p_filter = BaseFilter.re_pattern()
        f = re_p_filter.finditer(q_string)
        bfs = [BaseFilter(o.groupdict().get('whole_filter')) for o in re_p_filter.finditer(q_string)]
        for dict_item in data_set.json_data:
            loc_string: str = phrase_filter
            for bf in bfs:
                loc_string = loc_string.replace(bf.whole_filter, 'True' if bf.match_on_dict(dict_item) else 'False')
            if eval(loc_string):
                yield dict_item


if __name__ == '__main__':
    if args._include_keys == 'LIST':
        print(', '.join(data.keys))
    else:
        filter_string = ''
        if args.evidence_of_execution:
            filter_string = EVIDENCE_OF_EXECUTION
        elif args.filter_string is not None:
            filter_string = args.filter_string
        filtered = phrase_filter_iterator(filter_string, data)
        for an_item in filtered:
            print(f'-' * 200)
            for a_key in args.included_keys(data):
                print(f'{a_key}\n\t{an_item.get(a_key)}\n')






