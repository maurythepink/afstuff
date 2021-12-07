#! /usr/bin/env python3.9


from datetime import datetime
import argparse
import re
from pathlib import Path
import json
from enum import Enum, auto
from typing import Union, Optional, Iterator, Callable


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


ap = argparse.ArgumentParser(prog='AF Stuff', usage='this tool is a facility for applying filters on plaso psort '
                                                    'outputs. It contains the pre-made filters built for specific '
                                                    'evidence production. Es: Evidence of execution, deletion, file '
                                                    'opening, etc.. You shoud use this tool with the default psort '
                                                    'output, the one with type \'dynamic\', adding the extension '
                                                    '\'.dynamic\' at the end of the csv file name.')
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
ap.add_argument('-c --csv-output',
                dest='csv_output',
                action='store_true',
                help='set the otput to be in \'csv\' format.')
ap.add_argument('-f --filter',
                dest='filter_string',
                type=str,
                action='store',
                help='filter string, like \'message contains \"https:\"\', \'ANY contains "google"\'. Only datetime '
                     'strings in ISO format can be filtered using the < and > operations. '
                     'Es: datetime > \'2020-12-10T08:34:05+00:00\'')
ap.add_argument('-x --filter-files',
                dest='filter_files',
                type=str,
                nargs='*',
                help='pass any text files witch contains filters, they will be processed together with an'
                     ' \'and\' joint.')
ap.add_argument('--debug',
                dest='debug',
                action='store_true')


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
        self.csv_output = _parsed_args.csv_output
        self.debug = _parsed_args.debug is True
        self._filter_files: Optional[list[str]] = _parsed_args.filter_files

    def file_filters(self) -> Iterator[str]:
        for a_str in self._filter_files:
            _path = Path(a_str)
            if not _path.exists():
                raise ValueError(f'file at path \'{_path}\' does not exist!!')
            with _path.open('r') as _filter_file:
                yield f'({_filter_file.read().strip()})'

    @property
    def complete_filter(self) -> Optional[str]:
        if self.filter_string is None and self._filter_files is None:
            return None
        _the_str = ''
        if self._filter_files is not None:
            _the_str = f'{" and ".join(self.file_filters())}'
        if self.filter_string is not None:
            _the_str = self.filter_string if self._filter_files is None else f'{_the_str} and ({self.filter_string})'
        return _the_str

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
    EQ = auto()
    NotEQ = auto()
    REGEX = auto()
    IREGEX = auto()
    DateGT = auto()
    DateLT = auto()

    @property
    def symbol(self) -> str:
        if self is Operation.CONTAINS:
            return 'contains'
        elif self is Operation.EQ:
            return '=='
        elif self is Operation.NotEQ:
            return '!='
        elif self is Operation.REGEX:
            return 'regex'
        elif self is Operation.IREGEX:
            return 'iregex'
        elif self is Operation.DateGT:
            return '>'
        elif self is Operation.DateLT:
            return '<'
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
        elif self is Operation.EQ:
            def _opr(_arg: str):
                return test_string == _arg
            return _opr
        elif self is Operation.NotEQ:
            def _opr(_arg: str):
                return test_string != _arg
            return _opr
        elif self is Operation.REGEX or self is Operation.IREGEX:
            def _opr(_arg: str):
                _re_pattern = re.compile(test_string, re.RegexFlag.I if self is Operation.IREGEX else 0)
                return _re_pattern.search(_arg) is not None
            return _opr
        elif self is Operation.DateGT:
            def _opr(_arg: str):
                return datetime.fromisoformat(_arg) > datetime.fromisoformat(test_string)
            return _opr
        elif self is Operation.DateLT:
            def _opr(_arg: str):
                return datetime.fromisoformat(_arg) < datetime.fromisoformat(test_string)
            return _opr


class BaseFilter:
    def __init__(self, base_string: str):
        self.re_pattern = BaseFilter.re_pattern()
        _match_dict = self.re_pattern.search(base_string).groupdict()
        self.key = _match_dict.get('key')
        self.not_operator = True if _match_dict.get('not') else False
        self.operation_on_value = Operation.from_symbol(_match_dict.get('op')).\
            get_operation_function(_match_dict.get('filter'))
        self.whole_filter = _match_dict.get('whole_filter')

    def match_on_dict(self, the_dict: dict) -> bool:
        for kk, vv in the_dict.items():
            if (kk == self.key or self.key == 'ANY') and self.operation_on_value(vv):
                return True
        return False

    @staticmethod
    def re_pattern_string() -> str:
        return r'(?P<whole_filter>(?P<key>' + \
               regex_options_string(data.keys + ['ANY']) + \
               r')(?P<not> not\b)? (?P<op>' + \
               regex_options_string([v.symbol for v in Operation]) + \
               r') (?P<q>[\"\'])(?P<filter>[^\"\']+)(?P=q))'

    @staticmethod
    def re_pattern() -> re.Pattern:
        return re.compile(BaseFilter.re_pattern_string())


def phrase_filter_iterator(phrase_filter: str, data_set: DataSet) -> Iterator:
        q_string = phrase_filter
        re_p_filter = BaseFilter.re_pattern()
        bfs = [BaseFilter(o.groupdict().get('whole_filter')) for o in re_p_filter.finditer(q_string)]
        for dict_item in data_set.json_data:
            loc_string: str = phrase_filter
            for bf in bfs:
                replacement_string = 'True' if bf.match_on_dict(dict_item) else 'False'
                if bf.not_operator:
                    replacement_string = f'not {replacement_string}'
                loc_string = loc_string.replace(bf.whole_filter, replacement_string)
            if eval(loc_string):
                yield dict_item


def csv_parser(entry: Optional[dict], keys: list[str], first_row=False) -> str:
    if first_row:
        return ','.join(keys).strip()
    else:
        return ','.join((entry[_the_key].strip() for _the_key in keys))


if __name__ == '__main__':
    if args.debug:
        print(args.complete_filter)
    elif args._include_keys == 'LIST':
        print(', '.join(data.keys))
    else:
        count = 0
        filtered = data.json_data if args.complete_filter is None else phrase_filter_iterator(args.complete_filter, data)
        if args.csv_output:
            _loc_keys = args.included_keys(data)
            print(csv_parser(None, _loc_keys, True))
            for an_item in filtered:
                print(csv_parser(an_item, _loc_keys))
        else:
            for an_item in filtered:
                print(f'-' * 200)
                count += 1
                for a_key in args.included_keys(data):
                    print(f'{a_key}\n\t{an_item.get(a_key)}\n')
            print(f'\nEnd of search: found {count} records.')






