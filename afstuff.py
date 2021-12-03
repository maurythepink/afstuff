#! /usr/bin/env python


import argparse
import re
from pathlib import Path
import json
from enum import Enum, auto
from typing import Union, Optional, Iterator, Generator, Callable


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
ap.add_argument('-f --filter',
                dest='filter_string',
                type=str,
                action='store',
                help='filter string, like \'message contains \"https:\"\'')


class ParsedArgs:
    def __init__(self):
        _parsed_args = ap.parse_args()
        _source_file = Path(_parsed_args.source)
        if not _source_file.exists():
            raise ValueError(f'File at path \'{_source_file}\' does not exist!')
        self.source_file = _source_file
        _source_type = _parsed_args.source_type
        if _source_type is not None:
            self.source_type = FileType.from_string(_source_type)
        else:
            self.source_type = FileType.infer_form_file_name(self.source_file)
        self.filter_string = _parsed_args.filter_string


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
        _re_pattern = re.compile(r'(?P<key>' +
                                     regex_options_string(data.keys) +
                                     r') (?P<op>' +
                                     regex_options_string([v.symbol for v in Operation]) +
                                     r') (?P<q>[\"\'])(?P<filter>[^(?P=q)]+)(?P=q)')
        _match_dict = _re_pattern.search(base_string).groupdict()
        self.key = _match_dict.get('key')
        self.operation_on_value = Operation.from_symbol(_match_dict.get('op')).\
            get_operation_function(_match_dict.get('filter'))

    def match_on_dict(self, the_dict: dict) -> bool:
        for kk, vv in the_dict.items():
            if kk == self.key and self.operation_on_value(vv):
                return True
        return False



query = '(message contains "https:" or message contains "url") and date > "2020-01-01 00:00:00"'


if __name__ == '__main__':
    bs = BaseFilter(args.filter_string) if args.filter_string is not None else None
    for element in data.json_data:
        for key, value in element.items():
            if bs is None or bs.match_on_dict(element):
                print(f'{"-" * 100}\n{key}\n\t{value}')

