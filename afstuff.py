#! /usr/bin/env python


import argparse
import re
from pathlib import Path
import json
from enum import Enum, auto
from typing import Union, Optional, Iterator, Generator, Callable


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


query = '(message contains "https:" or message contains "url") and date > "2020-01-01 00:00:00"'


if __name__ == '__main__':
    op = Operation.IREGEX.get_operation_function('HTTPS')
    op2 = Operation.CONTAINS.get_operation_function('~U:neo')
    for dict_element in data.json_data:
        for dict_key, dict_value in dict_element.items():
            if op(dict_value) and op2(dict_value):
                print(f'{"-" * 50}\n{dict_key}\n\n\t{dict_value}\n')
