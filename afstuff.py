#! /usr/bin/env python


import argparse
from pathlib import Path
import json
from enum import Enum, auto
from typing import Union, Optional, Iterator, Generator


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


if __name__ == '__main__':
    i = 0
    for _d in data.keys:
        print(_d)
        if i == 10:
            break
