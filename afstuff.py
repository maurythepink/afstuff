#! /usr/bin/env python


import argparse
from pathlib import Path
from enum import Enum, auto


ap = argparse.ArgumentParser(prog='AF Stuff', usage='usage')
ap.add_argument('source',
                type=str,
                help='Source file',
                action='store')


class ParsedArgs:
    def __init__(self):
        _parsed_args = ap.parse_args()
        _source_file = Path(_parsed_args.source)
        if not _source_file.exists():
            raise ValueError(f'File at path \'{_source_file}\' does not exist!')
        self.source_file = _source_file


args = ParsedArgs()


if __name__ == '__main__':
    print(args.source_file)



