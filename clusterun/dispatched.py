import os
import sys
from argparse import ArgumentParser
from pathlib import Path

from .utils import load_name


def run(filename, callback_name, space_name, index):
    """Execute the callback with the given space.

    Arguments:
        filename (str): The file containing the space and the function.
        callback_name (str): The name of the function to run.
        space_name (str): The name of the variable/function of the argument space.
        index (int): The index of the space to run.
    """
    filename = Path(filename).resolve()
    sys.path.insert(0, str(filename.parent))
    os.chdir(filename.parent)
    space = load_name(filename, space_name)
    callback = load_name(filename, callback_name)
    if hasattr(space, '__call__'):
        space = space()
    for i, params in enumerate(space):
        if i == index:
            callback(params)


def dispatched():
    arg_parser = ArgumentParser()
    arg_parser.add_argument('code_path')
    arg_parser.add_argument('callback_name')
    arg_parser.add_argument('space_name')
    arg_parser.add_argument('--index', type=int)
    args = arg_parser.parse_args()
    run(args.code_path, args.callback_name, args.space_name, args.index)

if __name__ == '__main__':
    dispatched()
