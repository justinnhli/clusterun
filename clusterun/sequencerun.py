import re
from argparse import ArgumentParser
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path

from .clusterun import clusterun


def load_name(path, name):
    """Import an identifier from another module.

    Arguments:
        path (Path): The path of the file to import from.
        name (str): The name of the variable to import.

    Returns:
        Any: The value of the identifier in that module
    """
    specification = spec_from_file_location(path.name, path)
    module = module_from_spec(specification)
    specification.loader.exec_module(module)
    return getattr(module, name)


def run(filename, callback_name, space_name, index):
    """Execute the callback with the given space.

    Arguments:
        filename (str): The file containing the space and the function.
        callback_name (str): The name of the function to run.
        space_name (str): The name of the variable/function of the argument space.
        index (int): The index of the space to run.
    """
    filename = Path(filename).resolve()
    space = load_name(filename, space_name)
    callback = load_name(filename, callback_name)
    if hasattr(space, '__call__'):
        space = space()
    for i, params in enumerate(space):
        if i == index:
            callback(params)


def sequencerun(callback, space, job_name=None, directory=None, executable=None):
    """Command line interface to the module.

    Arguments:
        callback (Callable): The function to run.
        space (Union[str, Callable]): The argument space.
        job_name (str): The name of the job, if passed to pbs.
        directory (str): The working directory to run from.
        executable (str): The Python executable.

    Raises:
        ValueError: If space is neither a string nor a callable
    """
    code_path = Path(currentframe().f_back.f_code.co_filename).resolve()
    if not callable(callback):
        raise ValueError(f'{repr(callback)} is not callable')
    if directory is None:
        directory = code_path.parent
    if executable is None:
        executable = sys.executable
    if isinstance(space, str):
        space_name = space
    elif hasattr(space, '__call__'):
        space_name = space.__name__
        space = space()
    else:
        raise ValueError(f'space {repr(space)} is neither a string nor callable')
    callback_name = callback.__name__
    filepath = Path(__file__).resolve().parent.joinpath('sequencerun.py')
    variables = [
        ('sequencerun_index', list(range(len(space)))),
    ]
    command = ' && '.join([
        f'cd {directory}',
        f'{executable} {filepath} {code_path} {callback_name} {space_name} --index "$sequencerun_index"',
    ])
    clusterun(command, variables, job_name=job_name)


def main():
    arg_parser = ArgumentParser()
    arg_parser.add_argument('code_path')
    arg_parser.add_argument('callback_name')
    arg_parser.add_argument('space_name')
    arg_parser.add_argument('--index', type=int)
    args = arg_parser.parse_args()
    run(args.code_path, args.callback_name, args.space_name, args.index)
