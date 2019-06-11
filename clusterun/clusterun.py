import re
import subprocess
import sys
from argparse import ArgumentParser, ArgumentTypeError
from ast import literal_eval
from datetime import datetime
from inspect import currentframe
from itertools import islice, product
from pathlib import Path
from shlex import quote
from textwrap import dedent

from .utils import load_name


def get_parameters(space, num_cores=1, core=0, skip=0):
    """Split a parameter space into a size appropriate for one core.

    Arguments:
        space (Iterable): The space of parameters.
        num_cores (int): The number of cores to split jobs for.
        core (int): The core whose job to start.
        skip (int): The number of initial parameters to skip.

    Returns:
        Sequence[Namespace]: The relevant section of the parameter space.
    """
    return list(islice(space, core, None, num_cores))[skip:]


def create_command(args, indices):
    command = []
    command.append(sys.executable)
    command.append(str(Path(__file__).resolve()))
    command.append(f'--command {quote(args.command)}')
    for var, vals in args.variables:
        variable_argument = f'{var}={repr(vals)}'
        command.append(f'--variable {quote(variable_argument)}')
    command.append('--index ' + ','.join(str(i) for i in indices))
    command.append('--dispatch False')
    return ' '.join(command)


def dry_run(args):
    print('Command:')
    print(f'    {args.command}')
    print()
    print(f'{len(args.variables)} variable(s):')
    for var, vals in args.variables:
        print(f'    {var} ({len(vals)}): {", ".join(repr(v) for v in vals)}')
    print()
    if args.dispatch:
        print(' '.join([
            f'dispatching {sum(len(indices) for indices in args.indices)}',
            f'out of {args.size} permutations,',
            f'to {len(args.indices)} parallel job(s):',
        ]))
        for job_num, indices in enumerate(args.indices, start=1):
            print(f'    job {job_num} ({len(indices)}): ' + ', '.join(str(i) for i in indices))
    else:
        print(' '.join([
            f'running {sum(len(indices) for indices in args.indices)}',
            f'out of {args.size} permutations',
        ]))


def dispatch(args):
    for job_num, indices in enumerate(args.indices, start=1):
        command = create_command(args, indices)
        script = dedent(f'''
            #!/bin/sh

            #PBS -N {args.job_name}-{job_num}
            #PBS -q {args.queue}
            #PBS -l nodes=n006.cluster.com:ppn=1,mem=1000mb,file=4gb
            #PBS -r n

            {command}
        ''').strip()
        subprocess.run(
            ['qsub', '-'],
            input=script.encode('utf-8'),
            shell=True,
        )


def run_single(args):
    index = set(args.index)
    variables = [var for var, _ in args.variables]
    for i, values in enumerate(product(*(vals for _, vals in args.variables))):
        if i not in index:
            continue
        script = []
        for var, val in zip(variables, values):
            script.append(f'{var}={val}')
        script.append(args.command)
        script = '\n'.join(script)
        subprocess.run(script, shell=True)


def valid_variable(var):
    return re.fullmatch('[a-z_][a-z0-9_]*', var)


def valid_varval(varval):
    if '=' not in varval:
        raise ArgumentTypeError(f'failed to parse --variable "{varval}"')
    var, val = varval.split('=', maxsplit=1)
    if not valid_variable(var):
        raise ArgumentTypeError(f'variable "{var}" does not conform to [a-z][a-z0-9_]*')
    try:
        val = literal_eval(val)
    except ValueError:
        raise ArgumentTypeError(f'failed to parse values "{val}" for variable "{var}"')
    return (var, val)


def create_arg_parser(command=None, variables=None, job_name=None):
    if job_name is None:
        job_name = f'clusterun-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    arg_parser = ArgumentParser()
    arg_parser.set_defaults(
        command=command,
        variables=variables,
    )
    if command is None:
        arg_parser.add_argument(
            '--command', type=str,
            help='The command to run',
        )
    if variables is None:
        arg_parser.add_argument(
            '--variable', type=valid_varval, dest='variables', action='append',
            help='A variable, in the form str=<expr>.',
        )
    else:
        for var, _ in variables:
            if not valid_variable(var):
                raise ValueError(f'variable "{var}" does not conform to [a-z][a-z0-9_]*')
    arg_parser.add_argument(
        '--num-cores', type=int,
        help='Number of cores to run the job on.',
    )
    arg_parser.add_argument(
        '--core', type=int,
        help=' '.join([
            'The core to run the current job.',
            'Must be used with --num-cores.',
            'Must NOT be used with --index.',
        ]),
    )
    arg_parser.add_argument(
        '--index', type=str,
        help=' '.join([
            'The indices of the iterable to use.',
            'Must be used with --num-cores.',
            'Must NOT be used with --index.',
        ]),
    )
    arg_parser.add_argument(
        '--skip', type=int, default=0,
        help='Skip some initial parameters. Ignored if not running serially.',
    )
    arg_parser.add_argument(
        '--job-name', default=job_name,
        help='The name of the job, if passed to pbs. Ignored if not dispatched.',
    )
    arg_parser.add_argument(
        '--queue', default='justinli',
        help='The queue to submit jobs to.',
    )
    arg_parser.add_argument(
        '--dry-run', action='store_true',
        help='If set, print out the parameter space and exit.',
    )
    arg_parser.add_argument(
        '--dispatch', type=(lambda s: s.lower() == 'true'), default=None,
        help=' '.join([
            'Force job to be dispatched if true, or to run serially if not.',
            'By default, will dispatch if --num-cores is set',
            'but neither --core nor --index is set.',
        ]),
    )
    return arg_parser


def parse_indices(indices_str):
    if not re.fullmatch('[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*', indices_str):
        raise ValueError(' '.join([
            'index argument does not conform to',
            '[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*',
        ]))
    indices = set()
    for part in indices_str.split(','):
        if '-' in part:
            start, stop = part.split('-')
            indices |= set(range(int(start), int(stop)))
        else:
            indices.add(int(part))
    return sorted(indices)


def check_args(arg_parser, args):
    if args.command is None:
        arg_parser.error('--command must be set')
    if not args.variables:
        arg_parser.error('at least one --variable must be set')
    variables = set()
    for var, _ in args.variables:
        if var in variables:
            arg_parser.error('variable {variable} is defined multiple times')
        variables.add(var)
    if args.core is not None:
        if args.num_cores is None:
            arg_parser.error('--num-cores must be set if --core is set')
        if args.index is not None:
            arg_parser.error('only one of --core and --index can be set')
    if args.dispatch is False and args.num_cores is not None:
        arg_parser.error('--num-cores must not be set if --dispatch=False')
    if args.num_cores is not None and args.core is not None and args.core >= args.num_cores:
        arg_parser.error('--core must be less than --num-cores')


def parse_args(command=None, variables=None, job_name=None):
    arg_parser = create_arg_parser(command, variables, job_name)
    args = arg_parser.parse_args()
    check_args(arg_parser, args)
    if args.dispatch is None:
        args.dispatch = (
            args.num_cores is not None
            and args.core is None
            and args.index is None
        )
    args.size = 1
    for _, vals in args.variables:
        args.size *= len(vals)
    if args.core is not None:
        args.indices = [get_parameters(range(args.size), args.num_cores, args.core, args.skip)]
    else:
        if args.index is None:
            args.index = list(range(args.size))
        else:
            args.index = parse_indices(args.index)
            if args.index[-1] > args.size:
                arg_parser.error('maximum index is greater than size of the variable space')
        if args.num_cores is None:
            args.indices = [args.index][args.skip:]
        else:
            args.indices = [
                get_parameters(args.index, args.num_cores, core)
                for core in range(args.num_cores)
            ]
    if len(args.indices) != 1 and args.skip != 0:
        arg_parser.error('--skip must not be set if running in parallel')
    return args


def clusterun(command=None, variables=None, job_name=None):
    args = parse_args(command=command, variables=variables, job_name=job_name)
    if args.dry_run:
        dry_run(args)
    elif args.dispatch:
        dispatch(args)
    else:
        run_single(args)


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
        space = load_name(code_path, space_name)
    elif hasattr(space, '__call__'):
        space_name = space.__name__
        space = space()
    else:
        raise ValueError(f'space {repr(space)} is neither a string nor callable')
    callback_name = callback.__name__
    filepath = Path(__file__).resolve().parent.joinpath('dispatched.py')
    variables = [
        ('sequencerun_index', list(range(len(space)))),
    ]
    command = ' && '.join([
        f'cd {directory}',
        f'{executable} {filepath} {code_path} {callback_name} {space_name} --index "$sequencerun_index"',
    ])
    clusterun(command, variables, job_name=job_name)


if __name__ == '__main__':
    clusterun()
