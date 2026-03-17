import os
import sys
import tempfile
import ubelt as ub
LINUX = sys.platform.startswith('linux')


def get_complex_example_fpath():
    try:
        test_dpath = ub.Path(__file__).parent
    except NameError:
        # for development
        test_dpath = ub.Path('~/code/line_profiler/tests').expanduser()
    complex_fpath = test_dpath / 'complex_example.py'
    return complex_fpath


def test_complex_example_python_none():
    """
    Make sure the complex example script works without any profiling
    """
    complex_fpath = get_complex_example_fpath()
    info = ub.cmd(f'{sys.executable} {complex_fpath}', shell=True, verbose=3, env=ub.udict(os.environ) | {'PROFILE_TYPE': 'none'})
    assert info.stdout == ''
    info.check_returncode()


def test_varied_complex_invocations():
    """
    Tests variations of running the complex example:
        with / without kernprof
        with cProfile / LineProfiler backends
        with / without explicit profiler
    """

    # Enumerate valid cases to test
    cases = []
    for runner in ['python',  'kernprof']:
        for env_line_profile in ['0', '1']:
            if runner == 'kernprof':
                for profile_type in ['explicit', 'implicit']:
                    for kern_flags in ['-l', '-b']:
                        if 'l' in kern_flags:
                            outpath = 'complex_example.py.lprof'
                        else:
                            outpath = 'complex_example.py.prof'

                        cases.append({
                            'runner': runner,
                            'kern_flags': kern_flags,
                            'env_line_profile': env_line_profile,
                            'profile_type': profile_type,
                            'outpath': outpath,
                        })
            else:
                if env_line_profile == '1':
                    outpath = 'profile_output.txt'
                else:
                    outpath = None
                cases.append({
                    'runner': runner,
                    'env_line_profile': env_line_profile,
                    'profile_type': 'explicit',
                    'outpath': outpath,
                })

    # Add case for auto-profile
    # FIXME: this runs, but doesn't quite work.
    cases.append({
        'runner': 'kernprof',
        'kern_flags': '-l --prof-mod complex_example.py',
        'env_line_profile': '0',
        'profile_type': 'none',
        'outpath': 'complex_example.py.lprof',
        'ignore_checks': True,
    })

    if 0:
        # FIXME: this does not run with prof-imports
        cases.append({
            'runner': 'kernprof',
            'kern_flags': '-l --prof-imports --prof-mod complex_example.py',
            'env_line_profile': '0',
            'profile_type': 'none',
            'outpath': 'complex_example.py.lprof',
        })

    complex_fpath = get_complex_example_fpath()

    results = []

    for case in cases:
        temp_dpath = tempfile.mkdtemp()
        with ub.ChDir(temp_dpath):
            env = {}

            outpath = case['outpath']
            if outpath:
                outpath = ub.Path(outpath)

            # Construct the invocation for each case
            if case['runner'] == 'kernprof':
                kern_flags = case['kern_flags']
                # FIXME:
                # Note: kernprof doesn't seem to play well with multiprocessing
                prog_flags = ' --process_size=0'
                runner = f'{sys.executable} -m kernprof {kern_flags}'
            else:
                env['LINE_PROFILE'] = case["env_line_profile"]
                runner = f'{sys.executable}'
                prog_flags = ''
            env['PROFILE_TYPE'] = case["profile_type"]
            command = f'{runner} {complex_fpath}' + prog_flags

            HAS_SHELL = LINUX
            if HAS_SHELL:
                # Use shell because it gives a indication of what is happening
                environ_prefix = ' '.join([f'{k}={v}' for k, v in env.items()])
                info = ub.cmd(environ_prefix + ' ' + command, shell=True, verbose=3)
            else:
                env = ub.udict(os.environ) | env
                info = ub.cmd(command, env=env, verbose=3)

            info.check_returncode()

            result = case.copy()
            if outpath:
                result['outsize'] = outpath.stat().st_size
            else:
                result['outsize'] = None
            results.append(result)

            if outpath:
                assert outpath.exists()
                assert outpath.is_file()
                outpath.delete()

            if 0:
                import pandas as pd
                import rich
                table = pd.DataFrame(results)
                rich.print(table)

            # Ensure the scripts that produced output produced non-trivial output
            if not case.get('ignore_checks', False):
                for result in results:
                    if result['outpath'] is not None:
                        assert result['outsize'] > 100
