import tempfile
import sys
import os
import ubelt as ub


def test_single_function_autoprofile():
    """
    Test that every function in a file is profiled when autoprofile is enabled.
    """
    temp_dpath = ub.Path(tempfile.mkdtemp())

    code = ub.codeblock(
        '''
        def func1(a):
            return a + 1

        func1(1)
        ''')
    with ub.ChDir(temp_dpath):

        script_fpath = ub.Path('script.py')
        script_fpath.write_text(code)

        args = [sys.executable, '-m', 'kernprof', '-p', 'script.py', '-l', os.fspath(script_fpath)]
        proc = ub.cmd(args)
        print(proc.stdout)
        print(proc.stderr)
        proc.check_returncode()

        args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
        proc = ub.cmd(args)
        raw_output = proc.stdout
        proc.check_returncode()

    assert 'func1' in raw_output
    temp_dpath.delete()


def test_multi_function_autoprofile():
    """
    Test that every function in a file is profiled when autoprofile is enabled.
    """
    temp_dpath = ub.Path(tempfile.mkdtemp())

    code = ub.codeblock(
        '''
        def func1(a):
            return a + 1

        def func2(a):
            return a * 2 + 2

        def func3(a):
            return a / 10 + 3

        def func4(a):
            return a % 2 + 4

        func1(1)
        ''')
    with ub.ChDir(temp_dpath):

        script_fpath = ub.Path('script.py')
        script_fpath.write_text(code)

        args = [sys.executable, '-m', 'kernprof', '-p', 'script.py', '-l', os.fspath(script_fpath)]
        proc = ub.cmd(args)
        print(proc.stdout)
        print(proc.stderr)
        proc.check_returncode()

        args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
        proc = ub.cmd(args)
        raw_output = proc.stdout
        proc.check_returncode()

    assert 'func1' in raw_output
    assert 'func2' in raw_output
    assert 'func3' in raw_output
    assert 'func4' in raw_output

    temp_dpath.delete()


def test_duplicate_function_autoprofile():
    """
    Test that every function in a file is profiled when autoprofile is enabled.
    """
    temp_dpath = ub.Path(tempfile.mkdtemp())

    code = ub.codeblock(
        '''
        def func1(a):
            return a + 1

        def func2(a):
            return a + 1

        def func3(a):
            return a + 1

        def func4(a):
            return a + 1

        func1(1)
        func2(1)
        func3(1)
        ''')
    with ub.ChDir(temp_dpath):

        script_fpath = ub.Path('script.py')
        script_fpath.write_text(code)

        args = [sys.executable, '-m', 'kernprof', '-p', 'script.py', '-l', os.fspath(script_fpath)]
        proc = ub.cmd(args)
        print(proc.stdout)
        print(proc.stderr)
        proc.check_returncode()

        args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
        proc = ub.cmd(args)
        raw_output = proc.stdout
        print(raw_output)
        proc.check_returncode()

    assert 'Function: func1' in raw_output
    assert 'Function: func2' in raw_output
    assert 'Function: func3' in raw_output
    assert 'Function: func4' in raw_output

    temp_dpath.delete()


def _write_demo_module(temp_dpath):
    """
    Make a dummy test module structure
    """
    (temp_dpath / 'test_mod').ensuredir()
    (temp_dpath / 'test_mod/subpkg').ensuredir()

    (temp_dpath / 'test_mod/__init__.py').touch()
    (temp_dpath / 'test_mod/subpkg/__init__.py').touch()

    (temp_dpath / 'test_mod/util.py').write_text(ub.codeblock(
        '''
        def add_operator(a, b):
            return a + b
        '''))

    (temp_dpath / 'test_mod/submod1.py').write_text(ub.codeblock(
        '''
        from test_mod.util import add_operator
        def add_one(items):
            new_items = []
            for item in items:
                new_item = add_operator(item, 1)
                new_items.append(new_item)
            return new_items
        '''))
    (temp_dpath / 'test_mod/submod2.py').write_text(ub.codeblock(
        '''
        from test_mod.util import add_operator
        def add_two(items):
            new_items = [add_operator(item, 2) for item in items]
            return new_items
        '''))
    (temp_dpath / 'test_mod/subpkg/submod3.py').write_text(ub.codeblock(
        '''
        from test_mod.util import add_operator
        def add_three(items):
            new_items = [add_operator(item, 3) for item in items]
            return new_items
        '''))

    script_fpath = (temp_dpath / 'script.py')
    script_fpath.write_text(ub.codeblock(
        '''
        from test_mod import submod1
        from test_mod import submod2
        from test_mod.subpkg import submod3
        import statistics

        def main():
            data = [1, 2, 3]
            val = submod1.add_one(data)
            val = submod2.add_two(val)
            val = submod3.add_three(val)

            result = statistics.harmonic_mean(val)
            print(result)

        main()
        '''))
    return script_fpath


def test_autoprofile_script_with_module():
    """
    Test that every function in a file is profiled when autoprofile is enabled.
    """

    temp_dpath = ub.Path(tempfile.mkdtemp())

    script_fpath = _write_demo_module(temp_dpath)

    # args = [sys.executable, '-m', 'kernprof', '--prof-imports', '-p', 'script.py', '-l', os.fspath(script_fpath)]
    args = [sys.executable, '-m', 'kernprof', '-p', 'script.py', '-l', os.fspath(script_fpath)]
    proc = ub.cmd(args, cwd=temp_dpath, verbose=2)
    print(proc.stdout)
    print(proc.stderr)
    proc.check_returncode()

    args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
    proc = ub.cmd(args, cwd=temp_dpath)
    raw_output = proc.stdout
    print(raw_output)
    proc.check_returncode()

    assert 'Function: add_one' not in raw_output
    assert 'Function: main' in raw_output


def test_autoprofile_module():
    """
    Test that every function in a file is profiled when autoprofile is enabled.
    """

    temp_dpath = ub.Path(tempfile.mkdtemp())

    script_fpath = _write_demo_module(temp_dpath)

    # args = [sys.executable, '-m', 'kernprof', '--prof-imports', '-p', 'script.py', '-l', os.fspath(script_fpath)]
    args = [sys.executable, '-m', 'kernprof', '-p', 'test_mod', '-l', os.fspath(script_fpath)]
    proc = ub.cmd(args, cwd=temp_dpath, verbose=2)
    print(proc.stdout)
    print(proc.stderr)
    proc.check_returncode()

    args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
    proc = ub.cmd(args, cwd=temp_dpath)
    raw_output = proc.stdout
    print(raw_output)
    proc.check_returncode()

    assert 'Function: add_one' in raw_output
    assert 'Function: main' not in raw_output


def test_autoprofile_module_list():
    """
    Test only modules specified are autoprofiled
    """

    temp_dpath = ub.Path(tempfile.mkdtemp())

    script_fpath = _write_demo_module(temp_dpath)

    # args = [sys.executable, '-m', 'kernprof', '--prof-imports', '-p', 'script.py', '-l', os.fspath(script_fpath)]
    args = [sys.executable, '-m', 'kernprof', '-p', 'test_mod.submod1,test_mod.subpkg.submod3', '-l', os.fspath(script_fpath)]
    proc = ub.cmd(args, cwd=temp_dpath, verbose=2)
    print(proc.stdout)
    print(proc.stderr)
    proc.check_returncode()

    args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
    proc = ub.cmd(args, cwd=temp_dpath)
    raw_output = proc.stdout
    print(raw_output)
    proc.check_returncode()

    assert 'Function: add_one' in raw_output
    assert 'Function: add_two' not in raw_output
    assert 'Function: add_three' in raw_output
    assert 'Function: main' not in raw_output


def test_autoprofile_module_with_prof_imports():
    """
    Test the imports of the specified modules are profiled as well.
    """
    temp_dpath = ub.Path(tempfile.mkdtemp())
    script_fpath = _write_demo_module(temp_dpath)

    args = [sys.executable, '-m', 'kernprof', '--prof-imports', '-p', 'test_mod.submod1', '-l', os.fspath(script_fpath)]
    proc = ub.cmd(args, cwd=temp_dpath, verbose=2)
    print(proc.stdout)
    print(proc.stderr)
    proc.check_returncode()

    args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
    proc = ub.cmd(args, cwd=temp_dpath)
    raw_output = proc.stdout
    print(raw_output)
    proc.check_returncode()

    assert 'Function: add_one' in raw_output
    assert 'Function: add_operator' in raw_output
    assert 'Function: add_three' not in raw_output
    assert 'Function: main' not in raw_output


def test_autoprofile_script_with_prof_imports():
    """
    Test the imports of the specified modules are profiled as well.
    """
    temp_dpath = ub.Path(tempfile.mkdtemp())
    script_fpath = _write_demo_module(temp_dpath)

    # import sys
    # if sys.version_info[0:2] >= (3, 11):
    #     import pytest
    #     pytest.skip('Failing due to the noop bug')

    args = [sys.executable, '-m', 'kernprof', '--prof-imports', '-p', 'script.py', '-l', os.fspath(script_fpath)]
    proc = ub.cmd(args, cwd=temp_dpath, verbose=0)
    print('Kernprof Stdout:')
    print(proc.stdout)
    print('Kernprof Stderr:')
    print(proc.stderr)
    print('About to check kernprof return code')
    proc.check_returncode()

    args = [sys.executable, '-m', 'line_profiler', os.fspath(script_fpath) + '.lprof']
    proc = ub.cmd(args, cwd=temp_dpath, verbose=0)
    raw_output = proc.stdout
    print('Line_profile Stdout:')
    print(raw_output)
    print('Line_profile Stderr:')
    print(proc.stderr)
    print('About to check line_profiler return code')
    proc.check_returncode()

    assert 'Function: add_one' in raw_output
    assert 'Function: harmonic_mean' in raw_output
    assert 'Function: main' in raw_output
