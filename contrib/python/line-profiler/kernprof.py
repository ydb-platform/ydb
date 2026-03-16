#!/usr/bin/env python
"""
Script to conveniently run profilers on code in a variety of circumstances.

To profile a script, decorate the functions of interest with ``@profile``

.. code:: bash

    echo "if 1:
        @profile
        def main():
            1 + 1
        main()
    " > script_to_profile.py

NOTE:

    New in 4.1.0: Instead of relying on injecting ``profile`` into the builtins
    you can now ``import line_profiler`` and use ``line_profiler.profile`` to
    decorate your functions. This allows the script to remain functional even
    if it is not actively profiled. See :py:mod:`line_profiler` for details.


Then run the script using kernprof:

.. code:: bash

    kernprof -b script_to_profile.py

By default this runs with the default :py:mod:`cProfile` profiler and does not
require compiled modules. Instructions to view the results will be given in the
output. Alternatively, adding ``-v`` to the command line will write results to
stdout.

To enable line-by-line profiling, then :py:mod:`line_profiler` must be
available and compiled. Add the ``-l`` argument to the kernprof invocation.

.. code:: bash

    kernprof -lb script_to_profile.py


For more details and options, refer to the CLI help.
To view kernprof help run:

.. code:: bash

    kenprof --help

which displays:

.. code::

    usage: kernprof [-h] [-V] [-l] [-b] [-o OUTFILE] [-s SETUP] [-v] [-r] [-u UNIT] [-z] [-i [OUTPUT_INTERVAL]] [-p PROF_MOD] [--prof-imports] script ...

    Run and profile a python script.

    positional arguments:
      script                The python script file to run
      args                  Optional script arguments

    options:
      -h, --help            show this help message and exit
      -V, --version         show program's version number and exit
      -l, --line-by-line    Use the line-by-line profiler instead of cProfile. Implies --builtin.
      -b, --builtin         Put 'profile' in the builtins. Use 'profile.enable()'/'.disable()', '@profile' to decorate functions, or 'with profile:' to profile a section of code.
      -o OUTFILE, --outfile OUTFILE
                            Save stats to <outfile> (default: 'scriptname.lprof' with --line-by-line, 'scriptname.prof' without)
      -s SETUP, --setup SETUP
                            Code to execute before the code to profile
      -v, --view            View the results of the profile in addition to saving it
      -r, --rich            Use rich formatting if viewing output
      -u UNIT, --unit UNIT  Output unit (in seconds) in which the timing info is displayed (default: 1e-6)
      -z, --skip-zero       Hide functions which have not been called
      -i [OUTPUT_INTERVAL], --output-interval [OUTPUT_INTERVAL]
                            Enables outputting of cumulative profiling results to file every n seconds. Uses the threading module. Minimum value is 1 (second). Defaults to disabled.
      -p PROF_MOD, --prof-mod PROF_MOD
                            List of modules, functions and/or classes to profile specified by their name or path. List is comma separated, adding the current script path profiles
                            full script. Only works with line_profiler -l, --line-by-line
      --prof-imports        If specified, modules specified to `--prof-mod` will also autoprofile modules that they import. Only works with line_profiler -l, --line-by-line
"""
import builtins
import functools
import os
import sys
import threading
import asyncio  # NOQA
import concurrent.futures  # NOQA
import time
from argparse import ArgumentError, ArgumentParser

# NOTE: This version needs to be manually maintained in
# line_profiler/line_profiler.py and line_profiler/__init__.py as well
__version__ = '4.2.0'

# Guard the import of cProfile such that 3.x people
# without lsprof can still use this script.
try:
    from cProfile import Profile
except ImportError:
    try:
        from lsprof import Profile
    except ImportError:
        from profile import Profile


def execfile(filename, globals=None, locals=None):
    """ Python 3.x doesn't have 'execfile' builtin """
    with open(filename, 'rb') as f:
        exec(compile(f.read(), filename, 'exec'), globals, locals)
# =====================================


CO_GENERATOR = 0x0020


def is_generator(f):
    """ Return True if a function is a generator.
    """
    isgen = (f.__code__.co_flags & CO_GENERATOR) != 0
    return isgen


class ContextualProfile(Profile):
    """ A subclass of Profile that adds a context manager for Python
    2.5 with: statements and a decorator.
    """

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self.enable_count = 0

    def enable_by_count(self, subcalls=True, builtins=True):
        """ Enable the profiler if it hasn't been enabled before.
        """
        if self.enable_count == 0:
            self.enable(subcalls=subcalls, builtins=builtins)
        self.enable_count += 1

    def disable_by_count(self):
        """ Disable the profiler if the number of disable requests matches the
        number of enable requests.
        """
        if self.enable_count > 0:
            self.enable_count -= 1
            if self.enable_count == 0:
                self.disable()

    def __call__(self, func):
        """ Decorate a function to start the profiler on function entry and stop
        it on function exit.
        """
        # FIXME: refactor this into a utility function so that both it and
        # line_profiler can use it.
        if is_generator(func):
            wrapper = self.wrap_generator(func)
        else:
            wrapper = self.wrap_function(func)
        return wrapper

    # FIXME: refactor this stuff so that both LineProfiler and
    # ContextualProfile can use the same implementation.
    def wrap_generator(self, func):
        """ Wrap a generator to profile it.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwds):
            g = func(*args, **kwds)
            # The first iterate will not be a .send()
            self.enable_by_count()
            try:
                item = next(g)
            except StopIteration:
                return
            finally:
                self.disable_by_count()
            input = (yield item)
            # But any following one might be.
            while True:
                self.enable_by_count()
                try:
                    item = g.send(input)
                except StopIteration:
                    return
                finally:
                    self.disable_by_count()
                input = (yield item)
        return wrapper

    def wrap_function(self, func):
        """ Wrap a function to profile it.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwds):
            self.enable_by_count()
            try:
                result = func(*args, **kwds)
            finally:
                self.disable_by_count()
            return result
        return wrapper

    def __enter__(self):
        self.enable_by_count()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable_by_count()


class RepeatedTimer(object):
    """
    Background timer for outputting file every n seconds.

    Adapted from [SO474528]_.

    References:
        .. [SO474528] https://stackoverflow.com/questions/474528/execute-function-every-x-seconds/40965385#40965385
    """
    def __init__(self, interval, dump_func, outfile):
        self._timer = None
        self.interval = interval
        self.dump_func = dump_func
        self.outfile = outfile
        self.is_running = False
        self.next_call = time.time()
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.dump_func(self.outfile)

    def start(self):
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def find_script(script_name):
    """ Find the script.

    If the input is not a file, then $PATH will be searched.
    """
    if os.path.isfile(script_name):
        return script_name
    path = os.getenv('PATH', os.defpath).split(os.pathsep)
    for dir in path:
        if dir == '':
            continue
        fn = os.path.join(dir, script_name)
        if os.path.isfile(fn):
            return fn

    sys.stderr.write('Could not find script %s\n' % script_name)
    raise SystemExit(1)


def _python_command():
    """
    Return a command that corresponds to :py:obj:`sys.executable`.
    """
    import shutil
    if shutil.which('python') == sys.executable:
        return 'python'
    elif shutil.which('python3') == sys.executable:
        return 'python3'
    else:
        return sys.executable


def main(args=None):
    """
    Runs the command line interface
    """
    def positive_float(value):
        val = float(value)
        if val <= 0:
            raise ArgumentError
        return val

    parser = ArgumentParser(description='Run and profile a python script.')
    parser.add_argument('-V', '--version', action='version', version=__version__)
    parser.add_argument('-l', '--line-by-line', action='store_true',
                        help='Use the line-by-line profiler instead of cProfile. Implies --builtin.')
    parser.add_argument('-b', '--builtin', action='store_true',
                        help="Put 'profile' in the builtins. Use 'profile.enable()'/'.disable()', "
                        "'@profile' to decorate functions, or 'with profile:' to profile a "
                        'section of code.')
    parser.add_argument('-o', '--outfile',
                        help="Save stats to <outfile> (default: 'scriptname.lprof' with "
                        "--line-by-line, 'scriptname.prof' without)")
    parser.add_argument('-s', '--setup',
                        help='Code to execute before the code to profile')
    parser.add_argument('-v', '--view', action='store_true',
                        help='View the results of the profile in addition to saving it')
    parser.add_argument('-r', '--rich', action='store_true',
                        help='Use rich formatting if viewing output')
    parser.add_argument('-u', '--unit', default='1e-6', type=positive_float,
                        help='Output unit (in seconds) in which the timing info is '
                        'displayed (default: 1e-6)')
    parser.add_argument('-z', '--skip-zero', action='store_true',
                        help="Hide functions which have not been called")
    parser.add_argument('-i', '--output-interval', type=int, default=0, const=0, nargs='?',
                        help="Enables outputting of cumulative profiling results to file every n seconds. Uses the threading module. "
                        "Minimum value is 1 (second). Defaults to disabled.")
    parser.add_argument('-p', '--prof-mod', type=str, default='',
                        help="List of modules, functions and/or classes to profile specified by their name or path. "
                        "List is comma separated, adding the current script path profiles full script. "
                        "Only works with line_profiler -l, --line-by-line")
    parser.add_argument('--prof-imports', action='store_true',
                        help="If specified, modules specified to `--prof-mod` will also autoprofile modules that they import. "
                        "Only works with line_profiler -l, --line-by-line")

    parser.add_argument('script', help='The python script file to run')
    parser.add_argument('args', nargs='...', help='Optional script arguments')

    options = parser.parse_args(args)

    if not options.outfile:
        extension = 'lprof' if options.line_by_line else 'prof'
        options.outfile = '%s.%s' % (os.path.basename(options.script), extension)

    sys.argv = [options.script] + options.args
    if options.setup is not None:
        # Run some setup code outside of the profiler. This is good for large
        # imports.
        setup_file = find_script(options.setup)
        __file__ = setup_file
        __name__ = '__main__'
        # Make sure the script's directory is on sys.path instead of just
        # kernprof.py's.
        sys.path.insert(0, os.path.dirname(setup_file))
        ns = locals()
        execfile(setup_file, ns, ns)

    if options.line_by_line:
        import line_profiler
        prof = line_profiler.LineProfiler()
        options.builtin = True
    else:
        prof = ContextualProfile()

    # If line_profiler is installed, then overwrite the explicit decorator
    try:
        import line_profiler
    except ImportError:
        ...
    else:
        line_profiler.profile._kernprof_overwrite(prof)

    if options.builtin:
        builtins.__dict__['profile'] = prof

    script_file = find_script(options.script)
    __file__ = script_file
    __name__ = '__main__'
    # Make sure the script's directory is on sys.path instead of just
    # kernprof.py's.
    sys.path.insert(0, os.path.dirname(script_file))

    if options.output_interval:
        rt = RepeatedTimer(max(options.output_interval, 1), prof.dump_stats, options.outfile)
    original_stdout = sys.stdout
    if options.output_interval:
        rt = RepeatedTimer(max(options.output_interval, 1), prof.dump_stats, options.outfile)
    try:
        try:
            execfile_ = execfile
            ns = locals()
            if options.prof_mod and options.line_by_line:
                from line_profiler.autoprofile import autoprofile
                prof_mod = options.prof_mod.split(',')
                autoprofile.run(script_file, ns, prof_mod=prof_mod, profile_imports=options.prof_imports)
            elif options.builtin:
                execfile(script_file, ns, ns)
            else:
                prof.runctx('execfile_(%r, globals())' % (script_file,), ns, ns)
        except (KeyboardInterrupt, SystemExit):
            pass
    finally:
        if options.output_interval:
            rt.stop()
        prof.dump_stats(options.outfile)
        print('Wrote profile results to %s' % options.outfile)
        if options.view:
            if isinstance(prof, ContextualProfile):
                prof.print_stats()
            else:
                prof.print_stats(output_unit=options.unit,
                                 stripzeros=options.skip_zero,
                                 rich=options.rich,
                                 stream=original_stdout)
        else:
            print('Inspect results with:')
            py_exe = _python_command()
            if isinstance(prof, ContextualProfile):
                print(f'{py_exe} -m pstats "{options.outfile}"')
            else:
                print(f'{py_exe} -m line_profiler -rmt "{options.outfile}"')


if __name__ == '__main__':
    main(sys.argv[1:])
