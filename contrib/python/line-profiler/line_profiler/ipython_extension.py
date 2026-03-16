"""
This module defines the ``%lprun`` IPython magic.

If you are using IPython, there is an implementation of an %lprun magic command
which will let you specify functions to profile and a statement to execute. It
will also add its LineProfiler instance into the __builtins__, but typically,
you would not use it like that.

For IPython 0.11+, you can install it by editing the IPython configuration file
``~/.ipython/profile_default/ipython_config.py`` to add the ``'line_profiler'``
item to the extensions list::

    c.TerminalIPythonApp.extensions = [
        'line_profiler',
    ]

Or explicitly call::

    %load_ext line_profiler

To get usage help for %lprun, use the standard IPython help mechanism::

    In [1]: %lprun?
"""

from io import StringIO

from IPython.core.magic import Magics, magics_class, line_magic
from IPython.core.page import page
from IPython.utils.ipstruct import Struct
from IPython.core.error import UsageError

from .line_profiler import LineProfiler


@magics_class
class LineProfilerMagics(Magics):
    @line_magic
    def lprun(self, parameter_s=""):
        """ Execute a statement under the line-by-line profiler from the
        line_profiler module.

        Usage:

            %lprun -f func1 -f func2 <statement>

        The given statement (which doesn't require quote marks) is run via the
        LineProfiler. Profiling is enabled for the functions specified by the -f
        options. The statistics will be shown side-by-side with the code through the
        pager once the statement has completed.

        Options:

        -f <function>: LineProfiler only profiles functions and methods it is told
        to profile.  This option tells the profiler about these functions. Multiple
        -f options may be used. The argument may be any expression that gives
        a Python function or method object. However, one must be careful to avoid
        spaces that may confuse the option parser.

        -m <module>: Get all the functions/methods in a module

        One or more -f or -m options are required to get any useful results.

        -D <filename>: dump the raw statistics out to a pickle file on disk. The
        usual extension for this is ".lprof". These statistics may be viewed later
        by running line_profiler.py as a script.

        -T <filename>: dump the text-formatted statistics with the code side-by-side
        out to a text file.

        -r: return the LineProfiler object after it has completed profiling.

        -s: strip out all entries from the print-out that have zeros.

        -u: specify time unit for the print-out in seconds.
        """

        # Escape quote markers.
        opts_def = Struct(D=[""], T=[""], f=[], m=[], u=None)
        parameter_s = parameter_s.replace('"', r"\"").replace("'", r"\'")
        opts, arg_str = self.parse_options(parameter_s, "rsf:m:D:T:u:", list_all=True)
        opts.merge(opts_def)

        global_ns = self.shell.user_global_ns
        local_ns = self.shell.user_ns

        # Get the requested functions.
        funcs = []
        for name in opts.f:
            try:
                funcs.append(eval(name, global_ns, local_ns))
            except Exception as e:
                raise UsageError(
                    f"Could not find module {name}.\n{e.__class__.__name__}: {e}"
                )

        profile = LineProfiler(*funcs)

        # Get the modules, too
        for modname in opts.m:
            try:
                mod = __import__(modname, fromlist=[""])
                profile.add_module(mod)
            except Exception as e:
                raise UsageError(
                    f"Could not find module {modname}.\n{e.__class__.__name__}: {e}"
                )

        if opts.u is not None:
            try:
                output_unit = float(opts.u[0])
            except Exception:
                raise TypeError("Timer unit setting must be a float.")
        else:
            output_unit = None

        # Add the profiler to the builtins for @profile.
        import builtins

        if "profile" in builtins.__dict__:
            had_profile = True
            old_profile = builtins.__dict__["profile"]
        else:
            had_profile = False
            old_profile = None
        builtins.__dict__["profile"] = profile

        try:
            try:
                profile.runctx(arg_str, global_ns, local_ns)
                message = ""
            except SystemExit:
                message = """*** SystemExit exception caught in code being profiled."""
            except KeyboardInterrupt:
                message = (
                    "*** KeyboardInterrupt exception caught in code being " "profiled."
                )
        finally:
            if had_profile:
                builtins.__dict__["profile"] = old_profile

        # Trap text output.
        stdout_trap = StringIO()
        profile.print_stats(
            stdout_trap, output_unit=output_unit, stripzeros="s" in opts
        )
        output = stdout_trap.getvalue()
        output = output.rstrip()

        page(output)
        print(message, end="")

        dump_file = opts.D[0]
        if dump_file:
            profile.dump_stats(dump_file)
            print(f"\n*** Profile stats pickled to file {dump_file!r}. {message}")

        text_file = opts.T[0]
        if text_file:
            pfile = open(text_file, "w")
            pfile.write(output)
            pfile.close()
            print(f"\n*** Profile printout saved to text file {text_file!r}. {message}")

        return_value = None
        if "r" in opts:
            return_value = profile

        return return_value
