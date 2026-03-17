"""
Python shell for mpmath.

This is just a normal Python shell (IPython shell if you have the
IPython package installed), that adds default imports and run
some initialization code.
"""

import argparse
import ast
import atexit
import os
import readline
import rlcompleter
import sys
import tokenize

from mpmath import __version__
from mpmath._interactive import (IntegerDivisionWrapper, wrap_float_literals,
                                 wrap_hexbinfloats)


__all__ = ()


parser = argparse.ArgumentParser(description=__doc__,
                                 prog='python -m mpmath')
parser.add_argument('--no-wrap-division',
                    help="Don't wrap integer divisions with Fraction",
                    action='store_true')
parser.add_argument('--no-ipython', help="Don't use IPython",
                    action='store_true')
parser.add_argument('--no-wrap-floats',
                    help="Don't wrap float/complex literals",
                    action='store_true')
parser.add_argument('-V', '--version',
                    help='Print the mpmath version and exit',
                    action='store_true')
parser.add_argument('--prec', type=int,
                    help='Set default mpmath precision')
parser.add_argument('--no-pretty', help='Disable pretty-printing',
                    action='store_true')
parser.add_argument('--int-limits',
                    help="Enable string conversion length limitation for int's",
                    action='store_true')


def main():
    args, ipython_args = parser.parse_known_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if not args.int_limits:
        sys.set_int_max_str_digits(0)

    lines = ['from mpmath import *',
             'from fractions import Fraction']

    if args.prec:
        lines.append(f'mp.prec = {args.prec}')
    if not args.no_pretty:
        lines.append('mp.pretty = True')
        lines.append('mp.pretty_dps = "repr"')

    try:
        import IPython
        import traitlets
    except ImportError:
        args.no_ipython = True

    if not args.no_ipython:
        config = traitlets.config.loader.Config()
        shell = config.InteractiveShell
        ast_transformers = shell.ast_transformers
        if not args.no_wrap_division:
            ast_transformers.append(IntegerDivisionWrapper())
        shell.confirm_exit = False
        config.TerminalIPythonApp.display_banner = False
        config.TerminalInteractiveShell.autoformatter = None

        app = IPython.terminal.ipapp.TerminalIPythonApp.instance(config=config)
        app.initialize(ipython_args)
        shell = app.shell
        for l in lines:
            shell.run_cell(l, silent=True)
        if not args.no_wrap_floats:
            source = """
from mpmath._interactive import wrap_float_literals, wrap_hexbinfloats
ip = get_ipython()
ip.input_transformers_post.append(wrap_float_literals)
ip.input_transformers_post.append(wrap_float_literals)
del ip
"""
            shell.run_cell(source)
        app.start()
    else:
        ast_transformers = []
        source_transformers = []
        ns = {}

        if not args.no_wrap_division:
            ast_transformers.append(IntegerDivisionWrapper())
        if not args.no_wrap_floats:
            source_transformers.append(wrap_hexbinfloats)
            source_transformers.append(wrap_float_literals)

        try:
            from _pyrepl.main import CAN_USE_PYREPL
            if CAN_USE_PYREPL:  # pragma: no cover
                from _pyrepl.console import \
                    InteractiveColoredConsole as InteractiveConsole
            else:
                raise ImportError
        except ImportError:  # pragma: no cover
            from code import InteractiveConsole

        class MpmathConsole(InteractiveConsole):
            """An interactive console with readline support."""

            def __init__(self, ast_transformers=[],
                         source_transformers=[], **kwargs):
                super().__init__(**kwargs)
                self.ast_transformers = ast_transformers
                self.source_transformers = source_transformers

            def runsource(self, source, filename='<input>', symbol='single'):
                if self.source_transformers:
                    last_line = source.endswith("\n")  # signals the end of a block
                    try:
                        for t in self.source_transformers:
                            source = ''.join(t(source.splitlines(keepends=True)))
                    except SyntaxError:
                        pass  # XXX: emit warning?
                    if last_line:
                        source += "\n"

                try:
                    code = self.compile(source, filename, 'exec')
                except (OverflowError, SyntaxError, ValueError):
                    if sys.version_info >= (3, 13):
                        self.showsyntaxerror(filename, source=source)
                    else:  # pragma: no cover
                        self.showsyntaxerror(filename)
                    return False

                if code is None:
                    return True

                if self.ast_transformers:
                    tree = ast.parse(source)
                    for t in self.ast_transformers:
                        tree = t.visit(tree)
                    ast.fix_missing_locations(tree)
                    source = ast.unparse(tree)
                    source += "\n"

                return super().runsource(source, filename=filename, symbol=symbol)

        c = MpmathConsole(ast_transformers=ast_transformers,
                          source_transformers=source_transformers, locals=ns)

        interactive_hook = getattr(sys, "__interactivehook__", None)
        if interactive_hook is not None:  # pragma: no branch
            sys.audit("cpython.run_interactivehook", interactive_hook)
            interactive_hook()

        for l in lines:
            c.push(l)

        try:
            from _pyrepl.main import CAN_USE_PYREPL
            if CAN_USE_PYREPL:  # pragma: no cover
                from _pyrepl.simple_interact import \
                    run_multiline_interactive_console
                run_multiline_interactive_console(c)
            else:
                raise ImportError
        except Exception:  # pragma: no cover
            c.interact('', '')


if __name__ == '__main__':
    main()
