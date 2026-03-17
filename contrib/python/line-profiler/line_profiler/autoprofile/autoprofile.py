"""

AutoProfile Script Demo
=======================

The following demo is end-to-end bash code that writes a demo script and
profiles it with autoprofile.

.. code:: bash

    # Write demo python script to disk
    python -c "if 1:
        import textwrap
        text = textwrap.dedent(
            '''
            def plus(a, b):
                return a + b

            def fib(n):
                a, b = 0, 1
                while a < n:
                    a, b = b, plus(a, b)

            def main():
                import math
                import time
                start = time.time()

                print('start calculating')
                while time.time() - start < 1:
                    fib(10)
                    math.factorial(1000)
                print('done calculating')

            main()
            '''
        ).strip()
        with open('demo.py', 'w') as file:
            file.write(text)
    "

    echo "---"
    echo "## Profile With AutoProfile"
    python -m kernprof -p demo.py -l demo.py
    python -m line_profiler -rmt demo.py.lprof
"""

import types
from .ast_tree_profiler import AstTreeProfiler
from .line_profiler_utils import add_imported_function_or_module

PROFILER_LOCALS_NAME = 'prof'


def _extend_line_profiler_for_profiling_imports(prof):
    """Allow profiler to handle functions/methods, classes & modules with a single call.

    Add a method to LineProfiler that can identify whether the object is a
    function/method, class or module and handle it's profiling accordingly.
    Mainly used for profiling objects that are imported.
    (Workaround to keep changes needed by autoprofile separate from base LineProfiler)

    Args:
        prof (LineProfiler):
            instance of LineProfiler.
    """
    prof.add_imported_function_or_module = types.MethodType(add_imported_function_or_module, prof)


def run(script_file, ns, prof_mod, profile_imports=False):
    """Automatically profile a script and run it.

    Profile functions, classes & modules specified in prof_mod without needing to add
    @profile decorators.

    Args:
        script_file (str):
            path to script being profiled.

        ns (dict):
            "locals" from kernprof scope.

        prof_mod (List[str]):
            list of imports to profile in script.
            passing the path to script will profile the whole script.
            the objects can be specified using its dotted path or full path (if applicable).

        profile_imports (bool):
            if True, when auto-profiling whole script, profile all imports aswell.
    """
    tree_profiled = AstTreeProfiler(script_file, prof_mod, profile_imports).profile()

    _extend_line_profiler_for_profiling_imports(ns[PROFILER_LOCALS_NAME])
    code_obj = compile(tree_profiled, script_file, 'exec')
    exec(code_obj, ns, ns)
