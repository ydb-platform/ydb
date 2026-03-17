import itertools
import sys
import time
import traceback

import pyperf


PYPY = (pyperf.python_implementation() == 'pypy')
DUMMY_SRC_NAME = "<timeit-src>"

# Don't change the indentation of the template; the reindent() calls
# in Timer.__init__() depend on setup being indented 4 spaces and stmt
# being indented 8 spaces.
TEMPLATE = """
def inner(_it, _timer{init}):
    {setup}
    _t0 = _timer()
    for _i in _it:
        {stmt}
    _t1 = _timer()
    {teardown}
    return _t1 - _t0
"""

PYPY_TEMPLATE = """
def inner(_it, _timer{init}):
    {setup}
    _t0 = _timer()
    while _it > 0:
        _it -= 1
        {stmt}
    _t1 = _timer()
    {teardown}
    return _t1 - _t0
"""


def reindent(src, indent):
    return src.replace("\n", "\n" + " " * indent)


class Timer:
    def __init__(self, stmt="pass", setup="pass", teardown="pass",
                 globals=None):
        self.local_ns = {}
        self.global_ns = {} if globals is None else globals
        self.filename = DUMMY_SRC_NAME

        init = ''
        if isinstance(setup, str):
            # Check that the code can be compiled outside a function
            compile(setup, self.filename, "exec")
            full = setup + '\n'
            setup = reindent(setup, 4)
        elif callable(setup):
            self.local_ns['_setup'] = setup
            init += ', _setup=_setup'
            full = ''
            setup = '_setup()'
        else:
            raise ValueError("setup is neither a string nor callable")

        if isinstance(stmt, str):
            # Check that the code can be compiled outside a function
            compile(full + stmt, self.filename, "exec")
            full = full + stmt + '\n'
            stmt = reindent(stmt, 8)
        elif callable(stmt):
            self.local_ns['_stmt'] = stmt
            init += ', _stmt=_stmt'
            full = ''
            stmt = '_stmt()'
        else:
            raise ValueError("stmt is neither a string nor callable")

        if isinstance(teardown, str):
            # Check that the code can be compiled outside a function
            compile(full + teardown, self.filename, "exec")
            teardown = reindent(teardown, 4)
        elif callable(teardown):
            self.local_ns['_teardown'] = teardown
            init += ', _teardown=_teardown'
            teardown = '_teardown()'
        else:
            raise ValueError("teardown is neither a string nor callable")

        if PYPY:
            template = PYPY_TEMPLATE
        else:
            template = TEMPLATE
        src = template.format(stmt=stmt, setup=setup, init=init,
                              teardown=teardown)
        self.src = src  # Save for traceback display

    def make_inner(self):
        # PyPy tweak: recompile the source code each time before
        # calling inner(). There are situations like Issue #1776
        # where PyPy tries to reuse the JIT code from before,
        # but that's not going to work: the first thing the
        # function does is the "-s" statement, which may declare
        # new classes (here a namedtuple). We end up with
        # bridges from the inner loop; more and more of them
        # every time we call inner().
        code = compile(self.src, self.filename, "exec")
        global_ns = dict(self.global_ns)
        local_ns = dict(self.local_ns)
        exec(code, global_ns, local_ns)
        return local_ns["inner"]

    def update_linecache(self):
        import linecache

        linecache.cache[self.filename] = (len(self.src),
                                          None,
                                          self.src.split("\n"),
                                          self.filename)

    def time_func(self, loops):
        inner = self.make_inner()
        timer = time.perf_counter
        if not PYPY:
            it = itertools.repeat(None, loops)
            return inner(it, timer)
        else:
            # PyPy
            return inner(loops, timer)


def strip_statements(statements):
    result = []
    for stmt in statements:
        stmt = stmt.rstrip()
        if stmt:
            result.append(stmt)
    return result


def format_statements(statements):
    return ' '.join(repr(stmt) for stmt in statements)


def create_timer(stmt, setup, teardown, globals):
    # Include the current directory, so that local imports work (sys.path
    # contains the directory of this script, rather than the current
    # directory)
    import os
    sys.path.insert(0, os.curdir)

    stmt = "\n".join(stmt)
    setup = "\n".join(setup)
    teardown = "\n".join(teardown)

    return Timer(stmt, setup, teardown, globals=globals)


def display_error(timer, stmt, setup, teardown):
    print("Error when running timeit benchmark:")
    print()

    print("Statement:")
    for expr in stmt:
        print(repr(expr))
    print()

    if setup:
        print("Setup:")
        for expr in setup:
            print(repr(expr))
        print()

    if teardown:
        print("Teardown:")
        for expr in teardown:
            print(repr(expr))
        print()

    if timer is not None:
        timer.update_linecache()

    traceback.print_exc()


def bench_timeit(runner, name, stmt, setup, teardown,
                 inner_loops=None, duplicate=None,
                 func_metadata=None, globals=None):

    if isinstance(stmt, str):
        stmt = (stmt,)
    if isinstance(setup, str):
        setup = (setup,)
    if isinstance(teardown, str):
        teardown = (teardown,)

    stmt = strip_statements(stmt)
    setup = strip_statements(setup)
    teardown = strip_statements(teardown)

    if not stmt:
        raise ValueError("need at least one statement")

    metadata = {}
    if func_metadata:
        metadata.update(func_metadata)
    if setup:
        metadata['timeit_setup'] = format_statements(setup)
    if teardown:
        metadata['timeit_teardown'] = format_statements(teardown)
    metadata['timeit_stmt'] = format_statements(stmt)

    orig_stmt = stmt

    # args must not be modified, it's passed to the worker process,
    # so use local variables.
    if duplicate and duplicate > 1:
        stmt = stmt * duplicate
        if inner_loops:
            inner_loops *= duplicate
        else:
            inner_loops = duplicate
        metadata['timeit_duplicate'] = duplicate

    kwargs = {'metadata': metadata}
    if inner_loops:
        kwargs['inner_loops'] = inner_loops

    timer = None
    try:
        timer = create_timer(stmt, setup, teardown, globals)
        runner.bench_time_func(name, timer.time_func, **kwargs)
    except SystemExit:
        raise
    except:   # noqa: E722
        display_error(timer, orig_stmt, setup, teardown)
        sys.exit(1)
