import os
import runpy
import importlib

import __res


cdef env_entry_point = 'Y_PYTHON_ENTRY_POINT'


cdef extern from 'main.h':
    pass


def find_pymain():
    py_main = __res.find('PY_MAIN')

    if isinstance(py_main, bytes):
        py_main = py_main.decode('utf8')

    if isinstance(py_main, unicode):
        return py_main

    return None


def run_main():
    entry_point = os.environ.pop(env_entry_point, None)

    if entry_point is None:
        entry_point = find_pymain()

    if entry_point is None:
        raise RuntimeError('No entry point found')

    module_name, colon, func_name = entry_point.partition(':')

    if not colon:
        runpy._run_module_as_main(module_name, alter_argv=False)
        return

    if not module_name:
        module_name = 'library.python.runtime_py3.entry_points'

    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    func()


run_main()
