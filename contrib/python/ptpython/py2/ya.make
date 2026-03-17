PY2_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(0.41)

PEERDIR(
    contrib/python/docopt
    contrib/python/jedi
    contrib/python/prompt-toolkit
    contrib/python/Pygments
    contrib/python/six
)

NO_CHECK_IMPORTS(
    ptpython.contrib.asyncssh_repl
    ptpython.ipython
)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    ptpython/__init__.py
    ptpython/completer.py
    ptpython/contrib/__init__.py
    ptpython/contrib/asyncssh_repl.py
    ptpython/entry_points/__init__.py
    ptpython/entry_points/run_ptipython.py
    ptpython/entry_points/run_ptpython.py
    ptpython/eventloop.py
    ptpython/filters.py
    ptpython/history_browser.py
    ptpython/ipython.py
    ptpython/key_bindings.py
    ptpython/layout.py
    ptpython/prompt_style.py
    ptpython/python_input.py
    ptpython/repl.py
    ptpython/style.py
    ptpython/utils.py
    ptpython/validator.py
)

RESOURCE_FILES(
    PREFIX contrib/python/ptpython/py2/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
)

END()

RECURSE(
    bin
)
