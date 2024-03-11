OWNER(g:ymake)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    TOP_LEVEL
    _common.py
    _requirements.py
    _xsyn_includes.py
    bundle.py
    coverage.py
    cp.py
    cpp_style.py
    create_init_py.py
    credits.py
    docs.py
    files.py
    gobuild.py
    ios_app_settings.py
    ios_assets.py
    java.py
    large_files.py
    linker_script.py
    lj_archive.py
    llvm_bc.py
    macros_with_error.py
    nots.py
    pybuild.py
    res.py
    suppressions.py
    yql_python_udf.py
    ytest.py
)

PEERDIR(
    build/plugins/lib/proxy
    build/plugins/lib/test_const/proxy
)

END()

RECURSE(
    tests
    lib
    lib/nots
    lib/proxy
    lib/test_const
    lib/test_const/proxy
    lib/tests/ruff
)
