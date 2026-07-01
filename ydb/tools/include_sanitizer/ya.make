PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cli.py
    common.py
    doctor.py
    pilot.py

    aggregate/__init__.py
    aggregate/graph.py
    aggregate/run.py

    analyze/__init__.py
    analyze/cache.py
    analyze/clang_runner.py
    analyze/header_probe.py
    analyze/per_tu.py
    analyze/schema.py
    analyze/source_includes.py

    compdb/__init__.py
    compdb/generate.py
    compdb/record_cc.py

    report/__init__.py
    report/diff_preview.py
    report/formats.py
    report/run.py
    report/selfcontain.py
    report/summary.py
    report/worklist.py

    timing/__init__.py
    timing/aggregate.py
    timing/collect.py
    timing/parse.py
    timing/run.py
)

END()

RECURSE(
    bin
)

RECURSE_FOR_TESTS(
    tests
)
