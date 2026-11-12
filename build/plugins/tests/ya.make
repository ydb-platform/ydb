PY3TEST()

SUBSCRIBER(g:ymake)

TEST_SRCS(
    test_cmake_export.py
    test_common.py
    test_dart_fields.py
    test_gobuild.py
    test_java.py
    test_nots_checks.py
    test_requirements.py
    test_res.py
    test_yql_prebuilt.py
    test_yql_python_udf.py
    test_ytest.py
)

PEERDIR(
    build/plugins
    build/plugins/lib/proxy
    build/plugins/lib/test_const/proxy
    build/plugins/tests/fake_ymake
    contrib/python/PyYAML
)

DATA(
    arcadia/build/rules/go/extended_lint.yaml
    arcadia/build/rules/go/migrations.yaml
    arcadia/build/rules/go/risky_imports.yaml
)

END()
