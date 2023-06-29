PY3_LIBRARY()

PEERDIR(
    contrib/python/PyYAML
    library/python/resource
    library/python/testing/style
)

TEST_SRCS(
    conftest.py
    test_cpp.py
)

RESOURCE(
    devtools/ya/handlers/style/style_config /cpp_style/config/12
    devtools/ya/handlers/style/style_config_14 /cpp_style/config/14
)

END()
