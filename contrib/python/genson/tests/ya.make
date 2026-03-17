PY3TEST()
NO_LINT()

PEERDIR(
    contrib/python/genson
    contrib/python/jsonschema
)

TEST_SRCS(
    base.py
    test_add_multi.py
    test_add_single.py
#    test_bin.py
    test_builder.py
    test_custom.py
    test_gen_multi.py
    test_gen_single.py
    test_misuse.py
    test_seed_schema.py
)

END()

