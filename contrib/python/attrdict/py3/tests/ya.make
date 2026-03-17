PY3TEST()

PEERDIR(
    contrib/python/attrdict
)

TEST_SRCS(
    test_attrdefault.py
    test_attrdict.py
    test_attrmap.py
    test_common.py
    test_depricated.py
    test_merge.py
    test_mixins.py
)

NO_LINT()

END()
