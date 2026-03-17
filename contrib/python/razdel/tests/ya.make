PY3TEST()

PEERDIR(
    contrib/python/razdel
)

SRCDIR(contrib/python/razdel/razdel/tests)

TEST_SRCS(
    __init__.py
    common.py
    conftest.py
    ctl.py
    gen.py
    test_sentenize.py
    test_tokenize.py
    zoo.py
    partition.py
)

RESOURCE_FILES(
    PREFIX contrib/python/razdel/razdel/tests
    data/sents.txt
    data/tokens.txt
)

NO_LINT()

END()
