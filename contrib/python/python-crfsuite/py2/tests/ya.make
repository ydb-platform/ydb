PY2TEST()

PEERDIR(
    contrib/python/python-crfsuite
    contrib/libs/crfsuite
)

NO_LINT()

TEST_SRCS(
    conftest.py
    test_itemsequence.py
    test_logparser.py
    test_misc.py
    test_tagger.py
    test_trainer.py
)

END()
