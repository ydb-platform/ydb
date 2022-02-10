PY3TEST()

WITHOUT_LICENSE_TEXTS()

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

PEERDIR(
    contrib/restricted/boost/libs/python/arcadia_test/mod
)

TEST_SRCS(test_hello.py)

END()
