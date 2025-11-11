PY3TEST()

PEERDIR(
    contrib/python/idna
)

TEST_SRCS(
    test_idna_compat.py
    test_idna.py
    test_intranges.py
    test_idna_codec.py
    test_idna_other.py
    test_idna_uts46.py
)

NO_LINT()

FORK_SUBTESTS()

END()
