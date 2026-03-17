PY3TEST()

PEERDIR(
    contrib/python/inflect
)

DATA(
    arcadia/contrib/python/inflect/py3/tests
)

TEST_SRCS(
    test_an.py
    test_classical_all.py
    test_classical_ancient.py
    test_classical_herd.py
    test_classical_names.py
    test_classical_person.py
    test_classical_zero.py
    test_compounds.py
    test_inflections.py
    test_join.py
    test_numwords.py
    test_pl_si.py
    test_pwd.py
    test_unicode.py
)

NO_LINT()

END()
