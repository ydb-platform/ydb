PY2TEST()

PEERDIR(
    contrib/python/dnspython
    contrib/deprecated/python/typing
)

PY_SRCS(
    NAMESPACE tests
    ttxt_module.py
)

TEST_SRCS(
    test_bugs.py
    test_dnssec.py
    test_edns.py
    test_exceptions.py
    test_flags.py
    test_generate.py
    test_grange.py
    test_message.py
    test_namedict.py
    test_name.py
    test_nsec3.py
    test_ntoaaton.py
    test_rdata.py
    test_rdtypeandclass.py
    test_rdtypeanydnskey.py
    test_rdtypeanyeui.py
    test_rdtypeanyloc.py
    test_resolver.py
    test_rrset.py
    test_set.py
    test_tokenizer.py
    test_update.py
    test_wiredata.py
    test_zone.py
)

DATA(
    arcadia/contrib/python/dnspython/py2/tests
)

NO_LINT()

END()
