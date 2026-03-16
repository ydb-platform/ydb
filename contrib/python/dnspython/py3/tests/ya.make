PY3TEST()

PEERDIR(
    contrib/python/dnspython
    contrib/python/cryptography
)

PY_SRCS(
    NAMESPACE tests
    md_module.py
    stxt_module.py
    ttxt_module.py
    util.py
)

TEST_SRCS(
    test_address.py
    test_async.py
    test_bugs.py
    test_constants.py
    test_dnssec.py
    test_doh.py
    test_edns.py
    test_entropy.py
    test_exceptions.py
    test_flags.py
    test_generate.py
    test_grange.py
    test_immutable.py
    test_message.py
    test_name.py
    test_namedict.py
    test_nsec3.py
    test_nsec3_hash.py
    test_ntoaaton.py
    test_processing_order.py
    test_query.py
    test_rdata.py
    test_rdataset.py
    test_rdtypeandclass.py
    test_rdtypeanydnskey.py
    test_rdtypeanyeui.py
    test_rdtypeanyloc.py
    test_rdtypeanytkey.py
    test_renderer.py
    test_resolution.py
    test_resolver.py
    test_resolver_override.py
    test_rrset.py
    test_rrset_reader.py
    test_serial.py
    test_set.py
    test_svcb.py
    test_tokenizer.py
    test_transaction.py
    test_tsig.py
    test_tsigkeyring.py
    test_ttl.py
    test_update.py
    test_wire.py
    test_xfr.py
    test_zone.py
    test_zonedigest.py
)

DATA(
    arcadia/contrib/python/dnspython/py3/tests
)

NO_LINT()

END()
