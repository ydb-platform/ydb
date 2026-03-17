PY3TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/pretend
    contrib/python/httpx
    contrib/python/pytest-asyncio
    contrib/python/requests-mock
    contrib/python/xmlsec
    contrib/python/zeep
)

DATA(
    arcadia/contrib/python/zeep/py3/tests
)

PY_SRCS(
    NAMESPACE tests
    utils.py
)

TEST_SRCS(
    __init__.py
    conftest.py
    integration/test_hello_world_recursive.py
    integration/test_http_post.py
    integration/test_recursive_schema.py
    test_async_client.py
    # test_async_transport.py - need pytest-httpx
    test_cache.py
    test_client.py
    test_client_factory.py
    test_helpers.py
    test_loader.py
    test_main.py
    test_pprint.py
    test_response.py
    test_settings.py
    test_soap_multiref.py
    test_soap_xop.py
    test_transports.py
    test_wsa.py
    test_wsdl.py
    test_wsdl_arrays.py
    test_wsdl_messages_document.py
    test_wsdl_messages_http.py
    test_wsdl_messages_rpc.py
    test_wsdl_no_output_message_part.py
    test_wsdl_soap.py
    test_wsse_signature.py
    test_wsse_username.py
    test_wsse_utils.py
    test_xsd.py
    test_xsd_any.py
    test_xsd_attributes.py
    test_xsd_builtins.py
    test_xsd_complex_types.py
    test_xsd_element.py
    test_xsd_extension.py
    test_xsd_indicators_all.py
    test_xsd_indicators_choice.py
    test_xsd_indicators_group.py
    test_xsd_indicators_sequence.py
    test_xsd_integration.py
    test_xsd_parse.py
    test_xsd_schemas.py
    test_xsd_signatures.py
    test_xsd_simple_types.py
    test_xsd_types.py
    test_xsd_union.py
    test_xsd_validation.py
    test_xsd_valueobjects.py
    test_xsd_visitor.py
)

NO_LINT()

END()
