PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/zope.interface
)

SRCDIR(
    contrib/python/zope.interface/py2/zope/interface
)

TEST_SRCS(
    common/tests/__init__.py
    common/tests/basemapping.py
    common/tests/test_collections.py
    common/tests/test_idatetime.py
    common/tests/test_import_interfaces.py
    common/tests/test_numbers.py
    tests/__init__.py
    tests/advisory_testing.py
    tests/dummy.py
    tests/idummy.py
    tests/m1.py
    tests/odd.py
    tests/test_adapter.py
    tests/test_advice.py
    tests/test_declarations.py
    tests/test_document.py
    tests/test_element.py
    tests/test_exceptions.py
    tests/test_interface.py
    tests/test_interfaces.py
    tests/test_odd_declarations.py
    tests/test_registry.py
    tests/test_ro.py
    tests/test_sorting.py
    tests/test_verify.py
)

NO_LINT()

END()
