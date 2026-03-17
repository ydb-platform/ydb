PY2TEST()

SIZE(MEDIUM)

FORK_TESTS()

PEERDIR(
    contrib/python/Faker
    contrib/python/freezegun
    contrib/python/validators
    contrib/python/mock
)

DATA(
    arcadia/contrib/python/Faker/py2/tests
)

PY_SRCS(
    NAMESPACE tests
    mymodule/__init__.py
    mymodule/en_US/__init__.py
)

TEST_SRCS(
    __init__.py
    providers/__init__.py
    providers/test_address.py
    providers/test_automotive.py
    providers/test_bank.py
    providers/test_barcode.py
    providers/test_color.py
    providers/test_company.py
    providers/test_credit_card.py
    providers/test_currency.py
    providers/test_date_time.py
    providers/test_file.py
    providers/test_geo.py
    providers/test_internet.py
    providers/test_isbn.py
    providers/test_job.py
    providers/test_misc.py
    providers/test_person.py
    providers/test_phone_number.py
    providers/test_python.py
    providers/test_ssn.py
    providers/test_user_agent.py
    test_base_provider.py
    test_factory.py
    test_generator.py
    test_proxy.py
    utils/__init__.py
    utils/test_utils.py
)

NO_LINT()

END()
