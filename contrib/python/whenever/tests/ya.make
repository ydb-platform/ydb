PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/whenever
    contrib/python/pytest
    contrib/python/hypothesis
    contrib/python/pydantic/pydantic-2
    contrib/python/tzdata
)

TEST_SRCS(
    common.py
    test_date_delta.py
    test_date.py
    test_datetime_delta.py
    # test_instant.py  # FileNotFoundError
    # test_misc.py  # FileNotFoundError
    test_month_day.py
    # test_offset_datetime.py  # FileNotFoundError
    # test_plain_datetime.py  # FileNotFoundError
    # test_system_datetime.py  # No module named '__tests__.test_offset_datetime'
    test_time_delta.py
    test_time.py
    test_year_month.py
    # test_zoned_datetime.py  # FileNotFoundError
)

END()
