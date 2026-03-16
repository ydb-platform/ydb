PY3TEST()

PEERDIR(
    contrib/python/python-crontab
)

DATA(
    arcadia/contrib/python/python-crontab/tests
)

TEST_SRCS(
    __init__.py
    test_compatibility.py
    test_context.py
    test_croniter.py
    test_description.py
    test_enums.py
    test_env.py
    test_equality.py
    test_every.py
    test_frequency.py
    test_interaction.py
    test_log.py
    test_range.py
    test_removal.py
    test_scheduler.py
    test_system_cron.py
    test_utf8.py
    utils.py
)

NO_LINT()

END()
