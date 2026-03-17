PY3TEST()

PEERDIR(
    contrib/python/aioftp
    contrib/python/async-timeout
    contrib/python/pytest
    contrib/python/pytest-asyncio
)

NO_LINT()

TEST_SRCS(
    conftest.py
    test_abort.py
#    test_client_side_socks.py  # Requires siosocks
    test_connection.py
    test_corner_cases.py
    test_current_directory.py
    test_directory_actions.py
    test_extra.py
    test_file.py
    test_list_fallback.py
    test_login.py
    test_maximum_connections.py
    test_passive.py
    test_pathio.py
    test_permissions.py
    test_restart.py
    test_simple_functions.py
    test_throttle.py
    test_user.py
)

END()
