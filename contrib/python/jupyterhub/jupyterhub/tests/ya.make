PY3_LIBRARY()

LICENSE(MIT)

VERSION(1.0.0)

NO_LINT()

PEERDIR(
    contrib/python/beautifulsoup4
    contrib/python/jupyterhub
    contrib/python/pytest
    contrib/python/requests-mock
)

SRCDIR(contrib/python/jupyterhub)

PY_SRCS(
    TOP_LEVEL
    jupyterhub/tests/__init__.py
    jupyterhub/tests/conftest.py
    jupyterhub/tests/mocking.py
    jupyterhub/tests/mockserverapp.py
    jupyterhub/tests/mockservice.py
    jupyterhub/tests/mocksu.py
    jupyterhub/tests/populate_db.py
    jupyterhub/tests/test_api.py
    jupyterhub/tests/test_app.py
    jupyterhub/tests/test_auth.py
    jupyterhub/tests/test_auth_expiry.py
    jupyterhub/tests/test_crypto.py
    jupyterhub/tests/test_db.py
    jupyterhub/tests/test_dummyauth.py
    jupyterhub/tests/test_eventlog.py
    jupyterhub/tests/test_internal_ssl_connections.py
    jupyterhub/tests/test_metrics.py
    jupyterhub/tests/test_named_servers.py
    jupyterhub/tests/test_objects.py
    jupyterhub/tests/test_orm.py
    jupyterhub/tests/test_pages.py
    jupyterhub/tests/test_pagination.py
    jupyterhub/tests/test_proxy.py
    jupyterhub/tests/test_services.py
    jupyterhub/tests/test_services_auth.py
    jupyterhub/tests/test_singleuser.py
    jupyterhub/tests/test_spawner.py
    jupyterhub/tests/test_traitlets.py
    jupyterhub/tests/test_user.py
    jupyterhub/tests/test_utils.py
    jupyterhub/tests/test_version.py
    jupyterhub/tests/utils.py
)

END()
