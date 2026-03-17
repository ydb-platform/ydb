PY3_LIBRARY()

VERSION(1.4.2+patched)

LICENSE(MIT)

PEERDIR(
    contrib/python/sqlalchemy/sqlalchemy-1.3
    contrib/deprecated/python/alembic
)

PY_SRCS(
    env.py
    versions/19c0846f6344_base_revision_for_0_5.py
    versions/1cebaf56856c_session_id.py
    versions/3ec6993fe20c_encrypted_auth_state.py
    versions/4dc2d5a8c53c_user_options.py
    versions/56cc5a70207e_token_tracking.py
    versions/896818069c98_token_expires.py
    versions/99a28a4418e1_user_created.py
    versions/af4cbdb2d13c_services.py
    versions/d68c98b66cd4_client_description.py
    versions/eeb276e51423_auth_state.py
)

NO_LINT()
NO_CHECK_IMPORTS(
    contrib.python.jupyterhub.jupyterhub.alembic.env  # env imports dynamically, so import test goes mad
)

END()
