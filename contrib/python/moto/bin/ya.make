PY3_PROGRAM(moto_server)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/Flask
    contrib/python/Flask-Cors
    contrib/python/moto
)

PY_MAIN(moto.server:main)

END()
