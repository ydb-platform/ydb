PY3_PROGRAM(distro)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/distro
)

PY_MAIN(distro.distro:main)

END()
