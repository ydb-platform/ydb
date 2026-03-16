PY3_PROGRAM(readelf)

VERSION(Service-proxy-version)

LICENSE(
    Public-Domain OR
    Unlicense
)

PEERDIR(
     contrib/python/pyelftools
)

PY_MAIN(readelf.readelf:main)

END()
