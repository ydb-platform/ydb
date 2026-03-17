PY3_PROGRAM(aws)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/awscli
)

PY_MAIN(awscli.__main__)

NO_LINT()

END()
