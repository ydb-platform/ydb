# A program to instantiate the pystan model .pyx template for a stan model name.

PY3_PROGRAM()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0 AND BSD-3-Clause AND GPL-2.0-only AND GPL-3.0-only AND GPL-3.0-or-later AND LGPL-3.0-only AND LicenseRef-scancode-unknown-license-reference AND MIT)

PEERDIR(
    library/python/resource
    contrib/python/pystan
)

PY_SRCS(
    MAIN
    main.py
)

END()
