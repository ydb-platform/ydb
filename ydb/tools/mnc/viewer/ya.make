PY3_PROGRAM(mnc_viewer)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.viewer.main)

    PY_SRCS(
        main.py
    )

    PEERDIR(
        contrib/python/PyYAML
        contrib/python/rich
        contrib/python/textual
    )

END()

RECURSE_FOR_TESTS(
    ut
)
