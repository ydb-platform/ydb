PY3_PROGRAM(mnc_viewer)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.viewer.main)

    PY_SRCS(
        commands.py
        main.py
        widgets.py
    )

    PEERDIR(
        contrib/python/PyYAML
        contrib/python/rich
        contrib/python/textual
        ydb/tools/mnc/lib
        ydb/tools/mnc/scheme
    )

END()

RECURSE_FOR_TESTS(
    ut
)
