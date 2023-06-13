UNITTEST()

    TIMEOUT(60)
    SIZE(SMALL)

    PEERDIR(
        ydb/core/debug_tools
    )

    SRCS(
        main.cpp
    )
    
END()
