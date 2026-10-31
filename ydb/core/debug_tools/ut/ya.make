UNITTEST()

    SIZE(SMALL)
    REQUIREMENTS(cpu:2)

    PEERDIR(
        ydb/core/debug_tools
    )

    SRCS(
        main.cpp
    )
    
END()
