LIBRARY()

    SRCS(
        defs.h
        group_overseer.cpp
        group_overseer.h
        group_state.cpp
        group_state.h
    )

    PEERDIR(
        ydb/core/base

        contrib/libs/t1ha
    )

END()
