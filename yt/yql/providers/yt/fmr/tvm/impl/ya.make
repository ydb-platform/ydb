LIBRARY()

IF (NOT OPENSOURCE)
    SRCS(
        yql_yt_fmr_tvm_impl.cpp
    )
    PEERDIR(
        library/cpp/tvmauth/client
    )
ELSE()
    SRCS(
        yql_yt_fmr_no_tvm.cpp
    )
ENDIF()

PEERDIR(
    yt/yql/providers/yt/fmr/tvm/interface
)

YQL_LAST_ABI_VERSION()

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(ut)
ENDIF()
