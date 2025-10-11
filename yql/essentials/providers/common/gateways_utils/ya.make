LIBRARY()

ENABLE(YQL_STYLE_CPP)

    SRCS(
        gateways_utils.cpp
    )

    PEERDIR(
        yql/essentials/utils
        yql/essentials/providers/common/proto
        yql/essentials/providers/common/provider
    )

END()
