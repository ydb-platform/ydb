YQL_LIBRARY()

    SRCS(
        gateways_utils.cpp
    )

    PEERDIR(
        yql/essentials/utils
        yql/essentials/providers/common/activation
        yql/essentials/providers/common/proto
        yql/essentials/providers/common/provider
    )

PEERDIR_FROM_POLICY(
    ALLOW
    logos/projects/ads/mldwh/querygen
)

END()
