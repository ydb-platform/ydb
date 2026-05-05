UNITTEST_FOR(yql/essentials/minikql/runtime_settings)

SRCS(
    runtime_settings_serialization_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf
    yql/essentials/core/credentials
    yql/essentials/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()
