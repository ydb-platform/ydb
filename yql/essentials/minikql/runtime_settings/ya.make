LIBRARY()

SRCS(
    runtime_settings_configuration.cpp
    runtime_settings_serialization.cpp
    runtime_settings.cpp
)

PEERDIR(
    yql/essentials/minikql/runtime_settings/proto
    yql/essentials/providers/common/config
    yql/essentials/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
