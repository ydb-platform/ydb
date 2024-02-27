LIBRARY()

PEERDIR(
    ydb/core/protos
)

SRCS(
    request_discriminator.h
    request_discriminator.cpp
    sampler.h
    sampling_throttling_configurator.cpp
    sampling_throttling_configurator.h
    sampling_throttling_control.cpp
    sampling_throttling_control.h
    sampling_throttling_control_internals.cpp
    sampling_throttling_control_internals.h
    settings.h
    throttler.h
    throttler.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
