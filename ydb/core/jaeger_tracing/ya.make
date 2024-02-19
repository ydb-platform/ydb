LIBRARY()

PEERDIR(
    ydb/core/protos
)

SRCS(
    control.h
    control_wrapper.h
    request_discriminator.h
    request_discriminator.cpp
    sampler.h
    sampling_throttling_configurator.h
    sampling_throttling_configurator.cpp
    sampling_throttling_control.h
    throttler.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
