LIBRARY()

SRCS(
    wilson_event.cpp
    wilson_span.cpp
    wilson_profile_span.cpp
    wilson_trace.cpp
    wilson_uploader.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    library/cpp/actors/wilson/protos
)

END()

RECURSE(
    protos
)
