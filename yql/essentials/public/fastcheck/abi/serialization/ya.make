LIBRARY()

SRCS(
    serialization.cpp
    message.cpp
    result.cpp
)

PEERDIR(
    yql/essentials/public/abi_impl
    yql/essentials/public/fastcheck/abi/protos
    yql/essentials/public/issue
    yql/essentials/public/langver
    yql/essentials/public/udf_meta/interface
)

END()
