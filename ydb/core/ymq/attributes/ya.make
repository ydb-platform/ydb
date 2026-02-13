LIBRARY()

SRCS(
    attributes_md5.cpp
    attribute_name.cpp
)

PEERDIR(
    library/cpp/digest/md5
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
