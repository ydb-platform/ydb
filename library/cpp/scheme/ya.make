LIBRARY()

SRCS(
    scheme.cpp
    scheme_cast.h
    scimpl.h
    scimpl_defs.h
    scimpl_private.cpp
    scimpl_protobuf.cpp
    scimpl_select.rl6
    scimpl_json_read.cpp
    scimpl_json_write.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    library/cpp/string_utils/relaxed_escaper
)

GENERATE_ENUM_SERIALIZATION(scheme.h)

END()

RECURSE(
    tests
    ut_utils
    util
)
