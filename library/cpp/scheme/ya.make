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
    library/cpp/json
    library/cpp/protobuf/runtime
    library/cpp/string_utils/relaxed_escaper
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(scheme.h)

END()

RECURSE(
    tests
    ut_utils
    util
)
