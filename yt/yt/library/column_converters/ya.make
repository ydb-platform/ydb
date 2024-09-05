LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    boolean_column_converter.cpp
    column_converter.cpp
    floating_point_column_converter.cpp
    helpers.cpp
    integer_column_converter.cpp
    null_column_converter.cpp
    string_column_converter.cpp
)

PEERDIR(
    yt/yt/client
)

END()
