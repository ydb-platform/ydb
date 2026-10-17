
LIBRARY()

# gn: move into parent

SRCS(
    gd_builder.cpp
    gd_entry.cpp
)

PEERDIR(
    library/cpp/containers/comptrie
    library/cpp/string_utils/relaxed_escaper
)

END()

RECURSE_FOR_TESTS(
    ut
)
