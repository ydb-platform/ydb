LIBRARY()

SRCS(
    line_reader.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    util
)

END()

RECURSE(
    models
    tools
)
