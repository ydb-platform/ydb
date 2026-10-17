LIBRARY()

# gn: move into parent

SRCS(
    GLOBAL version.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
)

END()
