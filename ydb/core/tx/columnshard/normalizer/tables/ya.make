LIBRARY()

# gn: move into parent

SRCS(
    GLOBAL normalizer.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
)

END()
