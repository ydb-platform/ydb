LIBRARY()

SRCS(
    GLOBAL normalizer.cpp
    GLOBAL clean_granule.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
)

END()
