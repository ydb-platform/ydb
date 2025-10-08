LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    stream_from_vector.cpp
)

PEERDIR(
    yql/essentials/public/purecalc/common
)

YQL_LAST_ABI_VERSION()

END()
