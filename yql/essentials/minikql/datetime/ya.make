LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    datetime.cpp
)

PEERDIR(
    yql/essentials/minikql/computation
)

YQL_LAST_ABI_VERSION()

END()
