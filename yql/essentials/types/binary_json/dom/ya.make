YQL_LIBRARY()

SRCDIR(yql/essentials/types/binary_json)

SRCS(
    read_dom.cpp
    write_dom.cpp
)

PEERDIR(
    library/cpp/json
    yql/essentials/minikql/dom
    yql/essentials/types/binary_json
)

END()
