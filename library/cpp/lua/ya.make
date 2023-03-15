LIBRARY()

PEERDIR(
    library/cpp/json
    contrib/libs/lua
    library/cpp/string_utils/ztstrbuf
)

SRCS(
    eval.cpp
    json.cpp
    wrapper.cpp
)

END()
