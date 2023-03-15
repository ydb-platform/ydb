LIBRARY()

PEERDIR(
    library/cpp/archive
    library/cpp/json
    library/cpp/lua
    library/cpp/string_utils/relaxed_escaper
)

ARCHIVE(
    NAME code.inc
    support/pp.lua
)

SRCS(
    config.cpp
    sax.cpp
    value.cpp
    markup.cpp
    markupfsm.h.rl6
    ini.cpp
    domscheme.cpp
)

END()
