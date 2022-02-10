LIBRARY()

OWNER(
    ddoarn
    xenoxeno
    g:kikimr
)

SRCS(
    mon.cpp
    mon.h
    crossref.cpp
    crossref.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/lwtrace/mon
    library/cpp/string_utils/url 
    ydb/core/base
    ydb/library/aclib
)

END()
