LIBRARY()

SRCS(
    inmemory.cpp
    inmemory.h
    lines/on_change_line_frontend.h
    lines/on_change_with_heartbeat_line_frontend.h
    lines/raw_line_frontend.h
)

PEERDIR(
    ydb/library/actors/util
)

END()
