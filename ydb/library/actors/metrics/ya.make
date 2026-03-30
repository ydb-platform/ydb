LIBRARY()

SRCS(
    inmemory_backend.cpp
    inmemory_backend.h
    line_base.cpp
    line_base.h
    line.h
    line_storage.cpp
    line_storage.h
    lines/on_change_line_frontend.h
    lines/raw_line_frontend.h
)

PEERDIR(
    ydb/library/actors/util
)

END()
