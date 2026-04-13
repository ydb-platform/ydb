LIBRARY()

SRCS(
    inmemory_backend.cpp
    inmemory_backend.h
    line.h
    line_impl.h
    line_read.cpp
    line_read.h
    line_storage.cpp
    line_storage.h
    line_types.cpp
    line_types.h
    line_write.h
    lines/on_change_line_frontend.h
    lines/raw_line_frontend.h
)

PEERDIR(
    ydb/library/actors/util
)

END()
