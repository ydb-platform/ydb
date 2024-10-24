GTEST()

SRCS(
    end_of_stream_ut.cpp
    readers_ut.cpp
    yamr_table_reader_ut.cpp

    ut_row.proto
)

PEERDIR(
    yt/cpp/mapreduce/io
)

END()
