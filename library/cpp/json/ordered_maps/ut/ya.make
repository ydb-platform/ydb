UNITTEST_FOR(library/cpp/json/ordered_maps)


PEERDIR(
    library/cpp/string_utils/relaxed_escaper
)

SRCS(
    json_value_ordered_ut.cpp
    json_reader_fast_ordered_ut.cpp
    json_reader_nan_ordered_ut.cpp
    json_reader_ordered_ut.cpp
    json_writer_ordered_ut.cpp
    json_ordered_ut.cpp
)

END()
