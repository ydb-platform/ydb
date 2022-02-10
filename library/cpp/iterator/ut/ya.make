GTEST()

PEERDIR(
    library/cpp/iterator
)

OWNER(g:util) 

SRCS(
    filtering_ut.cpp
    functools_ut.cpp
    iterate_keys_ut.cpp
    iterate_values_ut.cpp
    mapped_ut.cpp
    zip_ut.cpp
)

END()
