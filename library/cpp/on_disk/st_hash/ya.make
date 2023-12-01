LIBRARY()

SRCS(
    fake.cpp
    save_stl.h
    static_hash.h
    static_hash_map.h
    sthash_iterators.h
)

PEERDIR(
    library/cpp/deprecated/mapped_file
)

END()
