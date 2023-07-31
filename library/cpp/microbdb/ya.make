LIBRARY()

SRCS(
    align.h
    compressed.h
    extinfo.h
    file.cpp
    hashes.h
    header.h
    header.cpp
    heap.h
    input.h
    microbdb.cpp
    noextinfo.proto
    output.h
    powersorter.h
    reader.h
    safeopen.h
    sorter.h
    sorterdef.h
    utility.h
    wrappers.h
)

PEERDIR(
    contrib/libs/fastlz
    contrib/libs/libc_compat
    contrib/libs/protobuf
    contrib/libs/snappy
    contrib/libs/zlib
    library/cpp/deprecated/fgood
    library/cpp/on_disk/st_hash
    library/cpp/packedtypes
)

END()
