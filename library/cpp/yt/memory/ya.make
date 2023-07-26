LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    blob.cpp
    chunked_input_stream.cpp
    chunked_memory_allocator.cpp
    chunked_memory_pool.cpp
    chunked_memory_pool_output.cpp
    chunked_output_stream.cpp
    memory_tag.cpp
    ref.cpp
    ref_tracked.cpp
    safe_memory_reader.cpp
    shared_range.cpp
)

PEERDIR(
    library/cpp/sanitizer/include
    library/cpp/yt/assert
    library/cpp/yt/misc
    library/cpp/yt/malloc
)

CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY ALL
    build
    contrib
    library
    util
    library/cpp/yt/assert
    library/cpp/yt/misc
    library/cpp/yt/malloc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
