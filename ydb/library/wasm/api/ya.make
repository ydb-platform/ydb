LIBRARY()

SRCS(
    allocation_registry.cpp
    bytecode.cpp
    compartment.cpp
    data_transfer.cpp
    function.cpp
    memory_pool.cpp
    type_builder.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/error
    library/cpp/yt/memory
    library/cpp/yt/misc
    util
)

END()
