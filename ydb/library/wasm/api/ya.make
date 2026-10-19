LIBRARY()

SRCS(
    bytecode.cpp
    compartment.cpp
    data_transfer.cpp
    function.cpp
    memory_pool.cpp
    type_builder.cpp
    compartment.h  # gn: slot wasm self
    data_transfer.h  # gn: slot wasm self
    function.h  # gn: slot wasm self
    memory_pool.h  # gn: slot wasm self
    type_builder.h  # gn: slot wasm self
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/error
    library/cpp/yt/memory
    library/cpp/yt/misc
    util
)

END()
