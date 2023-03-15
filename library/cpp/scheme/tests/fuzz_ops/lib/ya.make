LIBRARY()

GENERATE_ENUM_SERIALIZATION(vm_defs.h)

SRCS(
    fuzz_ops.cpp
    vm_apply.cpp
    vm_defs.cpp
    vm_parse.cpp
)

PEERDIR(
    library/cpp/scheme
)

END()
