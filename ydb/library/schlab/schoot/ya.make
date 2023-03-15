LIBRARY()

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/protobuf/json
    ydb/library/schlab/protos
)

SRCS(
    defs.h
    schoot_gen.cpp
    schoot_gen.h
    schoot_gen_cfg.cpp
    schoot_gen_cfg.h
)

END()

RECURSE()
