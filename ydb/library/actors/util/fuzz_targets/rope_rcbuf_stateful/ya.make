FUZZ()

FUZZ_DICTS(
    ydb/library/actors/util/fuzz_targets/rope_rcbuf_stateful/rope_rcbuf_stateful.dict
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/util
)

END()
