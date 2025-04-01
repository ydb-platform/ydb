PROGRAM(arun)

SRCS(
    aggr_external_rh_ht.cpp
    aggr_external_merge.cpp
    aggr_memory_lp_ht.cpp
    aggr_memory_rh_ht.cpp
    aggr_memory_rhi_ht.cpp
    main.cpp
    spilling_mem.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()
