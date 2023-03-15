LIBRARY()

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    ydb/library/schlab/probes
)

SRCS(
    bin_log.h
    cbs.h
    cbs_bin.h
    cbs_state.h
    defs.h
    job.h
    job_kind.h
    job_state.h
    name_table.h
    scheduler.h
    schlog.h
    schlog_frame.h
    cbs.cpp
    cbs_bin.cpp
    cbs_state.cpp
    job.cpp
    job_kind.cpp
    job_state.cpp
    scheduler.cpp
    schlog.cpp
)

END()
