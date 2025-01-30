UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    main.cpp
    self_heal_actor_ut.cpp
    defs.h
    env.h
    events.h
    node_warden_mock.h
    timer_actor.h
    vdisk_mock.h
)

PEERDIR(
    ydb/apps/version
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/blobstorage/nodewarden
    ydb/core/mind
    ydb/core/blobstorage/pdisk/mock
    ydb/core/mind/bscontroller
    ydb/core/tx/scheme_board
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
