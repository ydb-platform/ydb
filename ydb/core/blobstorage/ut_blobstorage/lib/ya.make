LIBRARY()

OWNER(g:kikimr)

SRCS(
    activity.h
    common.h
    defs.h
    env.h
    node_warden_mock_bsc.cpp
    node_warden_mock.h
    node_warden_mock_pipe.cpp
    node_warden_mock_state.cpp
    node_warden_mock_state.h
    node_warden_mock_vdisk.h
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk/mock
    ydb/core/blobstorage/vdisk/common
    ydb/core/mind/bscontroller
    ydb/core/tx/scheme_board
    ydb/core/util
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

END()
