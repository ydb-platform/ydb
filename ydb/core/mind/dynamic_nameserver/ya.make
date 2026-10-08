LIBRARY()

SRCS(
    dynamic_nameserver.cpp
    dynamic_nameserver.h
    dynamic_nameserver_impl.h
    dynamic_nameserver_mon.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/cms/console/events
    ydb/core/mind/events
    ydb/core/mon
    ydb/core/protos
    ydb/core/util
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/services
)

END()
