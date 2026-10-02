#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_common_types.h>

namespace NKikimr::NColumnShard::NOverload {

enum EEvOverload {
    EvOverloadSubscribe = EventSpaceBegin(TKikimrEvents::ES_OVERLOAD_MANAGER),

    EvOverloadUnsubscribe,
    EvOverloadColumnShardDied,
    EvOverloadPipeServerDisconnected,
    EvOverloadResourcesReleased,
    EvPublishNodeOverloadStatus,
    EvCompactionOverloadState,
    EvSyncNodeOverloadPublication,

    EvEnd
};

class TEvOverloadSubscribe: public NActors::TEventLocal<TEvOverloadSubscribe, EvOverloadSubscribe> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);
    YDB_READONLY_DEF(TPipeServerInfo, PipeServerInfo);
    YDB_READONLY_DEF(TOverloadSubscriberInfo, OverloadSubscriberInfo);

public:
    TEvOverloadSubscribe(TColumnShardInfo&& columnShardInfo, TPipeServerInfo&& pipeServerInfo, TOverloadSubscriberInfo&& overloadSubscriberInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
        , PipeServerInfo(std::move(pipeServerInfo))
        , OverloadSubscriberInfo(std::move(overloadSubscriberInfo))
    {
    }
};

class TEvOverloadUnsubscribe: public NActors::TEventLocal<TEvOverloadUnsubscribe, EvOverloadUnsubscribe> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);
    YDB_READONLY_DEF(TOverloadSubscriberInfo, OverloadSubscriberInfo);

public:
    TEvOverloadUnsubscribe(TColumnShardInfo&& columnShardInfo, TOverloadSubscriberInfo&& overloadSubscriberInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
        , OverloadSubscriberInfo(std::move(overloadSubscriberInfo))
    {
    }
};

class TEvOverloadPipeServerDisconnected: public NActors::TEventLocal<TEvOverloadPipeServerDisconnected, EvOverloadPipeServerDisconnected> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);
    YDB_READONLY_DEF(TPipeServerInfo, PipeServerInfo);

public:
    TEvOverloadPipeServerDisconnected(TColumnShardInfo&& columnShardInfo, TPipeServerInfo&& pipeServerInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
        , PipeServerInfo(std::move(pipeServerInfo))
    {
    }
};

class TEvOverloadColumnShardDied: public NActors::TEventLocal<TEvOverloadColumnShardDied, EvOverloadColumnShardDied> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);

public:
    TEvOverloadColumnShardDied(TColumnShardInfo&& columnShardInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
    {
    }
};

class TEvOverloadResourcesReleased: public NActors::TEventLocal<TEvOverloadResourcesReleased, EvOverloadResourcesReleased> {};

class TEvPublishNodeOverloadStatus: public NActors::TEventLocal<TEvPublishNodeOverloadStatus, EvPublishNodeOverloadStatus> {
    YDB_READONLY_DEF(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus, Status);

public:
    explicit TEvPublishNodeOverloadStatus(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status)
        : Status(status)
    {
    }
};

class TEvCompactionOverloadState: public NActors::TEventLocal<TEvCompactionOverloadState, EvCompactionOverloadState> {
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(bool, Overloaded, false);

public:
    TEvCompactionOverloadState(ui64 tabletId, bool overloaded)
        : TabletId(tabletId)
        , Overloaded(overloaded)
    {
    }
};

class TEvSyncNodeOverloadPublication: public NActors::TEventLocal<TEvSyncNodeOverloadPublication, EvSyncNodeOverloadPublication> {};

}   // namespace NKikimr::NColumnShard::NOverload
