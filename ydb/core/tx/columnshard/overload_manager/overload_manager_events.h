#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_common_types.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NColumnShard::NOverload {

enum EEvOverload {
    EvOverloadSubscribe = EventSpaceBegin(TKikimrEvents::ES_OVERLOAD_MANAGER),

    EvOverloadUnsubscribe,
    EvOverloadColumnShardDied,
    EvOverloadPipeServerDisconnected,
    EvOverloadResourcesReleased,

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
        , OverloadSubscriberInfo(std::move(overloadSubscriberInfo)) {
    }
};

class TEvOverloadUnsubscribe: public NActors::TEventLocal<TEvOverloadUnsubscribe, EvOverloadUnsubscribe> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);
    YDB_READONLY_DEF(TOverloadSubscriberInfo, OverloadSubscriberInfo);

public:
    TEvOverloadUnsubscribe(TColumnShardInfo&& columnShardInfo, TOverloadSubscriberInfo&& overloadSubscriberInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
        , OverloadSubscriberInfo(std::move(overloadSubscriberInfo)) {
    }
};

class TEvOverloadPipeServerDisconnected: public NActors::TEventLocal<TEvOverloadPipeServerDisconnected, EvOverloadPipeServerDisconnected> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);
    YDB_READONLY_DEF(TPipeServerInfo, PipeServerInfo);

public:
    TEvOverloadPipeServerDisconnected(TColumnShardInfo&& columnShardInfo, TPipeServerInfo&& pipeServerInfo)
        : ColumnShardInfo(std::move(columnShardInfo))
        , PipeServerInfo(std::move(pipeServerInfo)) {
    }
};

class TEvOverloadColumnShardDied: public NActors::TEventLocal<TEvOverloadColumnShardDied, EvOverloadColumnShardDied> {
    YDB_READONLY_DEF(TColumnShardInfo, ColumnShardInfo);

public:
    TEvOverloadColumnShardDied(TColumnShardInfo&& columnShardInfo)
        : ColumnShardInfo(std::move(columnShardInfo)) {
    }
};

class TEvOverloadResourcesReleased: public NActors::TEventLocal<TEvOverloadResourcesReleased, EvOverloadResourcesReleased> {};

} // namespace NKikimr::NColumnShard::NOverload
