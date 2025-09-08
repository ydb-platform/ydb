#pragma once

#include <util/generic/intrlist.h>
#include <util/generic/hash.h>

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_common_types.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadSubscribers {
public:
    void AddOverloadSubscriber(const TColumnShardInfo& columnShardInfo, const TPipeServerInfo& pipeServerInfo, const TOverloadSubscriberInfo& overloadSubscriberInfo);
    void RemoveOverloadSubscriber(const TColumnShardInfo& columnShardInfo, const TOverloadSubscriberInfo& overloadSubscriberInfo);
    void RemovePipeServer(const TColumnShardInfo& columnShardInfo, const TPipeServerInfo& pipeServerInfo);
    void NotifyAllOverloadSubscribers();
    void NotifyColumnShardSubscribers(const TColumnShardInfo& columnShardInfo);

private:
    struct TSubscriptionInfo {
        TInterconnectSessionId InterconnectSessionId;
        TTabletId ColumnShardTabletId;
        THashMap<TOverloadSubscriberId, TSeqNo> OverloadSubscribers;
    };

    using TInfoByPipeServerId = THashMap<TPipeServerId, TSubscriptionInfo>;
    THashMap<TColumnShardId, TInfoByPipeServerId> ColumnShardsOverloadSubscribers;
};

} // namespace NKikimr::NColumnShard::NOverload
