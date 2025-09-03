#pragma once

#include <util/generic/intrlist.h>
#include <util/generic/hash.h>

#include <ydb/core/tx/columnshard/overload/reject_reason.h>
#include <ydb/core/tx/columnshard/overload/common_types.h>
#include <ydb/library/actors/core/actor.h>

#include <optional>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadSubscribers {
public:
    void AddOverloadSubscriber(const TColumnShardInfo& columnShardInfo, const TPipeServerInfo& pipeServerInfo, const TOverloadSubscriberInfo& overloadSubscriberInfo);
    void RemoveOverloadSubscriber(const TColumnShardInfo& columnShardInfo, const TOverloadSubscriberInfo& overloadSubscriberInfo);
    void RemovePipeServer(const TColumnShardInfo& columnShardInfo, const TPipeServerInfo& pipeServerInfo);
    void NotifyAllOverloadSubscribers();

    template <typename TResponseRecord>
    void SetOverloadSubscribed(const std::optional<TSeqNo>& overloadSubscribe, const TActorId& recipient, const TActorId& sender, const ERejectReasons rejectReasons, TResponseRecord& responseRecord) {
        if (rejectReasons == ERejectReasons::None || !overloadSubscribe || !HasPipeServer(recipient)) {
            return;
        }

        TSeqNo seqNo = overloadSubscribe.value();
        auto allowed = ERejectReasons::YellowChannels | ERejectReasons::OverloadByShardWritesInFly | ERejectReasons::OverloadByShardWritesSizeInFly;
        if ((rejectReasons & allowed) != ERejectReasons::None &&
            (rejectReasons - allowed) == ERejectReasons::None) {
            if (AddOverloadSubscriber(recipient, sender, seqNo, rejectReasons)) {
                responseRecord.SetOverloadSubscribed(seqNo);
            }
        }
    }

    void ScheduleNotification(const TActorId& actorId);
    void ProcessNotification();

private:
    struct TPipeServerInfoOverloadSubscribersTag {};

    struct TOverloadSubscriber {
        TSeqNo SeqNo = 0;
    };

    struct TPipeServerInfo1
        : public TIntrusiveListItem<TPipeServerInfo1, TPipeServerInfoOverloadSubscribersTag> {
        TPipeServerInfo1() = default;

        NActors::TActorId InterconnectSession;
        THashMap<NActors::TActorId, TOverloadSubscriber> OverloadSubscribers;
    };

    struct TInfo {
        TInterconnectSessionId InterconnectSessionId;
        TTabletId ColumnShardTabletId;
        THashMap<TOverloadSubscriberId, TSeqNo> OverloadSubscribers;
    };

    THashMap<TColumnShardId, THashMap<TPipeServerId, TInfo>> ColumnShardsOverloadSubscribers;

    THashMap<TActorId, TPipeServerInfo> ColumnShardsPipeServers1;

    void DiscardOverloadSubscribers(TPipeServerInfo1& pipeServer);
    bool HasPipeServer(const TActorId& pipeServerId);
    bool AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, TSeqNo seqNo, ERejectReasons reasons);

    THashMap<NActors::TActorId, TPipeServerInfo1> PipeServers;

    using TPipeServersWithOverloadSubscribers = TIntrusiveList<TPipeServerInfo1, TPipeServerInfoOverloadSubscribersTag>;
    TPipeServersWithOverloadSubscribers PipeServersWithOverloadSubscribers;
    size_t OverloadSubscribersByReason[RejectReasonCount] = {0};
    bool InFlightNotification = false;
};

} // namespace NKikimr::NColumnShard::NOverload
