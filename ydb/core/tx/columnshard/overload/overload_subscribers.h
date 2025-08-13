#pragma once

#include <util/generic/intrlist.h>
#include <util/generic/hash.h>
#include <ydb/library/actors/core/actor.h>

#include "reject_reason.h"

#include <optional>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadSubscribers {
public:
    using TSeqNo = ui64;

    void AddPipeServer(const NActors::TActorId& serverId, const NActors::TActorId& interconnectSession);
    void RemovePipeServer(const NActors::TActorId& serverId);
    void NotifyOverloadSubscribers(ERejectReason reason, const TActorId& sourceActorId, ui64 sourceTabletId);
    void NotifyAllOverloadSubscribers(const TActorId& sourceActorId, ui64 sourceTabletId);
    void RemoveOverloadSubscriber(TSeqNo seqNo, const TActorId& recipient, const TActorId& sender);

    template <typename TResponseRecord>
    void SetOverloadSubscribed(const std::optional<TSeqNo>& overloadSubscribe, const TActorId& recipient, const TActorId& sender, const ERejectReasons rejectReasons, TResponseRecord& responseRecord) {
        if (rejectReasons == ERejectReasons::None || !overloadSubscribe || !HasPipeServer(recipient)) {
            return;
        }

        TSeqNo seqNo = overloadSubscribe.value();
        auto allowed = ERejectReasons::YellowChannels;
        if ((rejectReasons & allowed) != ERejectReasons::None &&
            (rejectReasons - allowed) == ERejectReasons::None) {
            if (AddOverloadSubscriber(recipient, sender, seqNo, rejectReasons)) {
                responseRecord.SetOverloadSubscribed(seqNo);
            }
        }
    }

private:
    struct TPipeServerInfoOverloadSubscribersTag {};

    struct TOverloadSubscriber {
        TSeqNo SeqNo = 0;
        ERejectReasons Reasons = ERejectReasons::None;
    };

    struct TPipeServerInfo
        : public TIntrusiveListItem<TPipeServerInfo, TPipeServerInfoOverloadSubscribersTag> {
        TPipeServerInfo() = default;

        NActors::TActorId InterconnectSession;
        THashMap<NActors::TActorId, TOverloadSubscriber> OverloadSubscribers;
    };

    void DiscardOverloadSubscribers(TPipeServerInfo& pipeServer);
    bool HasPipeServer(const TActorId& pipeServerId);
    bool AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, TSeqNo seqNo, ERejectReasons reasons);

    THashMap<NActors::TActorId, TPipeServerInfo> PipeServers;

    using TPipeServersWithOverloadSubscribers = TIntrusiveList<TPipeServerInfo, TPipeServerInfoOverloadSubscribersTag>;
    TPipeServersWithOverloadSubscribers PipeServersWithOverloadSubscribers;
    size_t OverloadSubscribersByReason[RejectReasonCount] = {0};
};

} // namespace NKikimr::NColumnShard::NOverload
