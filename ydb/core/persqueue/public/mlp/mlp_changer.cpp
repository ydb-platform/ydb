#include "mlp_changer.h"

#include <util/generic/overloaded.h>

#define Service TBase::Service
#define LogBuilder TBase::LogBuilder

namespace NKikimr::NPQ::NMLP {

template<>
TEvPQ::TEvMLPCommitRequest* TChangerActor<TEvPQ::TEvMLPCommitRequest, TEvPQ::TEvMLPCommitResponse, TCommitterSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    return new TEvPQ::TEvMLPCommitRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets);
}

template<>
TEvPQ::TEvMLPUnlockRequest* TChangerActor<TEvPQ::TEvMLPUnlockRequest, TEvPQ::TEvMLPUnlockResponse, TUnlockerSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    return new TEvPQ::TEvMLPUnlockRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets);
}

template<>
TEvPQ::TEvMLPChangeMessageDeadlineRequest* TChangerActor<TEvPQ::TEvMLPChangeMessageDeadlineRequest, TEvPQ::TEvMLPChangeMessageDeadlineResponse,  TMessageDeadlineChangerSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    // Pair each partition-local offset with the deadline of the matching Settings.Messages entry.
    // Do not forward the full Settings.Deadlines vector (sizes diverge on multi-partition batches).
    std::vector<TInstant> deadlines;
    deadlines.reserve(offsets.size());
    size_t searchFrom = 0;
    for (ui64 offset : offsets) {
        bool found = false;
        for (size_t i = searchFrom; i < Settings.Messages.size(); ++i) {
            const auto& messageId = Settings.Messages[i];
            if (messageId.PartitionId == partitionId && messageId.Offset == offset) {
                AFL_ENSURE(i < Settings.Deadlines.size())
                    ("i", i)
                    ("deadlines", Settings.Deadlines.size());
                deadlines.push_back(Settings.Deadlines[i]);
                searchFrom = i + 1;
                found = true;
                break;
            }
        }
        AFL_ENSURE(found)
            ("partitionId", partitionId)
            ("offset", offset);
    }
    return new TEvPQ::TEvMLPChangeMessageDeadlineRequest(
        Settings.TopicName, Settings.Consumer, partitionId, offsets, deadlines);
}

IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings) {
    return new TChangerActor<TEvPQ::TEvMLPCommitRequest, TEvPQ::TEvMLPCommitResponse, TCommitterSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_COMMITTER);
}

IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSettings&& settings) {
    return new TChangerActor<TEvPQ::TEvMLPUnlockRequest, TEvPQ::TEvMLPUnlockResponse, TUnlockerSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_UNLOCKER);
}

IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSettings&& settings) {
    return new TChangerActor<TEvPQ::TEvMLPChangeMessageDeadlineRequest, TEvPQ::TEvMLPChangeMessageDeadlineResponse,  TMessageDeadlineChangerSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_DEADLINER);
}

} // namespace NKikimr::NPQ::NMLP
