#include "mlp_changer.h"

#include <ydb/core/persqueue/events/global.h>

#define Service TBase::Service
#define LogBuilder TBase::LogBuilder

namespace NKikimr::NPQ::NMLP {

template<>
TEvPersQueue::TEvMLPCommitRequest* TChangerActor<TEvPersQueue::TEvMLPCommitRequest, TEvPersQueue::TEvMLPCommitResponse, TCommitterSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    return new TEvPersQueue::TEvMLPCommitRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets);
}

template<>
TEvPersQueue::TEvMLPUnlockRequest* TChangerActor<TEvPersQueue::TEvMLPUnlockRequest, TEvPersQueue::TEvMLPUnlockResponse, TUnlockerSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    return new TEvPersQueue::TEvMLPUnlockRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets);
}

template<>
TEvPersQueue::TEvMLPChangeMessageDeadlineRequest* TChangerActor<TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, TEvPersQueue::TEvMLPChangeMessageDeadlineResponse,  TMessageDeadlineChangerSettings>::CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets) {
    return new TEvPersQueue::TEvMLPChangeMessageDeadlineRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets, Settings.Deadline);
}



IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings) {
    return new TChangerActor<TEvPersQueue::TEvMLPCommitRequest, TEvPersQueue::TEvMLPCommitResponse, TCommitterSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_COMMITTER);
}

IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSettings&& settings) {
    return new TChangerActor<TEvPersQueue::TEvMLPUnlockRequest, TEvPersQueue::TEvMLPUnlockResponse, TUnlockerSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_UNLOCKER);
}

IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSettings&& settings) {
    return new TChangerActor<TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, TEvPersQueue::TEvMLPChangeMessageDeadlineResponse,  TMessageDeadlineChangerSettings>(parentId, std::move(settings), NKikimrServices::EServiceKikimr::PQ_MLP_DEADLINER);
}

} // namespace NKikimr::NPQ::NMLP
