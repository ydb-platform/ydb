#include "mlp_changer.h"

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
    return new TEvPQ::TEvMLPChangeMessageDeadlineRequest(Settings.TopicName, Settings.Consumer, partitionId, offsets, Settings.Deadline);
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
