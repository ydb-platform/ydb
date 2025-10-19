#include "mlp_changer.h"

#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NPQ::NMLP {


IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings) {
    return new TChangerActor(parentId, {
        .DatabasePath = std::move(settings.DatabasePath),
        .TopicName = std::move(settings.TopicName),
        .Consumer = std::move(settings.Consumer),
        .Messages = std::move(settings.Messages),
        .RequestCreator = [](const TString& topicName, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets) {
            return new TEvPersQueue::TEvMLPCommitRequest(topicName, consumer, partitionId, offsets);
        }
    });
}

IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSetting&& settings) {
    return new TChangerActor(parentId, {
        .DatabasePath = std::move(settings.DatabasePath),
        .TopicName = std::move(settings.TopicName),
        .Consumer = std::move(settings.Consumer),
        .Messages = std::move(settings.Messages),
        .RequestCreator = [](const TString& topicName, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets) {
            return new TEvPersQueue::TEvMLPUnlockRequest(topicName, consumer, partitionId, offsets);
        }
    });
}

IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSetting&& settings) {
    return new TChangerActor(parentId, {
        .DatabasePath = std::move(settings.DatabasePath),
        .TopicName = std::move(settings.TopicName),
        .Consumer = std::move(settings.Consumer),
        .Messages = std::move(settings.Messages),
        .RequestCreator = [deadline=settings.Deadline](const TString& topicName, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets) {
            return new TEvPersQueue::TEvMLPChangeMessageDeadlineRequest(topicName, consumer, partitionId, deadline, offsets);
        }
    });
}

} // namespace NKikimr::NPQ::NMLP
