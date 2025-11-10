#include "mlp_dlq_mover.h"

namespace NKikimr::NPQ::NMLP {

TDLQMoverActor::TDLQMoverActor(TDLQMoverSettings&& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_DLQ_MOVER)
    , Settings(std::move(settings))
    , Queue(Settings.Messages)
{
}

void TDLQMoverActor::Bootstrap() {
    Become(&TDLQMoverActor::StateDescribe);
    RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.Database, { Settings.DestinationTopic }));
}

void TDLQMoverActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    auto& topics = ev->Get()->Topics;
    if (topics.size() != 1) {
        return ReplyError("Unexpected describe result");
    }

    auto& topic = topics[Settings.DestinationTopic];

    switch (topic.Status) {
        case NDescriber::EStatus::SUCCESS:
            TopicInfo = std::move(topic);
            return CreateWriter();

        default:
            return ReplyError(NDescriber::Description(Settings.DestinationTopic, topic.Status));
    }
}

NActors::IActor* CreateDLQMover(TDLQMoverSettings&& settings) {
    return new TDLQMoverActor(std::move(settings));
}

}
