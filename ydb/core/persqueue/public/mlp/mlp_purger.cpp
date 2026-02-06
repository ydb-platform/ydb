#include "mlp_purger.h"

namespace NKikimr::NPQ::NMLP {

TPurgerActor::TPurgerActor(const TActorId& parentId, const TPurgerSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_PURGER)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TPurgerActor::Bootstrap() {
    DoDescribe();
}

void TPurgerActor::DoDescribe() {
    LOG_D("Start describe");
    Become(&TPurgerActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::SelectRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TPurgerActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            ReadBalancerTabletId = topic.Info->Description.GetBalancerTabletID();
            return DoSelectPartition();
        }
        default: {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TPurgerActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

} // namespace NKikimr::NPQ::NMLP
