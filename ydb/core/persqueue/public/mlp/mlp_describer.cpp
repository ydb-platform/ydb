#include "mlp_describer.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NMLP {

TDescriberActor::TDescriberActor(const TActorId& parentId, const TDescribeSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_DESCRIBER)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TDescriberActor::Bootstrap() {
    DoDescribe();
}

void TDescriberActor::DoDescribe() {
    YDB_LOG_DEBUG("Start describe",
         {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TDescriberActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::SelectRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TDescriberActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle NDescriber::TEvDescribeTopicsResponse",
         {"logPrefix", NPQ_LOG_PREFIX});

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            TopicInfo = std::move(topic);
            auto consumerConfig = GetConsumer(TopicInfo.Info->Description.GetPQTabletConfig(), Settings.Consumer);
            if (!consumerConfig) {
                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Consumer '" << Settings.Consumer << "' does not exist");
            }
            return DoRuntimeAttributes();
        }
        default: {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TDescriberActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TDescriberActor::DoRuntimeAttributes() {
    YDB_LOG_DEBUG("Start DoRuntimeAttributes",
         {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TDescriberActor::RuntimeAttributesState);
    SendToTablet(TopicInfo.Info->Description.GetBalancerTabletID(), new TEvPQ::TEvMLPGetRuntimeAttributesRequest(Settings.TopicName, Settings.Consumer));
}

void TDescriberActor::Handle(TEvPQ::TEvMLPGetRuntimeAttributesResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPGetRuntimeAttributesResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_ev->Get()->Record", ev->Get()->Record});
    auto* result = ev->Get();

    auto response = std::make_unique<TEvDescribeResponse>();
    response->TopicCreated = TInstant::MilliSeconds(TopicInfo.CreateStep);
    response->ApproximateMessageCount = result->GetApproximateMessageCount();
    response->ApproximateDelayedMessageCount = result->GetApproximateDelayedMessageCount();
    response->ApproximateLockedMessageCount = result->GetApproximateLockedMessageCount();

    Send(ParentId, std::move(response));
    PassAway();
}

void TDescriberActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie != Cookie) {
        return;
    }
    YDB_LOG_DEBUG("Handle TEvPipeCache::TEvDeliveryProblem",
         {"logPrefix", NPQ_LOG_PREFIX});
    if (Backoff.HasMore()) {
        Backoff.Next();
        return DoRuntimeAttributes();
    }
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Pipe error");
}

STFUNC(TDescriberActor::RuntimeAttributesState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPGetRuntimeAttributesResponse, Handle);
        hFunc(TEvPQ::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}


void TDescriberActor::Handle(TEvPQ::TEvMLPErrorResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPErrorResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_ev->Get()->Record", ev->Get()->Record});
    ReplyErrorAndDie(ev->Get()->GetStatus(), std::move(ev->Get()->GetErrorMessage()));
}

void TDescriberActor::SendToTablet(ui64 tabletId, IEventBase *ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, ++Cookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

void TDescriberActor::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    YDB_LOG_INFO("Reply error",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_Ydb::StatusIds::StatusCode_Name(errorCode)", Ydb::StatusIds::StatusCode_Name(errorCode)});
    Send(ParentId, new TEvDescribeResponse(errorCode, std::move(errorMessage)));
    PassAway();
}

void TDescriberActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

bool TDescriberActor::OnUnhandledException(const std::exception& exc) {
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR,
        TStringBuilder() <<"Unhandled exception: " << exc.what());
    return TBaseActor::OnUnhandledException(exc);
}

IActor* CreateDescriber(const NActors::TActorId& parentId, TDescribeSettings&& settings) {
    return new TDescriberActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NMLP
