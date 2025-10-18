#include "mlp_reader.h"

#define LOG_PREFIX_INT TStringBuilder() << "[" << SelfId() << "]"
#define LOG_D(stream) LOG_DEBUG_S (*NActors::TlsActivationContext, NKikimrServices::EServiceKikimr::PQ_MLP_READER, LOG_PREFIX_INT << stream)
#define LOG_I(stream) LOG_INFO_S  (*NActors::TlsActivationContext, NKikimrServices::EServiceKikimr::PQ_MLP_READER, LOG_PREFIX_INT << stream)
#define LOG_E(stream) LOG_ERROR_S (*NActors::TlsActivationContext, NKikimrServices::EServiceKikimr::PQ_MLP_READER, LOG_PREFIX_INT << stream)


namespace NKikimr::NPQ::NMLP {

TReaderActor::TReaderActor(const TActorId& parentId, const TReaderSetting& settings)
    : ParentId(parentId)
    , Settings(settings)
{
}

void TReaderActor::Bootstrap() {
    DoDescribe();
}

void TReaderActor::DoDescribe() {
    LOG_D("Start describe");
    Become(&TReaderActor::DescribeState);
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }));
}

void TReaderActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            ReadBalancerTabletId = topic.Info->Description.GetBalancerTabletID();
            DoSelectPartition();
            break;
        }
        default: {
            ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR,
                NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TReaderActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TReaderActor::DoSelectPartition() {
    LOG_D("Start select partition");
    Become(&TReaderActor::SelectPartitionState);
    SendToTablet(ReadBalancerTabletId, new TEvPersQueue::TEvMLPGetPartitionRequest(Settings.TopicName, Settings.Consumer));
}

void TReaderActor::Handle(TEvPersQueue::TEvMLPGetPartitionResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPGetPartitionResponse " << ev->Get()->Record.ShortDebugString());
    auto* result = ev->Get();
    switch (result->GetErrorCode()) {
        case NPersQueue::NErrorCode::EErrorCode::OK: {
            PartitionId = result->GetPartitionId();
            PQTabletId = result->GetTabletId();
            DoRead();
        }
        default:
            ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode::ERROR, "Patition choose error");
    }
}

void TReaderActor::HandleOnSelectPartition(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie != Cookie) {
        return;
    }
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    if (Backoff.HasMore()) {
        Backoff.Next();
        return DoSelectPartition();
    }
    ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode::ERROR, "Pipe error");
}

STFUNC(TReaderActor::SelectPartitionState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPGetPartitionResponse, Handle);
        hFunc(TEvPersQueue::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnSelectPartition);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TReaderActor::DoRead() {
    LOG_D("Start read");
    Become(&TReaderActor::ReadState);
    SendToTablet(PQTabletId, new TEvPersQueue::TEvMLPReadRequest(Settings.TopicName, Settings.Consumer, PartitionId,
        Settings.WaitTime, Settings.VisibilityTimeout, Settings.MaxNumberOfMessage));
}

void TReaderActor::Handle(TEvPersQueue::TEvMLPReadResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPReadResponse");
    Forward(ev, ParentId);
    PassAway();
}

void TReaderActor::Handle(TEvPersQueue::TEvMLPErrorResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPErrorResponse " << ev->Get()->GetErrorMessage());
    Forward(ev, ParentId);
    PassAway();
}

void TReaderActor::HandleOnRead(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie != Cookie) {
        return;
    }
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    if (Backoff.HasMore()) {
        Backoff.Next();
        return DoRead();
    }
    ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode::ERROR, "Pipe error");
}

STFUNC(TReaderActor::ReadState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadResponse, Handle);
        hFunc(TEvPersQueue::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnRead);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}


void TReaderActor::SendToTablet(ui64 tabletId, IEventBase *ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, ++Cookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

void TReaderActor::ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode errorCode, TString&& errorMessage) {
    LOG_I("Reply error " << NPersQueue::NErrorCode::EErrorCode_Name(errorCode));
    Send(ParentId, new TEvPersQueue::TEvMLPErrorResponse(errorCode, std::move(errorMessage)));
    PassAway();
}

void TReaderActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
}

bool TReaderActor::OnUnhandledException(const std::exception& exc) {
    LOG_E("unhandled exception " << TypeName(exc) << ": " << exc.what() << Endl
        << TBackTrace::FromCurrentException().PrintToString());

    ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode::ERROR,
        TStringBuilder() <<"Unhandled exception: " << exc.what());
    return true;
}

IActor* CreateReader(const NActors::TActorId& parentId, TReaderSetting&& settings) {
    return new TReaderActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NMLP
