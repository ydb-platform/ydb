#include "mlp_dlq_mover.h"

namespace NKikimr::NPQ::NMLP {

namespace {

static constexpr ui64 CacheSubscribeCookie = 1;

}

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

void TDLQMoverActor::PassAway() {
    if (PartitionWriterActorId) {
        Send(PartitionWriterActorId, new TEvents::TEvPoison());
    }

    if (Error) {
        Send(Settings.ParentActorId, new TEvPQ::TEvMLPDLQMoverResponse(Ydb::StatusIds::INTERNAL_ERROR, std::move(Processed), std::move(Error)));
    } else {
        Send(Settings.ParentActorId, new TEvPQ::TEvMLPDLQMoverResponse(Ydb::StatusIds::SUCCESS, std::move(Processed)));
    }

    TActor::PassAway();
}

TString TDLQMoverActor::BuildLogPrefix() const  {
    return TStringBuilder() << "[" << Settings.TabletId<< "][" << Settings.PartitionId << "][DLQ][" << Settings.ConsumerName << "] ";
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

void TDLQMoverActor::CreateWriter() {
    Become(&TDLQMoverActor::StateInit);

    ProducerId = TStringBuilder() << "DLQMover/" << Settings.TabletId << "/" << Settings.PartitionId << "/" << Settings.ConsumerGeneration << "/" << Settings.ConsumerName;
    TString sessionId = TStringBuilder() << "DLQMover/" << SelfId();

    auto& chooser = TopicInfo.Info->PartitionChooser;
    TargetPartition = chooser->GetPartition(ProducerId);
    AFL_ENSURE(TargetPartition)("p", ProducerId);

    TPartitionWriterOpts opts; // TODO request units
    opts.WithDeduplication(true)
        .WithSourceId(ProducerId)
        .WithAutoRegister(true)
        .WithDatabase(Settings.Database)
        .WithTopicPath(TopicInfo.RealPath)
        .WithSessionId(sessionId)
        .WithInitialSeqNo(Settings.FirstMessageSeqNo);

    PartitionWriterActorId = RegisterWithSameMailbox(CreatePartitionWriter(SelfId(), TargetPartition->TabletId, TargetPartition->PartitionId, opts));
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
    const auto* result = ev->Get();

    if (!result->IsSuccess()) {
        return ReplyError(TStringBuilder() << "The error of creating a writer: " << result->GetError().Reason);
    }

    ui64 targetSeqNo = result->GetResult().SourceIdInfo.GetSeqNo();
    for (SeqNo = Settings.FirstMessageSeqNo; SeqNo < targetSeqNo && !Queue.empty(); ++SeqNo) {
        Processed.push_back(Queue.front());
        Queue.pop_front();
    }

    AFL_ENSURE(targetSeqNo == SeqNo)("t", targetSeqNo)("s", SeqNo);

    ProcessQueue();
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvDisconnected::TPtr&) {
    ReplyError("The writer disconnected");
}

void TDLQMoverActor::ProcessQueue() {
    Become(&TDLQMoverActor::StateRead);

    if (Queue.empty()) {
       return ReplySuccess();
    }

    SendToTablet(MakeEvRead(SelfId(), "", Queue.front(), 1, ++FetchCookie, NextPartNo));
}

void TDLQMoverActor::Handle(TEvPQ::TEvProxyResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvProxyResponse");
    if (FetchCookie != GetCookie(ev)) {
        // TODO MLP
        LOG_D("Cookie mismatch: " << FetchCookie << " != " << GetCookie(ev));
        //return;
    }

    if (!IsSucess(ev)) {
        ReplyError(TStringBuilder() << "Fetch message failed: " << ev->Get()->Response->DebugString());
        return;
    }

    auto& response = ev->Get()->Response;
    AFL_ENSURE(response->GetPartitionResponse().HasCmdReadResult())("t", Settings.TabletId)("p", Settings.PartitionId)("c", Settings.ConsumerName);

    auto write = std::make_unique<TEvPartitionWriter::TEvWriteRequest>(++WriteCookie);
    auto* request = write->Record.MutablePartitionRequest();
    request->SetTopic(Settings.DestinationTopic);
    request->SetPartition(TargetPartition->PartitionId);

    auto writeTimeMs = TInstant::Now().MilliSeconds();

    auto currentOffset = Queue.front();
    for (auto& result : *response->MutablePartitionResponse()->MutableCmdReadResult()->MutableResult()) {
        if (currentOffset > result.GetOffset()) {
            continue;
        }
        AFL_ENSURE(currentOffset == result.GetOffset())("l", currentOffset)("r", result.GetOffset());

        if (NextPartNo > result.GetPartNo()) {
            continue;
        }
        AFL_ENSURE(NextPartNo == result.GetPartNo())("l", NextPartNo)("r", result.GetPartNo());

        auto* write = request->AddCmdWrite();
        write->SetSourceId(ProducerId);
        write->SetSeqNo(SeqNo);
        write->SetData(std::move(*result.MutableData()));
        write->SetPartNo(result.GetPartNo());
        write->SetTotalParts(result.GetTotalParts());
        write->SetTotalSize(result.GetTotalSize());
        write->SetCreateTimeMS(result.GetCreateTimestampMS());
        write->SetDisableDeduplication(false);
        write->SetWriteTimeMS(writeTimeMs);
        write->SetUncompressedSize(result.GetUncompressedSize());
        //write->SetClientDC(result.GetC);
        if (result.HasPartitionKey()) {
            write->SetPartitionKey(std::move(*result.MutablePartitionKey()));
            write->SetExplicitHash(std::move(*result.MutableExplicitHash()));
        }

        ++NextPartNo;
        TotalPartNo = result.GetTotalParts();
    }

    Send(PartitionWriterActorId, std::move(write));

    WaitWrite();
}

void TDLQMoverActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    ReplyError("Source topic unavailable");
}

void TDLQMoverActor::WaitWrite() {
    Become(&TDLQMoverActor::StateWrite);
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
    LOG_D("Handle TEvPartitionWriter::TEvWriteResponse");

    auto* result = ev->Get();
    if (!result->IsSuccess()) {
        return ReplyError(TStringBuilder() << "Write error: " << result->GetError().Reason);
    }

    if (NextPartNo == TotalPartNo) {
        Processed.push_back(Queue.front());
        Queue.pop_front();

        NextPartNo = 0;
    }

    ProcessQueue();
}

void TDLQMoverActor::ReplySuccess() {
    PassAway();
}

void TDLQMoverActor::ReplyError(TString&& error) {
    Error = std::move(error);
    PassAway();
}

void TDLQMoverActor::SendToTablet(std::unique_ptr<IEventBase> ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), Settings.TabletId, FirstRequest, CacheSubscribeCookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
    FirstRequest = false;
}

STFUNC(TDLQMoverActor::StateDescribe) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateDescribe", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateDescribe", ev));
    }
}

STFUNC(TDLQMoverActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPartitionWriter::TEvInitResult, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateDescribe", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateDescribe", ev));
    }
}

STFUNC(TDLQMoverActor::StateRead) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvProxyResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateDescribe", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateDescribe", ev));
    }
}

STFUNC(TDLQMoverActor::StateWrite) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateDescribe", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateDescribe", ev));
    }
}

NActors::IActor* CreateDLQMover(TDLQMoverSettings&& settings) {
    return new TDLQMoverActor(std::move(settings));
}

}
