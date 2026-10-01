#include "mlp_dlq_mover.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/ymq/actor/serviceid.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NMLP {

namespace {

static constexpr ui64 CacheSubscribeCookie = 1;
static constexpr ui64 MaxPendingMessagesSize = 100_MB;

}

TDLQMoverActor::TDLQMoverActor(TDLQMoverSettings&& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_DLQ_MOVER)
    , Settings(std::move(settings))
    , Queue(Settings.Messages)
{
}

void TDLQMoverActor::Bootstrap() {
    Become(&TDLQMoverActor::StateDescribe);
    if (Settings.DestinationTopic.StartsWith("sqs://")) {
        auto tokensStr = Settings.DestinationTopic.substr("sqs://"sv.size());
        auto tokens = StringSplitter(tokensStr).Split('/').ToList<TString>();
        if (tokens.size() != 3) {
            return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected SQS destination topic format: " << Settings.DestinationTopic);
        }

        SQSUserName = tokens[0];
        SQSFolderId = tokens[1];
        SQSQueueName = tokens[2];

        this->Send(NSQS::MakeSqsServiceID(this->SelfId().NodeId()),
            MakeHolder<NSQS::TSqsEvents::TEvGetConfiguration>(
                TStringBuilder() << "DLQMover/" << Settings.TabletId << "/" << Settings.PartitionId << "/" << SelfId(),
                SQSUserName,
                SQSQueueName,
                SQSFolderId,
                false,
                0)
        );
    } else {
        TopicName = Settings.DestinationTopic;
        RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.Database, { TopicName }));
    }
    LOG_D(
        "Dump QUEUE, queueItems",
        {"QUEUE", Queue.size()},
        {"queueItems", JoinRange(", ", Queue.begin(), Queue.end())}
    );
}

void TDLQMoverActor::PassAway() {
    if (PartitionWriterActorId) {
        Send(PartitionWriterActorId, new TEvents::TEvPoison());
    }

    Send(Settings.ParentActorId, new TEvPQ::TEvMLPDLQMoverResponse(ResponseStatus, std::move(Processed), std::move(Error)));
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

    TActor::PassAway();
}

TStructuredMessage TDLQMoverActor::BuildLogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE(
        {"tabletId", Settings.TabletId},
        {"partition", Settings.PartitionId},
        {"consumer", Settings.ConsumerName});
}

void TDLQMoverActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    auto& topics = ev->Get()->Topics;
    if (topics.size() != 1) {
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected describe result");
    }

    auto& topic = topics[TopicName];

    switch (topic.Status) {
        case NDescriber::EStatus::Success:
            TopicInfo = std::move(topic);
            return CreateWriter();

        default:
            LOG_D(NDescriber::Description(Settings.DestinationTopic, topic.Status));
            return ReplyError(NDescriber::Convert(topic.Status), NDescriber::Description(Settings.DestinationTopic, topic.Status));
    }
}

void TDLQMoverActor::Handle(NSQS::TSqsEvents::TEvConfiguration::TPtr& ev) {
    LOG_D("Handle NSQS::TSqsEvents::TEvConfiguration");
    const auto& result = *ev->Get();
    if (!result.UserExists || !result.QueueExists) {
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "SQS DLQ '" << Settings.DestinationTopic << "' queue does not exist");
    }

    if (!result.TopicCreated) {
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "SQS DLQ '" << Settings.DestinationTopic << "' queue is not initialized");
    }

    auto queueName = result.QueueName;
    auto queueVersion = result.QueueVersion;

    TopicName = Join("/", AppData()->SqsConfig.GetRoot(), SQSUserName, queueName, TStringBuilder() << "v" << queueVersion, "streamImpl");
    LOG_D(
        "SQS topic",
        {"name", TopicName}
    );
    RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.Database, { TopicName }));
}

void TDLQMoverActor::CreateWriter() {
    LOG_D("Writer creating");
    Become(&TDLQMoverActor::StateInit);

    ProducerId = TStringBuilder() << "DLQMover/" << Settings.TabletId
        << "/" << Settings.PartitionId
        << "/" << Settings.ConsumerGeneration
        << "/" << Settings.ConsumerName;
    TString sessionId = TStringBuilder() << "DLQMover/" << Settings.TabletId << "/" << SelfId();

    auto& chooser = TopicInfo.Info->PartitionChooser;
    TargetPartition = chooser->GetPartition(ProducerId);
    AFL_ENSURE(TargetPartition)("p", ProducerId);

    TPartitionWriterOpts opts; // TODO request units
    opts.WithDeduplication(true)
        .WithSourceId(ProducerId)
        .WithAutoRegister(true)
        .WithDatabase(Settings.Database)
        .WithTopicPath(TopicInfo.RealPath)
        .WithSessionId(sessionId);

    PartitionWriterActorId = RegisterWithSameMailbox(CreatePartitionWriter(SelfId(), TargetPartition->TabletId, TargetPartition->PartitionId, opts));
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
    LOG_D("Handle TEvPartitionWriter::TEvInitResult");

    const auto* result = ev->Get();
    if (!result->IsSuccess()) {
        LOG_E(
            "The error of creating a",
            {"writer", result->GetError().Reason}
        );
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "The error of creating a writer: " << result->GetError().Reason);
    }

    ui64 targetSeqNo = result->GetResult().SourceIdInfo.GetSeqNo() + 1;
    while (!Queue.empty() && targetSeqNo > Queue.front().SeqNo) {
        Processed.emplace_back(Queue.front().Offset, Queue.front().SeqNo);
        Queue.pop_front();
    }

    if (Queue.empty()) {
        return ReplySuccess();
    }

    Become(&TDLQMoverActor::StateWork);
    ProcessQueue();
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvDisconnected::TPtr&) {
    LOG_D("Handle TEvPartitionWriter::TEvDisconnected");
    ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "The writer disconnected");
}

void TDLQMoverActor::AppendProcessed(const TDLQMessage& message) {
    Processed.emplace_back(message.Offset, message.SeqNo);
}

void TDLQMoverActor::ReleaseResolvedPrefix() {
    while (!Pending.empty() && !Pending.front().WriteInFlight) {
        AppendProcessed(Pending.front().Message);
        Pending.pop_front();
    }
}

void TDLQMoverActor::ProcessQueue() {
    if (PendingMessagesSize >= MaxPendingMessagesSize || Queue.empty()) {
        return;
    }

    LOG_D(
        "ProcessQueue",
        {"size", Queue.size()},
        {"pendingMessagesSize", PendingMessagesSize}
    );
    SendToPQTablet(MakeEvPQRead(Settings.ConsumerName, Settings.PartitionId, Queue.front().Offset, 1));
}

void TDLQMoverActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvResponse");

    if (!IsSucess(ev)) {
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Fetch message failed: " << ev->Get()->Record.DebugString());
    }

    auto& response = ev->Get()->Record;
    const bool hasResult = response.GetPartitionResponse().HasCmdReadResult()
        && response.GetPartitionResponse().GetCmdReadResult().ResultSize() > 0;
    const ui64 requestedOffset = Queue.front().Offset;
    // A trimmed offset is read from StartOffset, so the result can be a later message.
    // That body belongs to another offset. The requested one is already gone.
    const bool gotRequested = hasResult
        && response.GetPartitionResponse().GetCmdReadResult().GetResult(0).GetOffset() == requestedOffset;
    if (!gotRequested) {
        LOG_D(
            "Source message is missing, treat as moved",
            {"offset", requestedOffset},
            {"seqNo", Queue.front().SeqNo},
            {"hasResult", hasResult}
        );
        const TDLQMessage message = Queue.front();
        Queue.pop_front();
        if (Pending.empty()) {
            AppendProcessed(message);
        } else {
            Pending.push_back({.Message = message, .Size = 0, .WriteInFlight = false});
        }
        if (Queue.empty() && Pending.empty()) {
            return ReplySuccess();
        }
        return ProcessQueue();
    }
    auto* result = response.MutablePartitionResponse()->MutableCmdReadResult()->MutableResult(0);
    auto messageSize = result->GetData().size();

    LOG_D(
        "Move message with offset seqNo",
        {"offset", result->GetOffset()},
            {"queueFrontSeqNo",
        Queue.front().SeqNo}
    );

    auto writeRequest = std::make_unique<TEvPartitionWriter::TEvWriteRequest>(++WriteCookie);
    auto* request = writeRequest->Record.MutablePartitionRequest();
    request->SetTopic(Settings.DestinationTopic);

    auto* write = request->AddCmdWrite();
    write->SetSourceId(ProducerId);
    write->SetSeqNo(Queue.front().SeqNo);
    write->SetData(std::move(*result->MutableData()));
    write->SetCreateTimeMS(result->GetCreateTimestampMS());
    write->SetUncompressedSize(result->GetUncompressedSize());
    if (result->HasPartitionKey()) {
        write->SetPartitionKey(std::move(*result->MutablePartitionKey()));
        write->SetExplicitHash(std::move(*result->MutableExplicitHash()));
    }

    Send(PartitionWriterActorId, std::move(writeRequest));

    Pending.push_back({.Message = Queue.front(), .Size = messageSize, .WriteInFlight = true});
    Queue.pop_front();

    PendingMessagesSize += messageSize;
    ProcessQueue();
}

void TDLQMoverActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "Source topic unavailable");
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr&) {
    LOG_D("Handle TEvPartitionWriter::TEvWriteAccepted");
}

void TDLQMoverActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
    LOG_D("Handle TEvPartitionWriter::TEvWriteResponse");

    auto* result = ev->Get();
    if (!result->IsSuccess()) {
        LOG_E(
            "Write error",
            {"error", result->GetError().Reason}
        );
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Write error: " << result->GetError().Reason);
    }

    if (Pending.empty() || !Pending.front().WriteInFlight) {
        return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Write error: unexpected result");
    }

    const ui64 messageSize = Pending.front().Size;
    AppendProcessed(Pending.front().Message);
    Pending.pop_front();
    ReleaseResolvedPrefix();

    LOG_D(
        "Dump NPQLOGPREFIX, queue, pending, processed",
        {"queue", Queue.size()},
        {"pending", Pending.size()},
        {"processed", Processed.size()}
    );

    bool processingPaused = PendingMessagesSize >= MaxPendingMessagesSize;
    AFL_ENSURE(PendingMessagesSize >= messageSize)
        ("PendingMessagesSize", PendingMessagesSize)
        ("messageSize", messageSize);
    PendingMessagesSize -= messageSize;
    if (Queue.empty() && Pending.empty()) {
        return ReplySuccess();
    }
    if (processingPaused) {
        ProcessQueue();
    }
}

void TDLQMoverActor::ReplySuccess() {
    ResponseStatus = Ydb::StatusIds::SUCCESS;
    Error = "";

    PassAway();
}

void TDLQMoverActor::ReplyError(Ydb::StatusIds::StatusCode status, TString&& error) {
    ResponseStatus = status;
    Error = std::move(error);

    PassAway();
}

void TDLQMoverActor::SendToPQTablet(std::unique_ptr<IEventBase> ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), Settings.TabletId, FirstRequest, CacheSubscribeCookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
    FirstRequest = false;
}

STFUNC(TDLQMoverActor::StateDescribe) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        hFunc(NSQS::TSqsEvents::TEvConfiguration, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E(
                "Unexpected",
                {"event", EventStr("StateDescribe", ev)}
            );
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateDescribe", ev));
    }
}

STFUNC(TDLQMoverActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPartitionWriter::TEvInitResult, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E(
                "Unexpected",
                {"event", EventStr("StateInit", ev)}
            );
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateInit", ev));
    }
}

STFUNC(TDLQMoverActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPartitionWriter::TEvWriteAccepted, Handle);
        hFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E(
                "Unexpected",
                {"event", EventStr("StateWork", ev)}
            );
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateWork", ev));
    }
}

NActors::IActor* CreateDLQMover(TDLQMoverSettings&& settings) {
    return new TDLQMoverActor(std::move(settings));
}

}
