#include "local_partition_reader.h"
#include "local_proxy.h"
#include "logging.h"

#include <ydb/core/persqueue/writer/common.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

namespace NKikimr::NReplication {

TLocalTopicPartitionReaderActor::TLocalTopicPartitionReaderActor(const TActorId& parent, const std::string& database, TEvYdbProxy::TTopicReaderSettings& settings)
    : TBaseLocalTopicPartitionActor(database, std::move(settings.GetBase().Topics_[0].Path_), settings.GetBase().Topics_[0].PartitionIds_[0])
    , Parent(parent)
    , Consumer(std::move(settings.GetBase().ConsumerName_))
    , AutoCommit(settings.AutoCommit_)
{
}

void TLocalTopicPartitionReaderActor::OnDescribeFinished() {
    DoInitOffset();
}

void TLocalTopicPartitionReaderActor::OnError(const TString& error) {
    Send(Parent, CreateError(NYdb::EStatus::UNAVAILABLE, error));
    PassAway();
}

void TLocalTopicPartitionReaderActor::OnFatalError(const TString& error) {
    Send(Parent, CreateError(NYdb::EStatus::SCHEME_ERROR, error));
    PassAway();
}

TString TLocalTopicPartitionReaderActor::MakeLogPrefix() {
    return TStringBuilder() << "Reader[" << SelfId() << ":/" << Database << TopicPath <<" ] ";
}

std::unique_ptr<TEvYdbProxy::TEvTopicReaderGone> TLocalTopicPartitionReaderActor::CreateError(NYdb::EStatus status, const TString& error) {
    NYdb::NIssue::TIssues issues;
    issues.AddIssue(error);
    return std::make_unique<TEvYdbProxy::TEvTopicReaderGone>(NYdb::TStatus(status, std::move(issues)));
}

STATEFN(TLocalTopicPartitionReaderActor::OnInitEvent) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvYdbProxy::TEvReadTopicRequest, HandleInit);
        hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);
    default:
        Y_DEBUG_ABORT_S(TStringBuilder() << "Unhandled message " << ev->GetTypeName());
    }
}

bool GetSkipCommit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
    const auto args = std::move(ev->Get()->GetArgs());
    const auto& settings = std::get<TEvYdbProxy::TReadTopicSettings>(args);
    return settings.SkipCommit_;
}

void TLocalTopicPartitionReaderActor::HandleInit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
    LOG_T("Handle on init " << ev->Get()->ToString());

    RequestsQueue.emplace_back(ev->Sender, ev->Cookie, GetSkipCommit(ev));
}

void TLocalTopicPartitionReaderActor::Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());
}

void TLocalTopicPartitionReaderActor::DoInitOffset() {
    NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateGetOffsetRequest().release());
    Become(&TLocalTopicPartitionReaderActor::StateInitOffset);
}

void TLocalTopicPartitionReaderActor::HandleOnInitOffset(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto& record = ev->Get()->Record;
    if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
        Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup);
        return;
    }
    if (record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        return OnError(TStringBuilder() << "Unimplemented response: " << record.GetErrorReason());
    }
    if (!record.HasPartitionResponse() || !record.GetPartitionResponse().HasCmdGetClientOffsetResult()) {
        return OnError(TStringBuilder() << "Unimplemented response");
    }

    const auto& resp = record.GetPartitionResponse().GetCmdGetClientOffsetResult();
    Offset = resp.GetOffset();
    SentOffset = Offset;

    Send(Parent, new TEvYdbProxy::TEvStartTopicReadingSession(TStringBuilder() << "Session_" << SelfId()));

    DoWork();
}

void TLocalTopicPartitionReaderActor::HandleOnInitOffset(TEvents::TEvWakeup::TPtr& ev) {
    if (static_cast<ui64>(EWakeupType::InitOffset) == ev->Get()->Tag) {
        DoInitOffset();
    }
}

std::unique_ptr<TEvPersQueue::TEvRequest> TLocalTopicPartitionReaderActor::CreateGetOffsetRequest() const {
    auto request = std::make_unique<TEvPersQueue::TEvRequest>();

    auto& req = *request->Record.MutablePartitionRequest();
    req.SetPartition(PartitionId);
    auto& offset = *req.MutableCmdGetClientOffset();
    offset.SetClientId(Consumer);

    return request;
}

STATEFN(TLocalTopicPartitionReaderActor::StateInitOffset) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, HandleOnInitOffset);
        hFunc(TEvents::TEvWakeup, HandleOnInitOffset);

        hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    default:
        OnInitEvent(ev);
    }
}

void TLocalTopicPartitionReaderActor::DoWork() {
    Become(&TLocalTopicPartitionReaderActor::StateWork);

    if (!RequestsQueue.empty()) {
        Handle(RequestsQueue.front());
    }
}

void TLocalTopicPartitionReaderActor::Handle(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    HandleInit(ev);
    Handle(RequestsQueue.front());
}

void TLocalTopicPartitionReaderActor::Handle(ReadRequest& request) {
    Offset = SentOffset;

    if (AutoCommit && !request.SkipCommit) {
        request.SkipCommit = true;
        NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateCommitRequest().release());
    }

    NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateReadRequest().release());

    DoWaitData();
}

std::unique_ptr<TEvPersQueue::TEvRequest> TLocalTopicPartitionReaderActor::CreateReadRequest() const {
    auto request = std::make_unique<TEvPersQueue::TEvRequest>();

    auto& req = *request->Record.MutablePartitionRequest();
    req.SetPartition(PartitionId);
    auto& read = *req.MutableCmdRead();
    read.SetOffset(Offset);
    read.SetClientId(Consumer);
    read.SetTimeoutMs(ReadTimeout.MilliSeconds());
    read.SetBytes(ReadLimitBytes);

    return request;
}

std::unique_ptr<TEvPersQueue::TEvRequest> TLocalTopicPartitionReaderActor::CreateCommitRequest() const {
    auto request = std::make_unique<TEvPersQueue::TEvRequest>();

    auto& req = *request->Record.MutablePartitionRequest();
    req.SetPartition(PartitionId);
    auto& commit = *req.MutableCmdSetClientOffset();
    commit.SetOffset(Offset);
    commit.SetClientId(Consumer);

    return request;
}

STATEFN(TLocalTopicPartitionReaderActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvYdbProxy::TEvReadTopicRequest, Handle);
        hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);

        hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TLocalTopicPartitionReaderActor::DoWaitData() {
    Become(&TLocalTopicPartitionReaderActor::StateWaitData);
}

NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string) {
    NKikimrPQClient::TDataChunk proto;
    bool res = proto.ParseFromString(string);
    Y_ABORT_UNLESS(res, "Got invalid data from PQTablet");
    return proto;
}

void TLocalTopicPartitionReaderActor::HandleOnWaitData(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    const auto& record = ev->Get()->Record;

    TString error;
    if (!NPQ::BasicCheck(record, error)) {
        return OnError(TStringBuilder() << "Wrong read response: " << error);
    }

    if (record.GetPartitionResponse().HasCmdGetClientOffsetResult()) {
        // Skip set offset result for autocommit
        return;
    }

    if (!record.GetPartitionResponse().HasCmdReadResult()) {
        return OnError("Unsupported response from partition");
    }

    const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();

    auto partitionFinishedAndCommitted = readResult.GetReadingFinished() && readResult.GetCommittedToEnd();
    if (partitionFinishedAndCommitted) {
        Send(RequestsQueue.front().Sender, new TEvYdbProxy::TEvEndTopicPartition(
            PartitionId,
            TVector<ui64>(readResult.GetAdjacentPartitionIds().begin(), readResult.GetAdjacentPartitionIds().end()),
            TVector<ui64>(readResult.GetChildPartitionIds().begin(), readResult.GetChildPartitionIds().end())
        ));
    }

    if (!readResult.ResultSize()) {
        if (partitionFinishedAndCommitted) {
            return;
        }
        return Handle(RequestsQueue.front());
    }

    auto request = std::move(RequestsQueue.front());
    RequestsQueue.pop_front();

    auto gotOffset = Offset;
    TVector<NReplication::TTopicMessage> messages(::Reserve(readResult.ResultSize()));

    for (auto& result : readResult.GetResult()) {
        gotOffset = std::max(gotOffset, result.GetOffset());
        auto proto = GetDeserializedData(result.GetData());

        if (proto.has_codec() && proto.codec() != Ydb::Topic::CODEC_RAW - 1) {
            const NYdb::NTopic::ICodec* codecImpl = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(proto.codec() + 1));
            TString decompressed = codecImpl->Decompress(proto.GetData());
            messages.emplace_back(result.GetOffset(), decompressed);
        } else {
            messages.emplace_back(result.GetOffset(), proto.GetData());
        }
    }
    SentOffset = gotOffset + 1;

    Send(request.Sender, new TEvYdbProxy::TEvReadTopicResponse(PartitionId, std::move(messages)), 0, request.Cookie);

    DoWork();
}

STATEFN(TLocalTopicPartitionReaderActor::StateWaitData) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, HandleOnWaitData);

        hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    default:
        OnInitEvent(ev);
    }
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& settings = std::get<TEvYdbProxy::TTopicReaderSettings>(args);

    AFL_VERIFY(1 == settings.GetBase().Topics_.size())("topic count", settings.GetBase().Topics_.size());
    AFL_VERIFY(1 == settings.GetBase().Topics_[0].PartitionIds_.size())("partition count", settings.GetBase().Topics_[0].PartitionIds_.size());

    auto actor = new TLocalTopicPartitionReaderActor(ev->Sender, Database, settings);
    auto reader = RegisterWithSameMailbox(actor);
    Send(ev->Sender, new TEvYdbProxy::TEvCreateTopicReaderResponse(reader), 0, ev->Cookie);
}

}
