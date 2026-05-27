#include "local_partition_actor.h"
#include "local_proxy.h"
#include "logging.h"

#include <ydb/core/persqueue/writer/common.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

class TLocalTopicPartitionReaderActor: public TBaseLocalTopicPartitionActor {
    using TBase = TBaseLocalTopicPartitionActor;

    constexpr static TDuration ReadTimeout = TDuration::Seconds(1);
    constexpr static ui64 ReadLimitBytes = 1_MB;

    struct TReadRequest {
        TActorId Sender;
        ui64 Cookie;
        bool SkipCommit;
    };

public:
    TLocalTopicPartitionReaderActor(
            const TActorId& parent,
            const std::string& database,
            TEvYdbProxy::TTopicReaderSettings& settings)
        : TBaseLocalTopicPartitionActor(
            database,
            settings.GetBase().Topics_[0].Path_,
            settings.GetBase().Topics_[0].PartitionIds_[0]
        )
        , Parent(parent)
        , Consumer(std::move(settings.GetBase().ConsumerName_))
        , AutoCommit(settings.AutoCommit_)
    {
    }

protected:
    void OnDescribeFinished() override {
        DoInitOffset();
    }

    void OnError(const TString& error) override {
        Send(Parent, MakeError(NYdb::EStatus::UNAVAILABLE, error));
        PassAway();
    }

    void OnFatalError(const TString& error) override {
        Send(Parent, MakeError(NYdb::EStatus::SCHEME_ERROR, error));
        PassAway();
    }

    TString MakeLogPrefix() override {
        return TStringBuilder() << "Reader[" << SelfId() << ":/" << Database << TopicPath <<" ] ";
    }

    static std::unique_ptr<TEvYdbProxy::TEvTopicReaderGone> MakeError(NYdb::EStatus status, const TString& error) {
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(error);
        return std::make_unique<TEvYdbProxy::TEvTopicReaderGone>(NYdb::TStatus(status, std::move(issues)));
    }

    STATEFN(OnInitEvent) override {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvReadTopicRequest, HandleInit);
            hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);
        default:
            Y_DEBUG_ABORT_S(TStringBuilder() << "Unhandled message " << ev->GetTypeName());
        }
    }

private:
    static bool GetSkipCommit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        const auto args = std::move(ev->Get()->GetArgs());
        const auto& settings = std::get<TEvYdbProxy::TReadTopicSettings>(args);
        return settings.SkipCommit_;
    }

    void HandleInit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        LOG_T("Handle on init " << ev->Get()->ToString());
        RequestsQueue.emplace_back(ev->Sender, ev->Cookie, GetSkipCommit(ev));
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
    }

private:
    void DoInitOffset() {
        NTabletPipe::SendData(SelfId(), PartitionPipeClient, MakeGetOffsetRequest().release());
        Become(&TLocalTopicPartitionReaderActor::StateInitOffset);
    }

    void HandleOnInitOffset(TEvPersQueue::TEvResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto& record = ev->Get()->Record;
        if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup(static_cast<ui64>(EWakeupType::InitOffset)));
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

    void HandleOnInitOffset(TEvents::TEvWakeup::TPtr& ev) {
        if (static_cast<ui64>(EWakeupType::InitOffset) == ev->Get()->Tag) {
            DoInitOffset();
        }
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeGetOffsetRequest() const {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(PartitionId);
        auto& offset = *req.MutableCmdGetClientOffset();
        offset.SetClientId(Consumer);

        return request;
    }

    STATEFN(StateInitOffset) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, HandleOnInitOffset);
            hFunc(TEvents::TEvWakeup, HandleOnInitOffset);

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

private:
    void DoWork() {
        Become(&TLocalTopicPartitionReaderActor::StateWork);

        if (!RequestsQueue.empty()) {
            Handle(RequestsQueue.front());
        }
    }

    void Handle(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        HandleInit(ev);
        Handle(RequestsQueue.front());
    }

    void Handle(TReadRequest& request) {
        Offset = SentOffset;

        if (AutoCommit && !request.SkipCommit) {
            request.SkipCommit = true;
            NTabletPipe::SendData(SelfId(), PartitionPipeClient, MakeCommitRequest().release());
        }

        NTabletPipe::SendData(SelfId(), PartitionPipeClient, MakeReadRequest().release());

        DoWaitData();
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeReadRequest() const {
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

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeCommitRequest() const {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(PartitionId);
        auto& commit = *req.MutableCmdSetClientOffset();
        commit.SetOffset(Offset);
        commit.SetClientId(Consumer);

        return request;
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvReadTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoWaitData() {
        Become(&TLocalTopicPartitionReaderActor::StateWaitData);
    }

    static NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string) {
        NKikimrPQClient::TDataChunk proto;
        bool res = proto.ParseFromString(string);
        Y_ABORT_UNLESS(res, "Got invalid data from PQTablet");
        return proto;
    }

    void HandleOnWaitData(TEvPersQueue::TEvResponse::TPtr& ev) {
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
        TVector<TTopicMessage> messages(::Reserve(readResult.ResultSize()));

        for (auto& result : readResult.GetResult()) {
            gotOffset = std::max(gotOffset, result.GetOffset());
            auto proto = GetDeserializedData(result.GetData());

            auto messageMeta = MakeIntrusive<NYdb::NTopic::TMessageMeta>();
            for (auto& v : *proto.MutableMessageMeta()) {
                messageMeta->Fields.emplace_back(std::move(*v.mutable_key()), std::move(*v.mutable_value()));
            }

            NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation information(
                gotOffset,
                result.GetSourceId(), // producerId
                result.GetSeqNo(),
                TInstant::MilliSeconds(result.GetCreateTimestampMS()),
                TInstant::MilliSeconds(result.GetWriteTimestampMS()),
                nullptr, // write session meta
                messageMeta,
                result.GetUncompressedSize(),
                result.GetSourceId() // messageGroupId
            );

            TString data;
            bool isCompressed = proto.has_codec() && proto.codec() != Ydb::Topic::CODEC_RAW - 1;
            if (isCompressed && !AppData()->FeatureFlags.GetTransferInternalDataDecompression()) {
                const auto* codec = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(proto.codec() + 1));
                data = codec->Decompress(proto.GetData());
                isCompressed = false;
            } else {
                data = std::move(*proto.MutableData());
            }

            if (isCompressed) {
                messages.emplace_back(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage(
                    static_cast<NYdb::NTopic::ECodec>(proto.codec() + 1), std::move(data), information, nullptr
                ));
            } else {
                messages.emplace_back(std::move(information), std::move(data));
            }
        }

        SentOffset = gotOffset + 1;

        Send(request.Sender, new TEvYdbProxy::TEvReadTopicResponse(PartitionId, std::move(messages)), 0, request.Cookie);

        DoWork();
    }

    STATEFN(StateWaitData) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, HandleOnWaitData);

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

private:
    const TActorId Parent;
    const TString Consumer;
    const bool AutoCommit;

    std::deque<TReadRequest> RequestsQueue;

    ui64 Offset = 0;
    ui64 SentOffset = 0;

}; // TLocalTopicPartitionReaderActor

void TLocalProxyActor::Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& settings = std::get<TEvYdbProxy::TTopicReaderSettings>(args);

    const auto& topics = settings.GetBase().Topics_;
    AFL_VERIFY(1 == topics.size())("topic count", topics.size());
    AFL_VERIFY(1 == topics.at(0).PartitionIds_.size())("partition count", topics.at(0).PartitionIds_.size());

    auto reader = RegisterWithSameMailbox(new TLocalTopicPartitionReaderActor(ev->Sender, Database, settings));
    Send(ev->Sender, new TEvYdbProxy::TEvCreateTopicReaderResponse(reader), 0, ev->Cookie);
}

}
