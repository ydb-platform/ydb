#include "ydb_proxy.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/scheme_cache/helpers.h>

namespace NKikimr::NReplication {

using namespace NKikimrReplication;

enum class EWakeupType : ui64 {
    Describe,
    InitOffset
};

template<typename TDerived>
class TBaseLocalTopicPartitionActor : public TActorBootstrapped<TDerived>
                                    , protected NSchemeCache::TSchemeCacheHelpers {

    using  TThis = TDerived;
    static constexpr size_t MaxAttempts = 5;

public:
    TBaseLocalTopicPartitionActor(const std::string& database, const std::string&& topicName, const ui32 partitionId)
        : Database(database)
        , TopicName(std::move(topicName))
        , PartitionId(partitionId) {
    }

    void Bootstrap() {
        DoDescribe();
    }

protected:
    virtual void OnDescribeFinished() = 0;
    virtual void OnError(const TString& error) = 0;
    virtual void OnFatalError(const TString& error) = 0;
    virtual STATEFN(OnInitEvent) = 0;

private:
    void DoDescribe() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TopicName, TNavigate::OpTopic));
        IActor::Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TBaseLocalTopicPartitionActor::StateDescribe);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        static constexpr auto errorMarket = "LocalYdbProxy";

        auto& result = ev->Get()->Request;

        if (!CheckNotEmpty(errorMarket, result, &TDerived::LogCritAndLeave)) {
            return;
        }

        if (!CheckEntriesCount(errorMarket, result, 1, &TDerived::LogCritAndLeave)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckEntryKind(errorMarket, entry, TNavigate::EKind::KindTopic, &TDerived::LogCritAndLeave)) {
            return;
        }

        if (!CheckEntrySucceeded(errorMarket, entry, &TThis::DoRetryDescribe)) {
            return;
        }

        auto* node = entry.PQGroupInfo->PartitionGraph->GetPartition(PartitionId);
        if (!node) {
            return OnFatalError(TStringBuilder() << "The partition " << PartitionId << " of the topic '" << TopicName << "' not found");
        }
        PartitionTabletId = node->TabletId;
        DoCreatePipe();
    }

    void HandleOnDescribe(TEvents::TEvWakeup::TPtr& ev) {
        if (static_cast<ui64>(EWakeupType::Describe) == ev->Get()->Tag) {
            DoDescribe();
        }
    }

    void DoRetryDescribe(const TString& error) {
        if (Attempt == MaxAttempts) {
            OnError(error);
        } else {
            IActor::Schedule(TDuration::Seconds(1 << Attempt++), new TEvents::TEvWakeup(static_cast<ui64>(EWakeupType::Describe)));
        }
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvents::TEvWakeup, HandleOnDescribe);

            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

protected:
    void DoCreatePipe() {
        Attempt = 0;
        CreatePipe();
        Become(&TBaseLocalTopicPartitionActor::StateCreatePipe);
    }

    void CreatePipe() {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        PartitionPipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(TThis::SelfId(), PartitionTabletId, config));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        if (msg.Status != NKikimrProto::OK) {
            if (Attempt++ == MaxAttempts) {
                return OnError("Pipe creation error");
            }
            return CreatePipe();
        }

        OnDescribeFinished();
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr) {
        OnError("Pipe destroyed");
    }

    STATEFN(StateCreatePipe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

protected:
    void PassAway() {
        IActor::PassAway();
    }

protected:
    const std::string Database;
    const TString TopicName;
    const ui32 PartitionId;

    ui64 PartitionTabletId;
    TActorId PartitionPipeClient;

    size_t Attempt = 0;
};

class TLocalTopicPartitionReaderActor : public TBaseLocalTopicPartitionActor<TLocalTopicPartitionReaderActor> {

    using TBase = TBaseLocalTopicPartitionActor<TLocalTopicPartitionReaderActor>;

    constexpr static TDuration ReadTimeout = TDuration::MilliSeconds(1000);
    constexpr static ui64 ReadLimitBytes = 1_MB;

    struct ReadRequest {
        TActorId Sender;
        ui64 Cookie;
        bool SkipCommit;
    };

public:
    TLocalTopicPartitionReaderActor(const std::string& database, const TEvYdbProxy::TTopicReaderSettings& settings)
        : TBaseLocalTopicPartitionActor(database, std::move(settings.GetBase().Topics_[0].Path_), settings.GetBase().Topics_[0].PartitionIds_[0])
        , Consumer(std::move(settings.GetBase().ConsumerName_))
        , AutoCommit(settings.AutoCommit_) {
        AFL_VERIFY(1 == settings.GetBase().Topics_[0].PartitionIds_.size())("size", settings.GetBase().Topics_[0].PartitionIds_.size()); // TODO Move check
    }

protected:
    void OnDescribeFinished() override {
        DoInitOffset();
    }

    void OnError(const TString& error) override {
        Send(SelfId() /* TODO*/, CreateError(NYdb::EStatus::UNAVAILABLE, error));
    }

    void OnFatalError(const TString& error) override {
        Send(SelfId() /* TODO*/, CreateError(NYdb::EStatus::SCHEME_ERROR, error));
    }

    std::unique_ptr<TEvYdbProxy::TEvTopicReaderGone> CreateError(NYdb::EStatus status, const TString& error) {
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(error);
        return std::make_unique<TEvYdbProxy::TEvTopicReaderGone>(NYdb::TStatus(status, std::move(issues)));
    }

    STATEFN(OnInitEvent) override {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvReadTopicRequest, HandleInit);
        }
    }

private:
    void HandleInit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        RequestsQueue.emplace_back(ev->Sender, ev->Cookie, GetSkipCommit(ev));
    }

    bool GetSkipCommit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        const auto args = std::move(ev->Get()->GetArgs());
        const auto& settings = std::get<TEvYdbProxy::TReadTopicSettings>(args);
        return  settings.SkipCommit_;
    }

private:
    void DoInitOffset() {
        Send(PartitionPipeClient, CreateGetOffsetRequest().release());
        Become(&TLocalTopicPartitionReaderActor::StateInitOffset);
    }

    void HandleOnInitOffset(TEvPersQueue::TEvResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup);
            return;
        }
        if (record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
            return OnError(TStringBuilder() << "Unimplimented response " << record.GetErrorCode() << ": " << record.GetErrorReason());
        }
        if (!record.HasPartitionResponse() || !record.GetPartitionResponse().HasCmdGetClientOffsetResult()) {
            return OnError(TStringBuilder() << "Unimplimented response");
        }

        auto resp = record.GetPartitionResponse().GetCmdGetClientOffsetResult();
        Offset = resp.GetOffset();
        SentOffset = Offset;

        DoWork();
    }

    void HandleOnInitOffset(TEvents::TEvWakeup::TPtr& ev) {
        if (static_cast<ui64>(EWakeupType::InitOffset) == ev->Get()->Tag) {
            DoInitOffset();
        }
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateGetOffsetRequest() const {
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
        HandleInit(ev);
        Handle(RequestsQueue.front());
    }

    void Handle(ReadRequest& request) {
        Offset = SentOffset;

        if (AutoCommit && !request.SkipCommit) {
            request.SkipCommit = true;
            Send(PartitionPipeClient, CreateCommitRequest().release());
        }

        Send(PartitionPipeClient, CreateReadRequest().release());

        DoWaitData();
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateReadRequest() const {
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

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateCommitRequest() const {
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

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoWaitData() {
        Become(&TLocalTopicPartitionReaderActor::StateWaitData);
    }

    void HandleOnWaitData(TEvPersQueue::TEvResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.GetPartitionResponse().HasCmdGetClientOffsetResult()) {
            // Skip set offset result for autocommit
            return;
        }

        if (!record.GetPartitionResponse().HasCmdReadResult()) {
            return OnError("Unsupported response from partition");
        }

        const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();

        if (!readResult.ResultSize()) {
            return Handle(RequestsQueue.front());
        }

        auto request = std::move(RequestsQueue.front());
        RequestsQueue.pop_front();

        auto gotOffset = Offset;
        TVector<NReplication::TTopicMessage> messages(::Reserve(readResult.ResultSize()));

        for (auto& result : readResult.GetResult()) {
            gotOffset = std::max(gotOffset, result.GetOffset());
            messages.emplace_back(result.GetOffset(), GetDeserializedData(result.GetData()).GetData());
        }
        SentOffset = gotOffset + 1;

        Send(request.Sender, new TEvYdbProxy::TEvReadTopicResponse(PartitionId, std::move(messages)), 0, request.Cookie);

        DoWork();
    }

    static NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string) {
        NKikimrPQClient::TDataChunk proto;
        bool res = proto.ParseFromString(string);
        Y_ABORT_UNLESS(res, "Got invalid data from PQTablet");
        return proto;
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
    const TString Consumer;
    const bool AutoCommit;

    std::deque<ReadRequest> RequestsQueue;

    ui64 Offset;
    ui64 SentOffset;
};

class TLocalTopicPartitionCommitActor : public TBaseLocalTopicPartitionActor<TLocalTopicPartitionCommitActor> {
public:
    TLocalTopicPartitionCommitActor(const TActorId& replyTo, const std::string& database, std::string&& topicName, ui64 partitionId, std::string&& consumerName, std::optional<std::string>&& readSessionId, ui64 offset)
        : TBaseLocalTopicPartitionActor(database, std::move(topicName), partitionId)
        , ReplyTo(replyTo)
        , ConsumerName(std::move(consumerName))
        , ReadSessionId(std::move(readSessionId))
        , Offset(offset) {
        }

private:
    const TActorId ReplyTo;
    const std::string ConsumerName;
    const std::optional<std::string> ReadSessionId;
    const ui64 Offset;
};

class TLocalProxyActor: public TActorBootstrapped<TLocalProxyActor> {
private:
    void Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        const auto& settings = std::get<TEvYdbProxy::TTopicReaderSettings>(args);

        auto actor = new TLocalTopicPartitionReaderActor(Database, settings);
        auto reader = TlsActivationContext->RegisterWithSameMailbox(actor, SelfId());
        Send(ev->Sender, new TEvYdbProxy::TEvCreateTopicReaderResponse(reader));
    }

    void Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev) {
        Y_UNUSED(ev);
        // TODO
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        auto& [topicName, partitionId, consumerName, offset, settings] = args;

        auto actor = new TLocalTopicPartitionCommitActor(ev->Sender, Database, std::move(topicName), partitionId, std::move(consumerName), std::move(settings.ReadSessionId_), offset);
        TlsActivationContext->RegisterWithSameMailbox(actor, SelfId());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvCreateTopicReaderRequest, Handle);
            hFunc(TEvYdbProxy::TEvAlterTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            Y_UNREACHABLE();
        }
    }

public:
    TLocalProxyActor(const TString& database)
        : Database(database) {
    }

    void Bootstrap() {
        Become(&TLocalProxyActor::StateWork);
    }

private:
    const TString Database;
};

IActor* CreateLocalYdbProxy(const TString& database) {
    return new TLocalProxyActor(database);
}

}