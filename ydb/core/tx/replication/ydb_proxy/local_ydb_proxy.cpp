#include "ydb_proxy.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/common.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/scheme_cache/helpers.h>

namespace NKikimr::NReplication {

using namespace NKikimrReplication;
using namespace NSchemeCache;

enum class EWakeupType : ui64 {
    Describe,
    InitOffset
};

class TBaseLocalTopicPartitionActor : public TActorBootstrapped<TBaseLocalTopicPartitionActor>
                                    , private TSchemeCacheHelpers {

    using  TThis = TBaseLocalTopicPartitionActor;
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
        Cerr << ">>>>> TopicName " << "/" << Database << TopicName << Endl << Flush;
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TStringBuilder() << "/" << Database << TopicName, TNavigate::OpTopic));
        IActor::Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateDescribe);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        static const TString errorMarket = "LocalYdbProxy";

        auto& result = ev->Get()->Request;

        Cerr << ">>>>> result->DatabaseName " << result->DatabaseName << Endl << Flush;

        if (!CheckNotEmpty(errorMarket, result, LeaveOnError())) {
            return;
        }

        if (!CheckEntriesCount(errorMarket, result, 1, LeaveOnError())) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckEntryKind(errorMarket, entry, TNavigate::EKind::KindTopic, LeaveOnError())) {
            return;
        }

        if (!CheckEntrySucceeded(errorMarket, entry, DoRetryDescribe())) {
            return;
        }

        auto* node = entry.PQGroupInfo->PartitionGraph->GetPartition(PartitionId);
        if (!node) {
            return OnFatalError(TStringBuilder() << "The partition " << PartitionId << " of the topic '" << TopicName << "' not found");
        }
        PartitionTabletId = node->TabletId;
        Cerr << ">>>>> PartitionTabletId " << PartitionTabletId << Endl << Flush;
        DoCreatePipe();
    }

    void HandleOnDescribe(TEvents::TEvWakeup::TPtr& ev) {
        if (static_cast<ui64>(EWakeupType::Describe) == ev->Get()->Tag) {
            DoDescribe();
        }
    }

    TCheckFailFunc DoRetryDescribe() {
        return [this](const TString& error) {
            if (Attempt == MaxAttempts) {
                OnError(error);
            } else {
                IActor::Schedule(TDuration::Seconds(1 << Attempt++), new TEvents::TEvWakeup(static_cast<ui64>(EWakeupType::Describe)));
            }
        };
    }

    TCheckFailFunc LeaveOnError() {
        return [this](const TString& error) {
            OnFatalError(error);
        };
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
        config.RetryPolicy = {
            .RetryLimitCount = 3
        };
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
        Cerr << ">>>>> TEvTabletPipe::TEvClientDestroyed" << Endl << Flush;
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
        if (PartitionPipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PartitionPipeClient);
        }
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

class TLocalTopicPartitionReaderActor : public TBaseLocalTopicPartitionActor {

    using TBase = TBaseLocalTopicPartitionActor;

    constexpr static TDuration ReadTimeout = TDuration::MilliSeconds(1000);
    constexpr static ui64 ReadLimitBytes = 1_MB;

    struct ReadRequest {
        TActorId Sender;
        ui64 Cookie;
        bool SkipCommit;
    };

public:
    TLocalTopicPartitionReaderActor(const TActorId& parent, const std::string& database, TEvYdbProxy::TTopicReaderSettings& settings)
        : TBaseLocalTopicPartitionActor(database, std::move(settings.GetBase().Topics_[0].Path_), settings.GetBase().Topics_[0].PartitionIds_[0])
        , Parent(parent)
        , Consumer(std::move(settings.GetBase().ConsumerName_))
        , AutoCommit(settings.AutoCommit_) {
    }

protected:
    void OnDescribeFinished() override {
        DoInitOffset();
    }

    void OnError(const TString& error) override {
        Send(Parent, CreateError(NYdb::EStatus::UNAVAILABLE, error));
        PassAway();
    }

    void OnFatalError(const TString& error) override {
        Send(Parent, CreateError(NYdb::EStatus::SCHEME_ERROR, error));
        PassAway();
    }

    std::unique_ptr<TEvYdbProxy::TEvTopicReaderGone> CreateError(NYdb::EStatus status, const TString& error) {
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(error);
        return std::make_unique<TEvYdbProxy::TEvTopicReaderGone>(NYdb::TStatus(status, std::move(issues)));
    }

    STATEFN(OnInitEvent) override {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvReadTopicRequest, HandleInit);
        default:
            Y_VERIFY_DEBUG(TStringBuilder() << "Unhandled message " << ev->GetTypeName());
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
        Cerr << ">>>>> DoInitOffset " << Endl << Flush;
        NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateGetOffsetRequest().release());
        Become(&TLocalTopicPartitionReaderActor::StateInitOffset);
    }

    void HandleOnInitOffset(TEvPersQueue::TEvResponse::TPtr& ev) {
        Cerr << ">>>>> DoInitOffset TEvResponse " << Endl << Flush;

        auto& record = ev->Get()->Record;
        if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup);
            return;
        }
        if (record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
            return OnError(TStringBuilder() << "Unimplimented response: " << record.GetErrorReason());
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

        Cerr << ">>>>> " << request->Record.DebugString() << Endl << Flush;

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
        Cerr << ">>>>> DoWork " << Endl << Flush;

        Become(&TLocalTopicPartitionReaderActor::StateWork);

        if (!RequestsQueue.empty()) {
            Handle(RequestsQueue.front());
        }
    }

    void Handle(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        Cerr << ">>>>>> TEvYdbProxy::TEvReadTopicReques" << Endl << Flush;

        HandleInit(ev);
        Handle(RequestsQueue.front());
    }

    void Handle(ReadRequest& request) {
        Offset = SentOffset;

        if (AutoCommit && !request.SkipCommit) {
            request.SkipCommit = true;
            NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateCommitRequest().release());
        }

        NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateReadRequest().release());

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

        TString error;
        if (!NPQ::BasicCheck(record, error)) {
            return OnError(TStringBuilder() << "Wrong response: " << error);
        }

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
    const TActorId Parent;
    const TString Consumer;
    const bool AutoCommit;

    std::deque<ReadRequest> RequestsQueue;

    ui64 Offset;
    ui64 SentOffset;
};

class TLocalTopicPartitionCommitActor : public TBaseLocalTopicPartitionActor {

    using TBase = TBaseLocalTopicPartitionActor;

public:
    TLocalTopicPartitionCommitActor(const TActorId& parent, const std::string& database, std::string&& topicName, ui64 partitionId, std::string&& consumerName, std::optional<std::string>&& readSessionId, ui64 offset)
        : TBaseLocalTopicPartitionActor(database, std::move(topicName), partitionId)
        , Parent(parent)
        , ConsumerName(std::move(consumerName))
        , ReadSessionId(std::move(readSessionId))
        , Offset(offset) {
    }

protected:
    void OnDescribeFinished() override {
        DoCommitOffset();
    }

    void OnError(const TString& error) override {
        Send(Parent, CreateResponse(NYdb::EStatus::UNAVAILABLE, error));
        PassAway();
    }

    void OnFatalError(const TString& error) override {
        Send(Parent, CreateResponse(NYdb::EStatus::SCHEME_ERROR, error));
        PassAway();
    }

    std::unique_ptr<TEvYdbProxy::TEvCommitOffsetResponse> CreateResponse(NYdb::EStatus status, const TString& error) {
        NYdb::NIssue::TIssues issues;
        if (error) {
            issues.AddIssue(error);
        }
        return std::make_unique<TEvYdbProxy::TEvCommitOffsetResponse>(NYdb::TStatus(status, std::move(issues)));
    }

    STATEFN(OnInitEvent) override {
        Y_UNUSED(ev);
    }

private:
    void DoCommitOffset() {
        NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateCommitRequest().release());
        Become(&TLocalTopicPartitionCommitActor::StateCommitOffset);
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TString error;
        if (!NPQ::BasicCheck(record, error)) {
            return OnError(TStringBuilder() << "Wrong response: " << error);
        }

        if (!record.GetPartitionResponse().HasCmdGetClientOffsetResult()) {
            return OnError("Unsupported response from partition");
        }

        Send(Parent, CreateResponse(NYdb::EStatus::SUCCESS, ""));
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateCommitRequest() const {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(PartitionId);
        auto& commit = *req.MutableCmdSetClientOffset();
        commit.SetOffset(Offset);
        commit.SetClientId(ConsumerName);
        if (ReadSessionId) {
            commit.SetSessionId(ReadSessionId.value());
        }

        return request;
    }

    STATEFN(StateCommitOffset) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, Handle);

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

private:
    const TActorId Parent;
    const TString ConsumerName;
    const std::optional<TString> ReadSessionId;
    const ui64 Offset;
};

class TLocalProxyRequest : public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TLocalProxyRequest;

    TLocalProxyRequest(
           // TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString path,
            TString databaseName,
            std::unique_ptr<google::protobuf::Message>&& request,
            const std::function<void(Ydb::StatusIds::StatusCode, const google::protobuf::Message*)> sendResultCallback)
        : //UserToken(userToken)
         Path(path)
        , DatabaseName(databaseName)
        , Request(std::move(request))
        , SendResultCallback(sendResultCallback)
    {
    };

    const TString path() const {
        return Path;
    }

    TMaybe<TString> GetTraceId() const override {
        return Nothing();
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return DatabaseName;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return UserToken;
    }

    const TString& GetSerializedToken() const override {
        return DummyString;
    }

    bool IsClientLost() const override {
        return false;
    };

    const google::protobuf::Message* GetRequest() const override {
        return Request.get();
    };

    const TMaybe<TString> GetRequestType() const override {
        return Nothing();
    };

    void SetFinishAction(std::function<void()>&& cb) override {
        Y_UNUSED(cb);
    };

    google::protobuf::Arena* GetArena() override {
        return nullptr;
    };

    bool HasClientCapability(const TString& capability) const override {
        Y_UNUSED(capability);
        return false;
    };

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        ProcessYdbStatusCode(status, NULL);
    };

    void ReplyWithRpcStatus(grpc::StatusCode code, const TString& msg = "", const TString& details = "") override {
        Y_UNUSED(code);
        Y_UNUSED(msg);
        Y_UNUSED(details);
    }

    TString GetPeerName() const override {
        return "";
    }

    TInstant GetDeadline() const override {
        return TInstant();
    }

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        return Nothing();
    }

    TVector<TStringBuf> FindClientCert() const override {
        return TVector<TStringBuf>();
    }

    TMaybe<NKikimr::NRpcService::TRlPath> GetRlPath() const override {
        return Nothing();
    }

    void RaiseIssue(const NYql::TIssue& issue) override{
        Issue = issue;
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        Y_UNUSED(issues);
    };

    const TString& GetRequestName() const override {
        return DummyString;
    };

    bool GetDiskQuotaExceeded() const override {
        return false;
    };

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        Y_UNUSED(name);
        Y_UNUSED(value);
    };

    const NKikimr::NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        return DummyAuditLogParts;
    };

    void SetRuHeader(ui64 ru) override {
        Y_UNUSED(ru);
    };

    void AddServerHint(const TString& hint) override {
        Y_UNUSED(hint);
    };

    void SetCostInfo(float consumed_units) override {
        Y_UNUSED(consumed_units);
    };

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        Y_UNUSED(cb);
    };

    void FinishStream(ui32 status) override {
        Y_UNUSED(status);
    };

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl) override {
        Y_UNUSED(in);
        Y_UNUSED(status);
    };

    void Reply(NProtoBuf::Message* resp, ui32 status = 0) override {
        Y_UNUSED(resp);
        Y_UNUSED(status);
    };

    void SendOperation(const Ydb::Operations::Operation& operation) override {
        Y_UNUSED(operation);
    };

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        Y_UNUSED(result);
        ProcessYdbStatusCode(status, &result);
    };

    void SendResult(
            const google::protobuf::Message& result,
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(result);
        Y_UNUSED(message);
        ProcessYdbStatusCode(status, NULL);
    };

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TLocalProxyRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TLocalProxyRequest*>(&(*request));
    }

protected:
    void FinishRequest() override {
    };

private:
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString Path;
    const TString DatabaseName;
    std::unique_ptr<google::protobuf::Message> Request;
    const std::function<void(Ydb::StatusIds::StatusCode, const google::protobuf::Message*)> SendResultCallback;
    NYql::TIssue Issue;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status, const google::protobuf::Message* result) {
        SendResultCallback(status, result);
    }
};

class TLocalProxyActor: public TActorBootstrapped<TLocalProxyActor>
                      , public NGRpcService::IFacilityProvider {
public:
    ui64 GetChannelBufferSize() const override {
        return Max<ui32>();
    }

    // Registers new actor using method chosen by grpc proxy
    TActorId RegisterActor(IActor* actor) const override {
        return TlsActivationContext->RegisterWithSameMailbox(actor, SelfId());
    }

private:
    void Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        auto& settings = std::get<TEvYdbProxy::TTopicReaderSettings>(args);

        Cerr << ">>>>> TEvYdbProxy::TEvCreateTopicReaderRequest" << Endl << Flush;

        AFL_VERIFY(1 == settings.GetBase().Topics_.size())("topic count", settings.GetBase().Topics_.size());
        AFL_VERIFY(1 == settings.GetBase().Topics_[0].PartitionIds_.size())("partition count", settings.GetBase().Topics_[0].PartitionIds_.size());

        auto actor = new TLocalTopicPartitionReaderActor(ev->Sender, Database, settings);
        auto reader = TlsActivationContext->RegisterWithSameMailbox(actor, SelfId());
        Send(ev->Sender, new TEvYdbProxy::TEvCreateTopicReaderResponse(reader), 0, ev->Cookie);
    }

    void Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        auto& path = std::get<TString>(args);
        auto& settings = std::get<NYdb::NTopic::TAlterTopicSettings>(args);

        Cerr << ">>>>> TEvYdbProxy::TEvAlterTopicRequest" << Endl << Flush;

        auto request = std::make_unique<Ydb::Topic::AlterTopicRequest>();
        request.get()->set_path(TStringBuilder() << "/" << Database << path);
        for(auto& c : settings.AddConsumers_) {
            auto* consumer = request.get()->add_add_consumers();
            consumer->set_name(c.ConsumerName_);
        }
        for(auto& c : settings.DropConsumers_) {
            auto* consumer = request.get()->add_drop_consumers();
            *consumer = c;
        }

        auto callback = [replyTo= ev->Sender, cookie = ev->Cookie, path=path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message*) {
            Cerr << ">>>>> TEvAlterTopicRequest RESULT" << path << Endl << Flush;

            NYdb::NIssue::TIssues issues;
            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            Send(replyTo, new TEvYdbProxy::TEvAlterTopicResponse(std::move(status)), 0, cookie);
            Cerr << ">>>>> TEvAlterTopicRequest RESULT SENT" << path << Endl << Flush;
        };

        NGRpcService::DoAlterTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
        Y_UNUSED(ev);

        Cerr << ">>>>> TEvYdbProxy::TEvCommitOffsetRequest" << Endl << Flush;

        auto args = std::move(ev->Get()->GetArgs());
        auto& [topicName, partitionId, consumerName, offset, settings] = args;

        auto* actor = new TLocalTopicPartitionCommitActor(ev->Sender, Database, std::move(topicName), partitionId, std::move(consumerName), std::move(settings.ReadSessionId_), offset);
        TlsActivationContext->RegisterWithSameMailbox(actor, SelfId());
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        auto& path = std::get<TString>(args);
        auto& settings = std::get<NYdb::NTopic::TDescribeTopicSettings>(args);
        Y_UNUSED(settings);

        auto request = std::make_unique<Ydb::Topic::DescribeTopicRequest>();
        request.get()->set_path(TStringBuilder() << "/" << Database << path);

        Cerr << ">>>>> TEvDescribeTopicRequest" << path << Endl << Flush;
        auto callback = [replyTo= ev->Sender, cookie = ev->Cookie, path=path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* result) {
            Cerr << ">>>>> TEvDescribeTopicRequest RESULT" << path << Endl << Flush;

            NYdb::NIssue::TIssues issues;
            Ydb::Topic::DescribeTopicResult describe;
            if (statusCode == Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS) {
                const auto* v = dynamic_cast<const Ydb::Topic::DescribeTopicResult*>(result);
                if (v) {
                    describe = *v;
                } else {
                    statusCode = Ydb::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
                    issues.AddIssue(TStringBuilder() << "Unexpected result type " << result->GetTypeName());
                }
            }

            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            NYdb::NTopic::TDescribeTopicResult r(std::move(status), std::move(describe));
            Send(replyTo, new TEvYdbProxy::TEvDescribeTopicResponse(r), 0, cookie);
            Cerr << ">>>>> TEvDescribeTopicRequest RESULT SENT" << path << Endl << Flush;
        };

        NGRpcService::DoDescribeTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
    }

    void Handle(TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
        auto args = std::move(ev->Get()->GetArgs());
        auto& path = std::get<TString>(args);
        auto& settings = std::get<NYdb::NScheme::TDescribePathSettings>(args);
        Y_UNUSED(settings);

        auto request = std::make_unique<Ydb::Scheme::DescribePathRequest>();
        request.get()->set_path(TStringBuilder() << "/" << Database << path);

        auto callback = [replyTo= ev->Sender, cookie = ev->Cookie, path=path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* result) {
            NYdb::NIssue::TIssues issues;
            NYdb::NScheme::TSchemeEntry entry;
            if (statusCode == Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS) {
                const auto* v = dynamic_cast<const Ydb::Scheme::DescribePathResult*>(result);
                if (v) {
                    entry = v->self();
                } else {
                    statusCode = Ydb::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
                    issues.AddIssue(TStringBuilder() << "Unexpected result type " << result->GetTypeName());
                }
            }

            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            NYdb::NScheme::TDescribePathResult r(std::move(status), std::move(entry));
            Send(replyTo, new TEvYdbProxy::TEvDescribePathResponse(r), 0, cookie);
        };

        NGRpcService::DoDescribePathRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvCreateTopicReaderRequest, Handle);
            hFunc(TEvYdbProxy::TEvAlterTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribePathRequest, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            Y_VERIFY_DEBUG(TStringBuilder() << "Unhandled message " << ev->GetTypeName());
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