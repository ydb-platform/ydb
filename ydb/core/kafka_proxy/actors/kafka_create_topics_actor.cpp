#include "kafka_create_topics_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>


namespace NKafka {

class TKafkaCreateTopicRequest : public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TKafkaCreateTopicRequest;

    TKafkaCreateTopicRequest(
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            const std::function<void(EKafkaErrors, TString&)> sendResultCallback)
        : UserToken(userToken)
        , TopicPath(topicPath)
        , DatabaseName(databaseName)
        , SendResultCallback(sendResultCallback)
    {
    };

    const TString path() const {
        return TopicPath;
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
        return UserToken->GetSerializedToken();
    }

    bool IsClientLost() const override {
        return false;
    };

    virtual const google::protobuf::Message* GetRequest() const override {
        return nullptr;
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
        ProcessYdbStatusCode(status);
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
        ReplyMessage = issue.GetMessage();
        Y_UNUSED(issue);
    }
    void RaiseIssues(const NYql::TIssues& issues) override {
        Y_UNUSED(issues);
    };
    const TString& GetRequestName() const override {
        return DummyString;
    };
    void SetDiskQuotaExceeded(bool disk) override {
        Y_UNUSED(disk);
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

    google::protobuf::Message* GetRequestMut() override {
        return nullptr;
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

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl flag) override {
        Y_UNUSED(in);
        Y_UNUSED(status);
        Y_UNUSED(flag);
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
        ProcessYdbStatusCode(status);
    };

    void SendResult(
            const google::protobuf::Message& result,
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(result);
        Y_UNUSED(message);
        ProcessYdbStatusCode(status);
    };

    void SendResult(
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(message);
        ProcessYdbStatusCode(status);
    };

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TKafkaCreateTopicRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TKafkaCreateTopicRequest*>(&(*request));
    }

protected:
    void FinishRequest() override {
    };

private:
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString TopicPath;
    const TString DatabaseName;
    const std::function<void(EKafkaErrors status, TString& message)> SendResultCallback;
    TString ReplyMessage;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status) {
        SendResultCallback(ConvertErrorCode(status), ReplyMessage);
    }
};

class TCreateTopicActor : public NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TCreateTopicActor, TKafkaCreateTopicRequest> {
    using TBase = NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TCreateTopicActor, TKafkaCreateTopicRequest>;
public:

    TCreateTopicActor(
            TActorId requester, 
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            ui32 partitionsNumber,
            std::optional<ui64> retentionMs,
            std::optional<ui64> retentionBytes)
        : TBase(new TKafkaCreateTopicRequest(
            userToken,
            topicPath,
            databaseName,
            [this](EKafkaErrors status, TString& message) {
                this->SendResult(status, message);
            })
        )
        , Requester(requester)
        , TopicPath(topicPath)
        , PartionsNumber(partitionsNumber)
        , RetentionMs(retentionMs)
        , RetentionBytes(retentionBytes)
    {
        KAFKA_LOG_D(LogMessage(databaseName));
    };

    ~TCreateTopicActor() = default;

    void SendResult(EKafkaErrors status, TString& message) {
        THolder<TEvKafka::TEvTopicModificationResponse> response(new TEvKafka::TEvTopicModificationResponse());
        response->Status = status;
        response->TopicPath = TopicPath;
        response->Message = message;
        Send(Requester, response.Release());
        Send(SelfId(), new TEvents::TEvPoison());
    }

    void FillProposeRequest(
            NKikimr::TEvTxUserProxy::TEvProposeTransaction &proposal,
            const TActorContext &ctx,
            const TString &workingDir,
            const TString &name
    ) {
        NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
        modifyScheme.SetWorkingDir(workingDir);

        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();
        pqDescr->SetPartitionPerTablet(1);

        Ydb::Topic::CreateTopicRequest topicRequest;
        topicRequest.mutable_partitioning_settings()->set_min_active_partitions(PartionsNumber);
        if (RetentionMs.has_value()) {
            topicRequest.mutable_retention_period()->set_seconds(RetentionMs.value() / 1000);
        }
        if (RetentionBytes.has_value()) {
            topicRequest.set_retention_storage_mb(RetentionBytes.value() / 1000'000);
        }
        topicRequest.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);

        TString error;
        TYdbPqCodes codes = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(
                name,
                topicRequest,
                modifyScheme,
                ctx,
                error,
                workingDir,
                proposal.Record.GetDatabaseName()
        );
        if (codes.YdbCode != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(codes.YdbCode, codes.PQCode, error, ctx);
        }
    };

    void Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendProposeRequest(ctx);
        Become(&TCreateTopicActor::StateWork);
    };

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult, TActorBase::Handle);
        default: TBase::StateWork(ev);
        }
    }

    void HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }

private:
    const TActorId Requester;
    const TString TopicPath;
    const std::shared_ptr<TString> SerializedToken;
    const ui32 PartionsNumber;
    std::optional<ui64> RetentionMs;
    std::optional<ui64> RetentionBytes;

    TStringBuilder LogMessage(TString& databaseName) {
        TStringBuilder stringBuilder = TStringBuilder()
            << "Create Topic actor. DatabaseName: " << databaseName
            << ". TopicPath: " << TopicPath
            << ". PartitionsNumber: " << PartionsNumber;
        if (RetentionMs.has_value()) {
            stringBuilder << ". RetentionMs: " << RetentionMs.value();
        }
        if (RetentionBytes.has_value()) {
            stringBuilder << ". RetentionBytes: " << RetentionBytes.value();
        }
        return stringBuilder;
    }
};

NActors::IActor* CreateKafkaCreateTopicsActor(
        const TContext::TPtr context,
        const ui64 correlationId,
        const TMessagePtr<TCreateTopicsRequestData>& message
) {
    return new TKafkaCreateTopicsActor(context, correlationId, message);
}

void TKafkaCreateTopicsActor::Bootstrap(const NActors::TActorContext& ctx) {
    KAFKA_LOG_D(InputLogMessage());

    if (Message->ValidateOnly) {
        ProcessValidateOnly(ctx);
        return;
    }

    std::unordered_set<TString> topicNames;
    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();
        if (topicNames.contains(topicName)) {
            DuplicateTopicNames.insert(topicName);
        } else {
            topicNames.insert(topicName);
        }
    }

    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();

        if (DuplicateTopicNames.contains(topicName)) {
            continue;
        }

        if (topicName == "") {
            auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
            result->Status = EKafkaErrors::INVALID_REQUEST;
            result->Message = "Empty topic name";
            this->TopicNamesToResponses[topicName] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(result.Release());
            continue;
        }

        std::optional<ui64> retentionMs;
        std::optional<ui64> retentionBytes;

        auto parseRetention = [this, topic](
                TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig& config,
                std::optional<ui64>& retention) -> bool {
            try {
                retention = std::stoul(config.Value.value());
                return true;
            } catch(std::invalid_argument) {
                auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
                result->Status = EKafkaErrors::INVALID_CONFIG;
                result->Message = "Provided retention value is not a number";
                this->TopicNamesToResponses[topic.Name.value()] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(result.Release());
                return false;
            }
        };

        auto processConfig = [&topic, &retentionMs, &retentionBytes, &parseRetention]() -> bool { 
            for (auto& config : topic.Configs) {
                bool result = true;
                if (config.Name.value() == RETENTION_BYTES_CONFIG_NAME) {
                    result = parseRetention(config, retentionBytes);
                } else if (config.Name.value() == RETENTION_MS_CONFIG_NAME) {
                    result = parseRetention(config, retentionMs);
                }
                if (!result) {
                    return false;
                }
            }
            return true;
        };

        if (!processConfig()) {
            continue;
        }

        TopicNamesToRetentions[topicName] = std::pair<std::optional<ui64>, std::optional<ui64>>(
            retentionMs,
            retentionBytes
        );

        ctx.Register(new TCreateTopicActor(
            SelfId(),
            Context->UserToken,
            topic.Name.value(),
            Context->DatabasePath,
            topic.NumPartitions,
            retentionMs,
            retentionBytes
        ));

        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaCreateTopicsActor::StateWork);
    } else {
        Reply(ctx);
    }
};

void TKafkaCreateTopicsActor::Handle(const TEvKafka::TEvTopicModificationResponse::TPtr& ev, const TActorContext& ctx) {
    auto eventPtr = ev->Release();
    KAFKA_LOG_D(TStringBuilder() << "Create topics actor. Topic's " << eventPtr->TopicPath << " response received." << std::to_string(eventPtr->Status));
    TopicNamesToResponses[eventPtr->TopicPath] = eventPtr;
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply(ctx);
    }
};

void TKafkaCreateTopicsActor::Reply(const TActorContext& ctx) {
    TCreateTopicsResponseData::TPtr response = std::make_shared<TCreateTopicsResponseData>();
    EKafkaErrors responseStatus = NONE_ERROR;

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        TCreateTopicsResponseData::TCreatableTopicResult responseTopic;
        responseTopic.Name = topicName;

        if (TopicNamesToResponses.contains(topicName)) {
            responseTopic.ErrorMessage = TopicNamesToResponses[topicName]->Message;
        }

        auto addConfigIfRequired = [this, &topicName, &responseTopic](std::optional<ui64> configValue, TString configName) { 
            if (configValue.has_value()) {
                TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs config;
                config.Name = configName;
                config.Value = std::to_string(TopicNamesToRetentions[topicName].first.value());
                config.IsSensitive = false;
                config.ReadOnly = false;
                responseTopic.Configs.push_back(config);
            }
        };

        auto setError= [&responseTopic, &responseStatus](EKafkaErrors status) { 
            responseTopic.ErrorCode = status;
            responseStatus = status;
        };

        if (DuplicateTopicNames.contains(topicName)) {
            setError(DUPLICATE_RESOURCE);
        } else {
            auto status = TopicNamesToResponses[topicName]->Status;
            if (status == EKafkaErrors::NONE_ERROR) {
                addConfigIfRequired(TopicNamesToRetentions[topicName].first, RETENTION_MS_CONFIG_NAME);
                addConfigIfRequired(TopicNamesToRetentions[topicName].second, RETENTION_BYTES_CONFIG_NAME);
            }
            setError(TopicNamesToResponses[topicName]->Status);
        } 
        response->Topics.push_back(responseTopic);
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));

    Die(ctx);
};

TStringBuilder TKafkaCreateTopicsActor::InputLogMessage() {
    TStringBuilder stringBuilder;
    stringBuilder << "Create topics actor: New request. ValidateOnly:" << (Message->ValidateOnly != 0) << " Topics: [";

    bool isFirst = true;
    for (auto& requestTopic : Message->Topics) {
        if (isFirst) {
            isFirst = false;
        } else {
            stringBuilder << ",";
        }
        stringBuilder << " " << requestTopic.Name.value();
    }
    stringBuilder << " ]";
    return stringBuilder;
};


void TKafkaCreateTopicsActor::ProcessValidateOnly(const NActors::TActorContext& ctx) {
    TCreateTopicsResponseData::TPtr response = std::make_shared<TCreateTopicsResponseData>();

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        TCreateTopicsResponseData::TCreatableTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.NumPartitions = requestTopic.NumPartitions;
        responseTopic.ErrorCode = NONE_ERROR;
        response->Topics.push_back(responseTopic);
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, response, NONE_ERROR));
    Die(ctx);
};
}
