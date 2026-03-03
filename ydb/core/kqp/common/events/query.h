#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/common/result_set_format/kqp_result_set_format_settings.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/grpc_services/base/iface.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/grpc_services/cancelation/cancelation.h>

#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvQueryRequestRemote: public TEventPB<TEvQueryRequestRemote, NKikimrKqp::TEvQueryRequest,
    TKqpEvents::EvQueryRequest> {
};

struct TQueryRequestSettings {
    TQueryRequestSettings& SetKeepSession(bool flag) {
        KeepSession = flag;
        return *this;
    }

    TQueryRequestSettings& SetUseCancelAfter(bool flag) {
        UseCancelAfter = flag;
        return *this;
    }

    TQueryRequestSettings& SetSyntax(const ::Ydb::Query::Syntax& syntax) {
        Syntax = syntax;
        return *this;
    }

    TQueryRequestSettings& SetSupportStreamTrailingResult(bool flag) {
        SupportsStreamTrailingResult = flag;
        return *this;
    }

    TQueryRequestSettings& SetOutputChunkMaxSize(ui64 size) {
        OutputChunkMaxSize = size;
        return *this;
    }

    TQueryRequestSettings& SetSchemaInclusionMode(const ::Ydb::Query::SchemaInclusionMode& mode) {
        SchemaInclusionMode = mode;
        return *this;
    }

    TQueryRequestSettings& SetResultSetFormat(const ::Ydb::ResultSet::Format& format) {
        ResultSetFormat = format;
        return *this;
    }

    ui64 OutputChunkMaxSize = 0;
    bool KeepSession = false;
    bool UseCancelAfter = true;
    ::Ydb::Query::Syntax Syntax = Ydb::Query::Syntax::SYNTAX_UNSPECIFIED;
    ::Ydb::Query::SchemaInclusionMode SchemaInclusionMode = Ydb::Query::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_UNSPECIFIED;
    ::Ydb::ResultSet::Format ResultSetFormat = Ydb::ResultSet::FORMAT_UNSPECIFIED;
    bool SupportsStreamTrailingResult = false;
};

struct TEvQueryRequest: public NActors::TEventLocal<TEvQueryRequest, TKqpEvents::EvQueryRequest> {
public:
    TEvQueryRequest(
        NKikimrKqp::EQueryAction queryAction,
        NKikimrKqp::EQueryType queryType,
        TActorId requestActorId,
        const std::shared_ptr<NGRpcService::IRequestCtxMtSafe>& ctx,
        const TString& sessionId,
        TString&& yqlText,
        TString&& queryId,
        const ::Ydb::Table::TransactionControl* txControl,
        const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* ydbParameters,
        const ::Ydb::Table::QueryStatsCollection::Mode collectStats,
        const ::Ydb::Table::QueryCachePolicy* queryCachePolicy,
        const ::Ydb::Operations::OperationParams* operationParams,
        const TQueryRequestSettings& querySettings = TQueryRequestSettings(),
        const TString& poolId = "",
        std::optional<NFormats::TArrowFormatSettings> arrowFormatSettings = std::nullopt);

    TEvQueryRequest() {
        Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
    }

    TEvQueryRequest(const TString& userSID);

    bool IsSerializable() const override {
        return true;
    }

    TEventSerializationInfo CreateSerializationInfo(bool /*allowExternalDataChannel*/) const override { return {}; }

    const TString& GetDatabase() const {
        return RequestCtx ? Database : Record.GetRequest().GetDatabase();
    }

    TString GetUserSID() const {
        auto token = GetUserToken();
        return token ? token->GetUserSID() : "";
    }

    const std::shared_ptr<NGRpcService::IRequestCtxMtSafe>& GetRequestCtx() const {
        return RequestCtx;
    }

    bool HasYdbStatus() const {
        return RequestCtx ? false : Record.HasYdbStatus();
    }

    const ::NKikimrKqp::TTopicOperationsRequest& GetTopicOperations() const {
        return Record.GetRequest().GetTopicOperations();
    }

    const ::NKikimrKqp::TKafkaApiOperationsRequest& GetKafkaApiOperations() const {
        return Record.GetRequest().GetKafkaApiOperations();
    }

    bool HasTopicOperations() const {
        return Record.GetRequest().HasTopicOperations();
    }

    bool HasKafkaApiOperations() const {
        return Record.GetRequest().HasKafkaApiOperations();
    }

    bool GetKeepSession() const {
        return RequestCtx ? QuerySettings.KeepSession : Record.GetRequest().GetKeepSession();
    }

    TDuration GetCancelAfter() const {
        return RequestCtx ? CancelAfter : TDuration::MilliSeconds(Record.GetRequest().GetCancelAfterMs());
    }

    TDuration GetOperationTimeout() const {
        return RequestCtx ? OperationTimeout : TDuration::MilliSeconds(Record.GetRequest().GetTimeoutMs());
    }

    bool HasAction() const {
        return RequestCtx ? true : Record.GetRequest().HasAction();
    }

    void SetSessionId(const TString& sessionId) {
        if (RequestCtx) {
            SessionId = sessionId;
        } else {
            Record.MutableRequest()->SetSessionId(sessionId);
        }
    }

    const TString& GetSessionId() const {
        return RequestCtx ? SessionId : Record.GetRequest().GetSessionId();
    }

    NKikimrKqp::EQueryAction GetAction() const {
        return RequestCtx ? QueryAction : Record.GetRequest().GetAction();
    }

    NKikimrKqp::EQueryType GetType() const {
        return RequestCtx ? QueryType : Record.GetRequest().GetType();
    }

    Ydb::Query::Syntax GetSyntax() const {
        return RequestCtx ? QuerySettings.Syntax : Record.GetRequest().GetSyntax();
    }

    bool HasPreparedQuery() const {
        return RequestCtx ? QueryId.size() > 0 : Record.GetRequest().HasPreparedQuery();
    }

    const TString& GetPreparedQuery() const {
        return RequestCtx ? QueryId : Record.GetRequest().GetPreparedQuery();
    }

    const TString& GetQuery() const {
        return RequestCtx ? YqlText : Record.GetRequest().GetQuery();
    }

    const ::Ydb::Table::TransactionControl& GetTxControl() const {
        return RequestCtx ? *TxControl : Record.GetRequest().GetTxControl();
    }

    bool GetUsePublicResponseDataFormat() const {
        return RequestCtx ? true : Record.GetRequest().GetUsePublicResponseDataFormat();
    }

    bool GetQueryKeepInCache() const {
        if (RequestCtx) {
            if (QueryCachePolicy != nullptr) {
                return QueryCachePolicy->keep_in_cache();
            }
            return false;
        }
        return Record.GetRequest().GetQueryCachePolicy().keep_in_cache();
    }

    bool HasTxControl() const {
        return RequestCtx ? TxControl != nullptr : Record.GetRequest().HasTxControl();
    }

    bool HasCollectStats() const {
        return RequestCtx ? true : Record.GetRequest().HasCollectStats();
    }

    TActorId GetRequestActorId() const {
        return RequestCtx ? RequestActorId : ActorIdFromProto(Record.GetRequestActorId());
    }

    const TString& GetTraceId() const {
        if (RequestCtx) {
            if (!TraceId) {
                TraceId = RequestCtx->GetTraceId().GetOrElse("");
            }
            return TraceId;
        }

        return Record.GetTraceId();
    }

    NWilson::TTraceId GetWilsonTraceId() const {
        if (RequestCtx) {
            return RequestCtx->GetWilsonTraceId();
        }
        return {};
    }

    const TString& GetRequestType() const {
        if (RequestCtx) {
            if (!RequestType) {
                RequestType = RequestCtx->GetRequestType().GetOrElse("");
            }
            return RequestType;
        }

        return Record.GetRequestType();
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetUserToken() const {
        if (RequestCtx && RequestCtx->GetInternalToken()) {
            return RequestCtx->GetInternalToken();
        }

        if (Token_) {
            return Token_;
        }

        Token_ = new NACLib::TUserToken(Record.GetUserToken());
        return Token_;
    }

    TString GetClientAddress() const {
        if (RequestCtx) {
            return RequestCtx->GetPeerName();
        }

        return Record.GetRequest().GetClientAddress();
    }

    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& GetYdbParameters() const {
        if (YdbParameters) {
            return *YdbParameters;
        }

        return Record.GetRequest().GetYdbParameters();
    }

    Ydb::StatusIds::StatusCode GetYdbStatus() const {
        return Record.GetYdbStatus();
    }

    ::Ydb::Table::QueryStatsCollection::Mode GetCollectStats() const {
        if (RequestCtx) {
            return CollectStats;
        }

        return Record.GetRequest().GetCollectStats();
    }

    const ::google::protobuf::RepeatedPtrField<::Ydb::Issue::IssueMessage>& GetQueryIssues() const {
        return Record.GetQueryIssues();
    }

    ui64 GetRequestSize() const {
        return Record.GetRequest().ByteSizeLong();
    }

    ui64 GetQuerySize() const {
        return RequestCtx ? YqlText.size() : Record.GetRequest().GetQuery().size();
    }

    bool IsInternalCall() const {
        return RequestCtx ? RequestCtx->IsInternalCall() : Record.GetRequest().GetIsInternalCall();
    }

    bool GetIsWarmupCompilation() const {
        // RequestCtx is set only if request came from grpc, warmup is internal operation
        return RequestCtx ? false : Record.GetRequest().GetIsWarmupCompilation();
    }

    ui64 GetParametersSize() const {
        if (ParametersSize > 0) {
            return ParametersSize;
        }

        for (const auto& [name, param] : GetYdbParameters()) {
            ParametersSize += name.size();
            ParametersSize += param.ByteSizeLong();
        }

        return ParametersSize;
    }

    bool GetCollectDiagnostics() const {
        return Record.GetRequest().GetCollectDiagnostics();
    }

    ui32 CalculateSerializedSize() const override {
        PrepareRemote();
        return Record.ByteSize();
    }

    bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
        PrepareRemote();
        return Record.SerializeToZeroCopyStream(chunker);
    }

    static TEvQueryRequest* Load(const TEventSerializedData* data) {
        auto pbEv = THolder<TEvQueryRequestRemote>(TEvQueryRequestRemote::Load(data));
        auto req = new TEvQueryRequest();
        req->Record.Swap(&pbEv->Record);
        return req;
    }

    void SetClientLostAction(TActorId actorId, NActors::TActorSystem* as);

    void SetUserRequestContext(TIntrusivePtr<TUserRequestContext> userRequestContext) {
        UserRequestContext = userRequestContext;
    }

    TIntrusivePtr<TUserRequestContext> GetUserRequestContext() const {
        return UserRequestContext;
    }

    void SetProgressStatsPeriod(TDuration progressStatsPeriod) {
        ProgressStatsPeriod = progressStatsPeriod;
    }

    bool GetSupportsStreamTrailingResult() const {
        return QuerySettings.SupportsStreamTrailingResult;
    }

    ui64 GetOutputChunkMaxSize() const {
        return RequestCtx ? QuerySettings.OutputChunkMaxSize : Record.GetRequest().GetOutputChunkMaxSize();
    }

    TDuration GetProgressStatsPeriod() const {
        return ProgressStatsPeriod;
    }

    void SetPoolId(const TString& poolId) {
        PoolId = poolId;
        Record.MutableRequest()->SetPoolId(PoolId);
    }

    TString GetPoolId() const {
        if (PoolId) {
            return PoolId;
        }
        return Record.GetRequest().GetPoolId();
    }

    void SetPoolConfig(const NResourcePool::TPoolSettings& config) {
        PoolConfig = config;
    }

    std::optional<NResourcePool::TPoolSettings> GetPoolConfig() const {
        return PoolConfig;
    }

    const TString& GetDatabaseId() const {
        return DatabaseId ? DatabaseId : Record.GetRequest().GetDatabaseId();
    }

    void SetDatabaseId(const TString& databaseId) {
        DatabaseId = databaseId;
    }

    ::Ydb::Query::SchemaInclusionMode GetSchemaInclusionMode() const {
        return RequestCtx ? QuerySettings.SchemaInclusionMode : Record.GetRequest().GetSchemaInclusionMode();
    }

    ::Ydb::ResultSet::Format GetResultSetFormat() const {
        return RequestCtx ? QuerySettings.ResultSetFormat : Record.GetRequest().GetResultSetFormat();
    }

    bool HasArrowFormatSettings() const {
        if (RequestCtx) {
            return ArrowFormatSettings.has_value();
        }
        return Record.GetRequest().HasArrowFormatSettings();
    }

    std::optional<NFormats::TArrowFormatSettings> GetArrowFormatSettings() const {
        if (RequestCtx) {
            return ArrowFormatSettings;
        }
        if (Record.GetRequest().HasArrowFormatSettings()) {
            return NFormats::TArrowFormatSettings::ImportFromProto(Record.GetRequest().GetArrowFormatSettings());
        }
        return std::nullopt;
    }

    bool GetSaveQueryPhysicalGraph() const {
        return SaveQueryPhysicalGraph;
    }

    void SetSaveQueryPhysicalGraph(bool saveQueryPhysicalGraph) {
        SaveQueryPhysicalGraph = saveQueryPhysicalGraph;
    }

    std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> GetQueryPhysicalGraph() const {
        return QueryPhysicalGraph;
    }

    void SetQueryPhysicalGraph(NKikimrKqp::TQueryPhysicalGraph queryPhysicalGraph) {
        QueryPhysicalGraph = std::make_shared<const NKikimrKqp::TQueryPhysicalGraph>(std::move(queryPhysicalGraph));
    }

    void SetGeneration(i64 generation) {
        Generation = generation;
    }

    i64 GetGeneration() const {
        return Generation;
    }

    void SetDisableDefaultTimeout(bool disableDefaultTimeout) {
        DisableDefaultTimeout = disableDefaultTimeout;
    }

    bool GetDisableDefaultTimeout() const {
        return DisableDefaultTimeout;
    }

    mutable NKikimrKqp::TEvQueryRequest Record;

private:
    void PrepareRemote() const;

    mutable ui64 ParametersSize = 0;
    mutable std::shared_ptr<NGRpcService::IRequestCtxMtSafe> RequestCtx;
    mutable TString TraceId;
    mutable TString RequestType;
    mutable TIntrusiveConstPtr<NACLib::TUserToken> Token_;
    TActorId RequestActorId;
    TString Database;
    TString DatabaseId;
    TString SessionId;
    TString YqlText;
    TString QueryId;
    TString PoolId;
    NKikimrKqp::EQueryAction QueryAction;
    NKikimrKqp::EQueryType QueryType;
    const ::Ydb::Table::TransactionControl* TxControl = nullptr;
    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* YdbParameters = nullptr;
    const ::Ydb::Table::QueryStatsCollection::Mode CollectStats = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
    const ::Ydb::Table::QueryCachePolicy* QueryCachePolicy = nullptr;
    const bool HasOperationParams = false;
    const TQueryRequestSettings QuerySettings = TQueryRequestSettings();
    TDuration OperationTimeout;
    TDuration CancelAfter;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    TDuration ProgressStatsPeriod;
    std::optional<NResourcePool::TPoolSettings> PoolConfig;
    std::optional<NFormats::TArrowFormatSettings> ArrowFormatSettings;
    bool SaveQueryPhysicalGraph = false;  // Used only in execute script queries
    std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> QueryPhysicalGraph;
    i64 Generation = 0;
    bool DisableDefaultTimeout = false;
};

struct TEvDataQueryStreamPart: public TEventPB<TEvDataQueryStreamPart,
    NKikimrKqp::TEvDataQueryStreamPart, TKqpEvents::EvDataQueryStreamPart> {
};

struct TEvDataQueryStreamPartAck: public TEventLocal<TEvDataQueryStreamPartAck, TKqpEvents::EvDataQueryStreamPartAck> {};

struct TEvQueryTimeout: public TEventLocal<TEvQueryTimeout, TKqpEvents::EvQueryTimeout> {
    TEvQueryTimeout(ui32 queryId)
        : QueryId(queryId) {
    }

    ui32 QueryId;
};

struct TEvQueryResponse: public TEventPBWithArena<TEvQueryResponse, NKikimrKqp::TEvQueryResponse, TKqpEvents::EvQueryResponse> {
    using TBaseEv = TEventPBWithArena<TEvQueryResponse, NKikimrKqp::TEvQueryResponse, TKqpEvents::EvQueryResponse> ;
    using TBaseEv::TEventPBBase;

    TEvQueryResponse() = default;
    explicit TEvQueryResponse(TIntrusivePtr<NActors::TProtoArenaHolder> arena)
        : TEventPBBase(arena ? std::move(arena) : MakeIntrusive<NActors::TProtoArenaHolder>())
    {}
};

} // namespace NKikimr::NKqp
