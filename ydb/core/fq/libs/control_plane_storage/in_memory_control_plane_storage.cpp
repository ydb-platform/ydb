#include "control_plane_storage.h"
#include "ydb_control_plane_storage_impl.h"

#include <ydb/core/fq/libs/control_plane_storage/internal/utils.h>

#include <ranges>

namespace NFq {

class TInMemoryControlPlaneStorageActor : public NActors::TActor<TInMemoryControlPlaneStorageActor>,
                                          public TControlPlaneStorageBase {
    struct TScopeKey {
        TString Scope;
        TString Id;

        std::strong_ordering operator<=>(const TScopeKey& other) const = default;
    };

    struct TQueries {
        using TKey = TScopeKey;

        struct TValue {
            FederatedQuery::Query Query;
            FederatedQuery::Internal::QueryInternal QueryInternal;
            TString LastJobId;
            TString User;
            TString ResultId;
            TInstant ResultExpireAt;
            ui64 Generation = 0;
            TInstant ExpireAt = TInstant::Zero();
        };

        TMap<TKey, TValue> Values;
    };

    struct TPendingQueries {
        struct TKey {
            TString Tenant;
            TString Scope;
            TString QueryId;
    
            std::strong_ordering operator<=>(const TKey& other) const = default;
        };

        struct TValue {
            TRetryLimiter RetryLimiter;
            TString Owner;
            TInstant AssignedUntil;
            TInstant LastSeenAt;
        };

        TMap<TKey, TValue> Values;
    };

    struct TJobs {
        struct TKey {
            TString Scope;
            TString QueryId;
            TString JobId;
    
            std::strong_ordering operator<=>(const TKey& other) const = default;
        };

        struct TValue {
            FederatedQuery::Job Job;
            TInstant ExpireAt = TInstant::Zero();
        };

        TMap<TKey, TValue> Values;
    };

    struct TConnections {
        using TKey = TScopeKey;
        using TEntity = FederatedQuery::Connection;

        struct TValue {
            TEntity Connection;
            TString User;

            const TEntity& GetEntity() const {
                return Connection;
            }
        };

        TMap<TKey, TValue> Values;
    };

    struct TBindings {
        using TKey = TScopeKey;
        using TEntity = FederatedQuery::Binding;

        struct TValue {
            TEntity Binding;
            TString User;

            const TEntity& GetEntity() const {
                return Binding;
            }
        };

        TMap<TKey, TValue> Values;
    };

    struct TIdempotencyKeys {
        using TKey = TScopeKey;

        struct TValue {
            TString Response;
            TInstant ExpireAt = TInstant::Zero();
        };

        TMap<TKey, TValue> Values;
    };

    struct TResultSets {
        struct TKey {
            TString ResultId;
            i32 ResultSetId;
    
            std::strong_ordering operator<=>(const TKey& other) const = default;
        };

        struct TValue {
            TVector<Ydb::Value> Rows;
            TInstant ExpireAt = TInstant::Zero();
        };

        TMap<TKey, TValue> Values;
    };

    struct TNodes {
        static constexpr TDuration TTL = TDuration::Seconds(15);

        struct TKey {
            TString Tenant;
            ui32 NodeId;
    
            std::strong_ordering operator<=>(const TKey& other) const = default;
        };

        struct TValue {
            Fq::Private::NodeInfo Node;
            TInstant ExpireAt = TInstant::Zero();
        };

        TMap<TKey, TValue> Values;
    };

    struct TComputeDatabases {
        struct TKey {
            TString Scope;
    
            std::strong_ordering operator<=>(const TKey& other) const = default;
        };

        struct TValue {
            FederatedQuery::Internal::ComputeDatabaseInternal Database;
            TInstant LastAccessAt;
        };

        TMap<TKey, TValue> Values;
    };

    using TBase = TControlPlaneStorageBase;

    TQueries Queries;
    TPendingQueries PendingQueries;
    TJobs Jobs;
    TConnections Connections;
    TBindings Bindings;
    TResultSets ResultSets;
    TIdempotencyKeys IdempotencyKeys;
    TNodes Nodes;
    TComputeDatabases ComputeDatabases;

public:
    TInMemoryControlPlaneStorageActor(
        const NConfig::TControlPlaneStorageConfig& config,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& common,
        const NConfig::TComputeConfig& computeConfig,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const TString& tenantName)
        : TActor(&TThis::StateFunc)
        , TBase(config, s3Config, common, computeConfig, counters, tenantName)
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_STORAGE";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvCreateQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListQueriesRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvControlQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListJobsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListConnectionsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListBindingsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeJobRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvWriteResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvPingTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvNodesHealthCheckRequest, Handle);
        hFunc(NActors::NMon::TEvHttpInfo, Handle);
        hFunc(TEvControlPlaneStorage::TEvFinalStatusReport, TBase::Handle);
        hFunc(TEvControlPlaneStorage::TEvGetQueryStatusRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest, Handle);
        hFunc(TEvQuotaService::TQuotaUsageRequest, Handle);
        hFunc(TEvQuotaService::TQuotaLimitChangeRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseRequest, Handle);
    )

    template <typename TEvRequest, typename TEvResponse, ERequestTypeScope TYPE_SCOPE, ERequestTypeCommon TYPE_COMMON, bool VALIDATE_REQUEST>
    class TCommonRequestContext {
    public:
        using TResultType = TPrepareResponseResultType<TEvResponse, typename TEvResponse::TProto>;
        using TResponse = TResultType::Type;
        using TAuditDetails = TResultType::TResponseAuditDetails;

        TCommonRequestContext(TInMemoryControlPlaneStorageActor& self, TEvRequest::TPtr& ev, const TString& cloudId,
            const TString& scope, const TString& logPrefix, const TString& requestStr, const TString& responseStr)
            : StartTime(TInstant::Now())
            , Event(*ev->Get())
            , Request(Event.Request)
            , LogPrefix(logPrefix)
            , RequestCounters(self.Counters.GetCounters(cloudId, scope, TYPE_SCOPE, TYPE_COMMON))
            , Self(self)
            , EventPtr(ev)
            , RequestStr(requestStr)
            , ResponseStr(responseStr)
        {
            Self.Cleanup();
            CPS_LOG_I(RequestStr);
            CPS_LOG_T(RequestStr << ":" << LogPrefix);

            RequestCounters.IncInFly();
            RequestCounters.Common->RequestBytes->Add(Event.GetByteSize());
        }

        TCommonRequestContext(TInMemoryControlPlaneStorageActor& self, TEvRequest::TPtr& ev, const TString& requestStr, const TString& responseStr)
            : TCommonRequestContext(self, ev, "", "", GetLogPrefix(ev), requestStr, responseStr)
        {}

        virtual bool Validate() {
            if constexpr(VALIDATE_REQUEST) {
                if (const auto& issues = Self.ValidateRequest(EventPtr)) {
                    Fail("query validation", issues);
                    return false;
                }
            }
            return true;
        }

        bool IsFailed() const {
            return Failed;
        }

        void Fail(const TString& logInfo, const NYql::TIssues& issues) {
            Y_ABORT_UNLESS(!Failed, "Can not fail twice");
            Failed = true;

            CPS_LOG_W(RequestStr << ":" << LogPrefix << logInfo << " FAILED: " << issues.ToOneLineString());
            Self.SendResponseIssues<TEvResponse>(EventPtr->Sender, issues, EventPtr->Cookie, TInstant::Now() - StartTime, RequestCounters);
        }

        virtual ~TCommonRequestContext() {
            if (Failed) {
                return;
            }

            Self.SendResponse<TEvResponse, typename TEvResponse::TProto>(
                TStringBuilder() << RequestStr << " - " << ResponseStr,
                NActors::TActivationContext::ActorSystem(),
                NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, {})),
                Self.SelfId(),
                std::move(EventPtr),
                StartTime,
                RequestCounters,
                [response = Response] { return response; },
                Self.Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{});
        }

        static TString GetLogPrefix(TEvRequest::TPtr& ev) {
            return TStringBuilder() << "{" << SecureDebugString(ev->Get()->Request) << "} ";
        }

    public:
        const TInstant StartTime;
        const TEvRequest& Event;
        const TEvRequest::TProto Request;
        const TString LogPrefix;
        TRequestCounters RequestCounters;
        TResponse Response;

    protected:
        TInMemoryControlPlaneStorageActor& Self;
        TEvRequest::TPtr& EventPtr;
        const TString RequestStr;
        const TString ResponseStr;
        bool Failed = false;
    };

    template <typename TEvRequest, typename TEvResponse, ERequestTypeScope TYPE_SCOPE, ERequestTypeCommon TYPE_COMMON, bool VALIDATE_REQUEST>
    class TRequestContext : public TCommonRequestContext<TEvRequest, TEvResponse, TYPE_SCOPE, TYPE_COMMON, VALIDATE_REQUEST> {
        using TBase = TCommonRequestContext<TEvRequest, TEvResponse, TYPE_SCOPE, TYPE_COMMON, VALIDATE_REQUEST>;

        Y_HAS_MEMBER(idempotency_key);
        static constexpr bool HasIdempotencyKey = THasidempotency_key<typename TEvRequest::TProto>::value;

    public:
        TRequestContext(TInMemoryControlPlaneStorageActor& self, TEvRequest::TPtr& ev, const TString& requestStr, const TString& responseStr)
            : TBase(
                self, ev, ev->Get()->CloudId, ev->Get()->Scope,
                TStringBuilder() << TBase::GetLogPrefix(ev) << MakeUserInfo(ev->Get()->User, ev->Get()->Token),
                requestStr, responseStr
            )
            , CloudId(TBase::Event.CloudId)
            , Scope(TBase::Event.Scope)
            , User(TBase::Event.User)
            , Token(TBase::Event.Token)
            , Permissions(TBase::Event.Permissions)
            , Quotas(TBase::Event.Quotas)
            , TenantInfo(TBase::Event.TenantInfo)
            , ComputeDatabase(TBase::Event.ComputeDatabase)
        {
            if constexpr (!std::is_same_v<typename TBase::TAuditDetails, void*>) {
                TBase::Response.second.CloudId = CloudId;
            }
        }

        bool Validate() override {
            if (!TBase::Validate()) {
                return false;
            }
            if constexpr (HasIdempotencyKey) {
                if (const TString& idempotencyKey = TBase::Request.idempotency_key()) {
                    if (const auto& value = TBase::Self.GetEntity(TBase::Self.IdempotencyKeys, {Scope, idempotencyKey})) {
                        if (!TBase::Response.first.ParseFromString(value->Response)) {
                            TBase::RequestCounters.Common->ParseProtobufError->Inc();
                            TBase::Fail("idempotency key parse", {NYql::TIssue("INTERNAL ERROR. Error parsing proto message for idempotency key request. Please contact internal support")});
                        } else {
                            TBase::Response.second.IdempotencyResult = true;
                        }
                        return false;
                    }
                }
            }
            return true;
        }

        ~TRequestContext() override {
            if (TBase::Failed) {
                return;
            }

            if constexpr (HasIdempotencyKey) {
                if (const TString& idempotencyKey = TBase::Request.idempotency_key()) {
                    this->Self.AddEntity(this->Self.IdempotencyKeys, {this->Scope, idempotencyKey}, {
                        .Response = TBase::Response.first.SerializeAsString(),
                        .ExpireAt = TBase::StartTime + this->Self.Config->IdempotencyKeyTtl
                    });
                }
            }
        }

    public:
        const TString CloudId;
        const TString Scope;
        const TString User;
        const TString Token;
        const TPermissions Permissions;
        const TMaybe<TQuotaMap> Quotas;
        const TTenantInfo::TPtr TenantInfo;
        const FederatedQuery::Internal::ComputeDatabaseInternal ComputeDatabase;
    };

#define HANDLE_CPS_REQUEST_IMPL(TEvRequest, TEvResponse, TContext, RTS_COUNTERS_ENUM, RTC_COUNTERS_ENUM, VALIDATE_REQUEST) \
    using TContext##TEvRequest = TContext<                                                                                 \
        TEvControlPlaneStorage::TEvRequest, TEvControlPlaneStorage::TEvResponse,                                           \
        RTS_COUNTERS_ENUM, RTC_COUNTERS_ENUM, VALIDATE_REQUEST>;                                                           \
    void Handle(TEvControlPlaneStorage::TEvRequest::TPtr& ev) {                                                            \
        TContext##TEvRequest ctx(*this, ev, #TEvRequest, #TEvResponse);                                                    \
        if (!ctx.Validate()) {                                                                                             \
            return;                                                                                                        \
        }                                                                                                                  \
        try {                                                                                                              \
            Process##TRequest(ctx);                                                                                        \
        } catch (...) {                                                                                                    \
            const auto& backtrace = TBackTrace::FromCurrentException().PrintToString();                                    \
            const auto logError = TStringBuilder() << "pocess "#TEvRequest" call, back trace:\n" << backtrace;             \
            ctx.Fail(logError, {NYql::TIssue(CurrentExceptionMessage())});                                                 \
        }                                                                                                                  \
    }                                                                                                                      \
    void Process##TRequest(TContext##TEvRequest& ctx)

#define HANDLE_CPS_REQUEST(TEvRequest, TEvResponse, COUNTERS_ENUM) HANDLE_CPS_REQUEST_IMPL(TEvRequest, TEvResponse, TRequestContext, RTS_##COUNTERS_ENUM, RTC_##COUNTERS_ENUM, true)

    HANDLE_CPS_REQUEST(TEvCreateQueryRequest, TEvCreateQueryResponse, CREATE_QUERY) {
        const auto& [query, job] = GetCreateQueryProtos(ctx.Request, ctx.User, ctx.StartTime);
        if (query.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            return ctx.Fail("query size validation", {NYql::TIssue(TStringBuilder() << "incoming request exceeded the size limit: " << query.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() <<  ". Please shorten your request")});
        }
        ctx.Response.second.After = query;

        const TString& queryId = query.meta().common().id();
        ctx.Response.first.set_query_id(queryId);

        auto queryInternal = GetQueryInternalProto(ctx.Request, ctx.CloudId, ctx.Token, ctx.Quotas);
        const auto queryType = ctx.Request.content().type();
        if (ctx.Request.execute_mode() != FederatedQuery::SAVE) {
            *queryInternal.mutable_compute_connection() = ctx.ComputeDatabase.connection();
            FillConnectionsAndBindings(
                queryInternal,
                queryType,
                GetEntities(Connections, ctx.Scope, ctx.User),
                GetEntitiesWithVisibilityPriority(Connections, ctx.Scope, ctx.User),
                GetEntitiesWithVisibilityPriority(Bindings, ctx.Scope, ctx.User)
            );
        }
        if (queryInternal.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            return ctx.Fail("query internal size validation", {NYql::TIssue(TStringBuilder() << "the size of all connections and bindings in the project exceeded the limit: " << queryInternal.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() << ". Please reduce the number of connections and bindings")});
        }

        const auto& jobId = job.meta().id();
        if (ctx.Request.execute_mode() != FederatedQuery::SAVE) {
            AddEntity(Jobs, {ctx.Scope, queryId, jobId}, {job});

            TRetryLimiter retryLimiter;
            retryLimiter.Assign(0, ctx.StartTime, 0.0);

            AddEntity(PendingQueries, {
                .Tenant = ctx.TenantInfo->Assign(ctx.CloudId, ctx.Scope, queryType, TenantName).TenantName,
                .Scope = ctx.Scope,
                .QueryId = queryId
            }, {.RetryLimiter = retryLimiter});
        }

        AddEntity(Queries, {ctx.Scope, queryId}, {
            .Query = query,
            .QueryInternal = queryInternal,
            .LastJobId = jobId,
            .User = ctx.User
        });
    }

    void Handle(TEvControlPlaneStorage::TEvListQueriesRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListQueriesRequest::TPtr,
            FederatedQuery::ListQueriesResult,
            TEvControlPlaneStorage::TEvListQueriesResponse>(ev, "ListQueriesRequest");
    }

    HANDLE_CPS_REQUEST(TEvDescribeQueryRequest, TEvDescribeQueryResponse, DESCRIBE_QUERY) {
        const auto& query = GetEntity(Queries, {ctx.Scope, ctx.Request.query_id()});
        if (!query) {
            return ctx.Fail("find query", {NYql::TIssue("Query does not exist")});
        }

        *ctx.Response.mutable_query() = query->Query;
        FillDescribeQueryResult(ctx.Response, query->QueryInternal, ctx.User, ctx.Permissions);
    }

    void Handle(TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr,
            FederatedQuery::ModifyQueryResult,
            TEvControlPlaneStorage::TEvModifyQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "ModifyQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr,
            FederatedQuery::DeleteQueryResult,
            TEvControlPlaneStorage::TEvDeleteQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "DeleteQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvControlQueryRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvControlQueryRequest::TPtr,
            FederatedQuery::ControlQueryResult,
            TEvControlPlaneStorage::TEvControlQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "ControlQueryRequest");
    }

    HANDLE_CPS_REQUEST(TEvGetResultDataRequest, TEvGetResultDataResponse, GET_RESULT_DATA) {
        const auto& query = GetEntity(Queries, {ctx.Scope, ctx.Request.query_id()});
        if (!query) {
            return ctx.Fail("find query", {NYql::TIssue("Query does not exist")});
        }

        if (!HasViewAccess(GetResultDataReadPerimssions(ctx.Event), query->Query.content().acl().visibility(), query->User, ctx.User)) {
            return ctx.Fail("check ACL", {NYql::TIssue("Permission denied")});
        }

        const auto resultSetIndex = ctx.Request.result_set_index();
        const auto& resultSetMeta = query->Query.result_set_meta();
        if (resultSetIndex >= resultSetMeta.size()) {
            return ctx.Fail("check result set index", {NYql::TIssue(TStringBuilder() << "Result set index out of bound: " << resultSetIndex << " >= " << resultSetMeta.size())});
        }

        const auto expireAt = query->ResultExpireAt;
        if (query->Query.meta().status() != FederatedQuery::QueryMeta::COMPLETED || !expireAt) {
            return ctx.Fail("check status", {NYql::TIssue("Result doesn't exist")});
        }

        if (expireAt < TInstant::Now()) {
            return ctx.Fail("check expiration", {NYql::TIssue("Result removed by TTL")});
        }

        const auto& result = GetEntity(ResultSets, {query->ResultId, ctx.Request.result_set_index()});
        if (!result) {
            return ctx.Fail("get result", {NYql::TIssue("INTERNAL ERROR. Failed to find result set")});
        }

        auto& resultSet = *ctx.Response.mutable_result_set();
        *resultSet.mutable_columns() = resultSetMeta[resultSetIndex].column();

        const i64 offset = ctx.Request.offset();
        const i64 numberRows = result->Rows.size();
        for (i64 rowId = offset; rowId < offset + ctx.Request.limit() && rowId < numberRows; ++rowId) {
            *resultSet.add_rows() = result->Rows[rowId];
        }
    }

    void Handle(TEvControlPlaneStorage::TEvListJobsRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListJobsRequest::TPtr,
            FederatedQuery::ListJobsResult,
            TEvControlPlaneStorage::TEvListJobsResponse>(ev, "ListJobsRequest");
    }

    HANDLE_CPS_REQUEST(TEvCreateConnectionRequest, TEvCreateConnectionResponse, CREATE_CONNECTION) {
        const auto& content = ctx.Request.content();
        const auto visibility = content.acl().visibility();
        const auto& name = content.name();
        if (!CheckConnectionOrBindingName(Connections, ctx.Scope, ctx.User, visibility, name)) {
            return ctx.Fail("check name", {NYql::TIssue("Connection with the same name already exists. Please choose another name")});
        }
        if (!CheckConnectionOrBindingName(Bindings, ctx.Scope, ctx.User, visibility, name)) {
            return ctx.Fail("check name", {NYql::TIssue("Binding with the same name already exists. Please choose another name")});
        }
        if (GetNumberEntitiesByScope(Connections, ctx.Scope) >= Config->Proto.GetMaxCountConnections()) {
            return ctx.Fail("check number", {NYql::TIssue(TStringBuilder() << "Too many connections in folder: " << Config->Proto.GetMaxCountConnections() << ". Please remove unused connections")});
        }

        const auto& [connection, _] = GetCreateConnectionProtos(ctx.Request, ctx.CloudId, ctx.User, ctx.StartTime);
        ctx.Response.second.After = connection;

        const TString& connectionId = connection.meta().id();
        ctx.Response.first.set_connection_id(connectionId);

        AddEntity(Connections, {ctx.Scope, connectionId}, {
            .Connection = connection,
            .User = ctx.User
        });
    }

    HANDLE_CPS_REQUEST(TEvListConnectionsRequest, TEvListConnectionsResponse, LIST_CONNECTIONS) {
        auto connections = GetEntities(Connections, ctx.Scope, ctx.User);
        auto& resultConnections = *ctx.Response.mutable_connection();
        const auto& filter = ctx.Request.filter();

        auto it = std::lower_bound(connections.begin(), connections.end(), ctx.Request.page_token(), [](const auto& l, const auto& r) {
            return l.meta().id() < r;
        });
        for (; it != connections.end(); ++it) {
            const auto& content = it->content();
            if (const auto& nameFilter = filter.name()) {
                const auto& name = content.name();
                if (ctx.Event.IsExactNameMatch ? name != nameFilter : !name.Contains(nameFilter)) {
                    continue;
                }
            }

            if (filter.created_by_me() && it->meta().created_by() != ctx.User) {
                continue;
            }

            if (filter.connection_type() != FederatedQuery::ConnectionSetting::CONNECTION_TYPE_UNSPECIFIED && content.setting().connection_case() != static_cast<FederatedQuery::ConnectionSetting::ConnectionCase>(filter.connection_type())) {
                continue;
            }

            if (filter.visibility() != FederatedQuery::Acl::VISIBILITY_UNSPECIFIED && content.acl().visibility() != filter.visibility()) {
                continue;
            }

            *resultConnections.Add() = *it;
            if (resultConnections.size() == ctx.Request.limit() + 1) {
                ctx.Response.set_next_page_token(ctx.Response.connection(ctx.Response.connection_size() - 1).meta().id());
                resultConnections.RemoveLast();
                break;
            }
        }
    }

    HANDLE_CPS_REQUEST(TEvDescribeConnectionRequest, TEvDescribeConnectionResponse, DESCRIBE_CONNECTION) {
        const auto& connection = GetEntity(Connections, {ctx.Scope, ctx.Request.connection_id()});
        if (!connection) {
            return ctx.Fail("find connection", {NYql::TIssue("Connection does not exist")});
        }

        const auto& connectionProto = connection->Connection;
        if (!HasViewAccess(GetPermissions(ctx.Permissions, ctx.User), connectionProto.content().acl().visibility(), connectionProto.meta().created_by(), ctx.User)) {
            return ctx.Fail("check permissions", {NYql::TIssue("Permission denied")});
        }

        *ctx.Response.mutable_connection() = connectionProto;
    }

    void Handle(TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr,
            FederatedQuery::ModifyConnectionResult,
            TEvControlPlaneStorage::TEvModifyConnectionResponse,
            TAuditDetails<FederatedQuery::Connection>>(ev, "ModifyConnectionRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr,
            FederatedQuery::DeleteConnectionResult,
            TEvControlPlaneStorage::TEvDeleteConnectionResponse,
            TAuditDetails<FederatedQuery::Connection>>(ev, "DeleteConnectionRequest");
    }

    HANDLE_CPS_REQUEST(TEvCreateBindingRequest, TEvCreateBindingResponse, CREATE_BINDING) {
        const auto& content = ctx.Request.content();
        const auto visibility = content.acl().visibility();
        const auto& name = content.name();
        if (!CheckConnectionOrBindingName(Connections, ctx.Scope, ctx.User, visibility, name)) {
            return ctx.Fail("check name", {NYql::TIssue("Connection with the same name already exists. Please choose another name")});
        }
        if (!CheckConnectionOrBindingName(Bindings, ctx.Scope, ctx.User, visibility, name)) {
            return ctx.Fail("check name", {NYql::TIssue("Binding with the same name already exists. Please choose another name")});
        }
        if (GetNumberEntitiesByScope(Bindings, ctx.Scope) >= Config->Proto.GetMaxCountBindings()) {
            return ctx.Fail("check number", {NYql::TIssue(TStringBuilder() << "Too many bindings in folder: " << Config->Proto.GetMaxCountBindings() << ". Please remove unused bindings")});
        }

        const auto& connection = GetEntity(Connections, {ctx.Scope, content.connection_id()});
        if (!connection) {
            return ctx.Fail("check connection", {NYql::TIssue("Connection for binding not found")});
        }

        const auto connectionVisibility = connection->Connection.content().acl().visibility();
        if (content.acl().visibility() == FederatedQuery::Acl::SCOPE && connectionVisibility == FederatedQuery::Acl::PRIVATE) {
            return ctx.Fail("check connection ACL", {NYql::TIssue("Binding with SCOPE visibility cannot refer to connection with PRIVATE visibility")});
        }

        if (!HasManageAccess(GetCreateBindingPerimssions(ctx.Event), connectionVisibility, connection->User, ctx.User)) {
            return ctx.Fail("check connection ACL", {NYql::TIssue("Permission denied for binding connection")});
        }

        const auto& [binding, _] = GetCreateBindingProtos(ctx.Request, ctx.CloudId, ctx.User, ctx.StartTime);
        ctx.Response.second.After = binding;

        const TString& bindingId = binding.meta().id();
        ctx.Response.first.set_binding_id(bindingId);

        AddEntity(Bindings, {ctx.Scope, bindingId}, {
            .Binding = binding,
            .User = ctx.User
        });
    }

    HANDLE_CPS_REQUEST(TEvListBindingsRequest, TEvListBindingsResponse, LIST_BINDINGS) {
        auto bindings = GetEntities(Bindings, ctx.Scope, ctx.User);
        auto& resultBindings = *ctx.Response.mutable_binding();
        const auto& filter = ctx.Request.filter();

        auto it = std::lower_bound(bindings.begin(), bindings.end(), ctx.Request.page_token(), [](const auto& l, const auto& r) {
            return l.meta().id() < r;
        });
        for (; it != bindings.end(); ++it) {
            const auto& content = it->content();
            const auto& connectionId = content.connection_id();
            if (filter.connection_id() && connectionId != filter.connection_id()) {
                continue;
            }

            const auto& name = content.name();
            if (const auto& nameFilter = filter.name()) {
                if (ctx.Event.IsExactNameMatch ? name != nameFilter : !name.Contains(nameFilter)) {
                    continue;
                }
            }

            const auto& meta = it->meta();
            if (filter.created_by_me() && meta.created_by() != ctx.User) {
                continue;
            }

            const auto visibility = content.acl().visibility();
            if (filter.visibility() != FederatedQuery::Acl::VISIBILITY_UNSPECIFIED && visibility != filter.visibility()) {
                continue;
            }

            auto& resultBinding = *resultBindings.Add();
            resultBinding.set_name(name);
            resultBinding.set_connection_id(connectionId);
            resultBinding.set_visibility(visibility);
            *resultBinding.mutable_meta() = meta;

            switch (content.setting().binding_case()) {
                case FederatedQuery::BindingSetting::kDataStreams:
                    resultBinding.set_type(FederatedQuery::BindingSetting::DATA_STREAMS);
                    break;
                case FederatedQuery::BindingSetting::kObjectStorage:
                    resultBinding.set_type(FederatedQuery::BindingSetting::OBJECT_STORAGE);
                    break;
                case FederatedQuery::BindingSetting::BINDING_NOT_SET:
                    break;
            }

            if (resultBindings.size() == ctx.Request.limit() + 1) {
                ctx.Response.set_next_page_token(ctx.Response.binding(ctx.Response.binding_size() - 1).meta().id());
                resultBindings.RemoveLast();
                break;
            }
        }
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr,
            FederatedQuery::DescribeBindingResult,
            TEvControlPlaneStorage::TEvDescribeBindingResponse>(ev, "DescribeBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr,
            FederatedQuery::ModifyBindingResult,
            TEvControlPlaneStorage::TEvModifyBindingResponse,
            TAuditDetails<FederatedQuery::Binding>>(ev, "ModifyBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr,
            FederatedQuery::DeleteBindingResult,
            TEvControlPlaneStorage::TEvDeleteBindingResponse,
            TAuditDetails<FederatedQuery::Binding>>(ev, "DeleteBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr,
            FederatedQuery::DescribeJobResult,
            TEvControlPlaneStorage::TEvDescribeJobResponse>(ev, "DescribeJobRequest");
    }

    HANDLE_CPS_REQUEST_IMPL(TEvWriteResultDataRequest, TEvWriteResultDataResponse, TCommonRequestContext, RTS_MAX, RTC_WRITE_RESULT_DATA, true) {
        ctx.Response.set_request_id(ctx.Request.request_id());

        const auto offset = ctx.Request.offset();
        const auto& newRows = ctx.Request.result_set().rows();

        auto& [resultRows, expireAt] = ResultSets.Values[{ctx.Request.result_id().value(), static_cast<i32>(ctx.Request.result_set_id())}];
        expireAt = NProtoInterop::CastFromProto(ctx.Request.deadline());

        resultRows.resize(std::max(offset + newRows.size(), resultRows.size()));
        for (size_t i = offset; const auto& row : newRows) {
            resultRows[i++] = row;
        }
    }

    HANDLE_CPS_REQUEST_IMPL(TEvGetTaskRequest, TEvGetTaskResponse, TCommonRequestContext, RTS_MAX, RTC_GET_TASK, true) {
        const auto& tasksInternal = GetActiveTasks(ctx);

        TVector<TTask> tasks;
        tasks.reserve(tasksInternal.size());
        for (const auto& taskInternal : tasksInternal) {
            if (const auto& task = AssignTask(ctx, taskInternal)) {
                tasks.emplace_back(*task);
            }
            if (ctx.IsFailed()) {
                return;
            }
        }

        FillGetTaskResult(ctx.Response, tasks);
    }

    HANDLE_CPS_REQUEST_IMPL(TEvPingTaskRequest, TEvPingTaskResponse, TCommonRequestContext, RTS_MAX, RTC_PING_TASK, true) {
        const auto& scope = ctx.Request.scope();
        const auto& queryId = ctx.Request.query_id().value();

        auto query = GetEntity(Queries, {scope, queryId});
        if (!query) {
            return ctx.Fail("get query", {NYql::TIssue("INTERNAL ERROR. Query for ping task not found")});
        }

        auto job = GetEntity(Jobs, {scope, queryId, query->LastJobId});
        if (!job) {
            return ctx.Fail("get job", {NYql::TIssue("INTERNAL ERROR. Job for ping task not found")});
        }

        auto pendingQuery = GetEntity(PendingQueries, {ctx.Request.tenant(), scope, queryId});
        if (!pendingQuery) {
            return ctx.Fail("get pending query", {NYql::TIssue("INTERNAL ERROR. Pending query for ping task not found")});
        }

        auto resuest = ctx.Request;
        auto finalStatus = std::make_shared<TFinalStatus>();

        TDuration backoff = Config->TaskLeaseTtl;
        TInstant expireAt = ctx.StartTime + Config->AutomaticQueriesTtl;
        UpdateTaskInfo(TActivationContext::ActorSystem(), resuest, finalStatus, query->Query, query->QueryInternal, job->Job, pendingQuery->Owner, pendingQuery->RetryLimiter, backoff, expireAt);
        PingTask(ctx, *query, *job, *pendingQuery, backoff, expireAt);

        if (IsTerminalStatus(ctx.Request.status())) {
            FillQueryStatistics(finalStatus, query->Query, query->QueryInternal, pendingQuery->RetryLimiter);
        }
        Send(SelfId(), new TEvControlPlaneStorage::TEvFinalStatusReport(
            queryId, finalStatus->JobId, finalStatus->CloudId, scope, std::move(finalStatus->FinalStatistics),
            finalStatus->Status, finalStatus->StatusCode, finalStatus->QueryType, finalStatus->Issues, finalStatus->TransientIssues));
    }

    HANDLE_CPS_REQUEST_IMPL(TEvNodesHealthCheckRequest, TEvNodesHealthCheckResponse, TCommonRequestContext, RTS_MAX, RTC_NODES_HEALTH_CHECK, true) {
        const auto& tenant = ctx.Request.tenant();
        const auto& node = ctx.Request.node();

        AddEntity(Nodes, {tenant, node.node_id()}, {
            .Node = node,
            .ExpireAt = ctx.StartTime + TNodes::TTL
        });

        for (const auto& [key, value] : Nodes.Values) {
            if (key.Tenant == tenant) {
                *ctx.Response.add_nodes() = value.Node;
            }
        }
    }

    void Handle(TEvControlPlaneStorage::TEvGetQueryStatusRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvGetQueryStatusRequest::TPtr,
            FederatedQuery::GetQueryStatusResult,
            TEvControlPlaneStorage::TEvGetQueryStatusResponse>(ev, "GetQueryStatusRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr,
            Fq::Private::CreateRateLimiterResourceResult,
            TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse>(ev, "CreateRateLimiterResourceRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr,
            Fq::Private::DeleteRateLimiterResourceResult,
            TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse>(ev, "DeleteRateLimiterResourceRequest");
    }

    void Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->MetricName, 0));
    }

    void Handle(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        Send(ev->Sender, new TEvQuotaService::TQuotaLimitChangeResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->MetricName, ev->Get()->Limit, ev->Get()->LimitRequested));
    }

    HANDLE_CPS_REQUEST_IMPL(TEvCreateDatabaseRequest, TEvCreateDatabaseResponse, TCommonRequestContext, RTS_CREATE_DATABASE, RTC_CREATE_DATABASE, false) {
        AddEntity(ComputeDatabases, {ctx.Event.Scope}, {
            .Database = ctx.Request,
            .LastAccessAt = TInstant::Now()
        });
    }

    HANDLE_CPS_REQUEST_IMPL(TEvDescribeDatabaseRequest, TEvDescribeDatabaseResponse, TCommonRequestContext, RTS_DESCRIBE_DATABASE, RTC_DESCRIBE_DATABASE, false) {
        const auto& database = GetEntity(ComputeDatabases, {ctx.Event.Scope});
        if (!database) {
            NYql::TIssue issue(TStringBuilder() << "Compute database does not exist for scope " << ctx.Event.Scope);
            issue.SetCode(TIssuesIds::ACCESS_DENIED, NYql::TSeverityIds::S_ERROR);
            return ctx.Fail("find compute database", {issue});
        }

        ctx.Response = database->Database;
    }

    HANDLE_CPS_REQUEST_IMPL(TEvModifyDatabaseRequest, TEvModifyDatabaseResponse, TCommonRequestContext, RTS_MODIFY_DATABASE, RTC_MODIFY_DATABASE, false) {
        const auto it = ComputeDatabases.Values.find({ctx.Event.Scope});

        if (const auto lastAccessAt = ctx.Event.LastAccessAt) {
            if (it != ComputeDatabases.Values.end()) {
                it->second.LastAccessAt = *lastAccessAt;
            }
            return;
        }

        if (it == ComputeDatabases.Values.end()) {
            NYql::TIssue issue(TStringBuilder() << "Compute database does not exist for scope " << ctx.Event.Scope);
            issue.SetCode(TIssuesIds::ACCESS_DENIED, NYql::TSeverityIds::S_ERROR);
            return ctx.Fail("update compute database", {issue});
        }

        if (const auto synchronized = ctx.Event.Synchronized) {
            it->second.Database.set_synchronized(*synchronized);
        }

        if (const auto wmSynchronized = ctx.Event.WorkloadManagerSynchronized) {
            it->second.Database.set_workload_manager_synchronized(*wmSynchronized);
        }
    }

#undef HANDLE_CPS_REQUEST
#undef HANDLE_CPS_REQUEST_IMPL

    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("Unimplemented " << __LINE__);
        TStringStream str;
        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(str.Str()));
    }

    template<typename TRequest, typename TResult, typename TEvResult>
    void SendEmptyResponse(TRequest& ev, std::string logText) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("SendEmptyResponse");
        CPS_LOG_I(logText);

        TResult result = {};
        auto event = std::make_unique<TEvResult>(result);
        NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
    }

    template<typename TRequest, typename TResult, typename TEvResult, typename TAuditDetails>
    void SendEmptyAuditResponse(TRequest& ev, std::string logText) {
        LOG_YQ_CONTROL_PLANE_STORAGE_CRIT("SendEmptyAuditResponse");
        CPS_LOG_I(logText);

        TResult result = {};
        TAuditDetails auditDetails = {};
        auto event = std::make_unique<TEvResult>(result, auditDetails);
        NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
    }

private:
    template <typename TTable>
    static std::optional<typename TTable::TValue> GetEntity(const TTable& table, const TTable::TKey& key) {
        const auto it = table.Values.find(key);
        if (it == table.Values.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    template <typename TTable>
    static bool AddEntity(TTable& table, const TTable::TKey& key, const TTable::TValue& value) {
        return table.Values.emplace(key, value).second;
    }

    template <typename TValue>
    auto GetScopeRange(const TMap<TScopeKey, TValue>& table, const TString& scope) const {
        const auto startIt = table.lower_bound({scope, ""});

        std::ranges::subrange range(startIt, table.end());
        return range | std::views::take_while([scope](auto element) {
            return element.first.Scope == scope;
        });
    }

    template <typename TValue>
    auto GetVisibleRange(const TMap<TScopeKey, TValue>& table, const TString& scope, const TString& user, std::optional<FederatedQuery::Acl::Visibility> visibility = std::nullopt) const {
        auto range = GetScopeRange(table, scope);
        return range | std::views::filter([user, visibility, ignorePrivate = Config->Proto.GetIgnorePrivateSources()](const auto& element) {
            const auto entityVisibility = element.second.GetEntity().content().acl().visibility();
            if (ignorePrivate && entityVisibility == FederatedQuery::Acl::PRIVATE) {
                return false;
            }
            if (visibility && entityVisibility != *visibility) {
                return false;
            }
            return entityVisibility == FederatedQuery::Acl::SCOPE || element.second.User == user;
        });
    }

    template <typename TTable>
    TVector<typename TTable::TEntity> GetEntities(const TTable& table, const TString& scope, const TString& user) const {
        auto range = GetVisibleRange(table.Values, scope, user) | std::views::transform([](const auto& element) {
            return element.second.GetEntity();
        }) | std::views::common;
        return {range.begin(), range.end()};
    }

    template <typename TTable>
    THashMap<TString, typename TTable::TEntity> GetEntitiesWithVisibilityPriority(const TTable& table, const TString& scope, const TString& user) const {
        THashMap<TString, typename TTable::TEntity> entities;
        for (const auto& [_, value] : GetVisibleRange(table.Values, scope, user)) {
            const auto& entity = value.GetEntity();
            const auto visibility = entity.content().acl().visibility();
            const TString& name = entity.content().name();
            if (auto it = entities.find(name); it != entities.end()) {
                if (visibility == FederatedQuery::Acl::PRIVATE) {
                    it->second = entity;
                }
            } else {
                entities[name] = entity;
            }
        }
        return entities;
    }

    template <typename TTable>
    bool CheckConnectionOrBindingName(const TTable& table, const TString& scope, const TString& user, FederatedQuery::Acl::Visibility visibility, const TString& name) const {
        auto range = GetVisibleRange(table.Values, scope, user, visibility) | std::views::transform([](const auto& element) {
            return element.second.GetEntity().content().name();
        });
        return std::ranges::find(range, name) == range.end();
    }

    template <typename TTable>
    ui64 GetNumberEntitiesByScope(const TTable& table, const TString& scope) const {
        auto range = GetScopeRange(table.Values, scope) | std::views::common;
        return std::distance(range.begin(), range.end());
    }

    TPermissions GetPermissions(const TPermissions& requestPermissions, const TString& user) const {
        TPermissions permissions = Config->Proto.GetEnablePermissions()
                    ? requestPermissions
                    : TPermissions{TPermissions::VIEW_PUBLIC};
        if (IsSuperUser(user)) {
            permissions.SetAll();
        }
        return permissions;
    }

private:
    void Cleanup() {
        CleanupTable(Queries);
        CleanupTable(Jobs);
        CleanupTable(ResultSets);
        CleanupTable(IdempotencyKeys);
        CleanupTable(Nodes);
    }

    template <typename TTable>
    static void CleanupTable(TTable& table) {
        const auto now = TInstant::Now();
        std::erase_if(table.Values, [now](const auto& item) {
            const auto expireAt = item.second.ExpireAt;
            return expireAt && expireAt < now;
        });
    }

private:
    // Get / Ping task utils

    struct TTaskInternal {
        TTask Task;
        TString Owner;
        TRetryLimiter RetryLimiter;
        TString TenantName;
        bool ShouldAbortTask;
    };

    TVector<TTaskInternal> GetActiveTasks(const TCommonRequestContextTEvGetTaskRequest& ctx) const {
        const ui64 tasksBatchSize = Config->Proto.GetTasksBatchSize();
        const TString& tenantName = ctx.Request.tenant();

        TVector<TTaskInternal> tasks;
        tasks.reserve(std::min(tasksBatchSize, PendingQueries.Values.size()));
        for (const auto& [key, query] : PendingQueries.Values) {
            if (key.Tenant != tenantName || query.AssignedUntil >= ctx.StartTime) {
                continue;
            }

            TTaskInternal& taskInternal = tasks.emplace_back();
            taskInternal.Owner = ctx.Request.owner_id();
            taskInternal.TenantName = tenantName;
            taskInternal.RetryLimiter = query.RetryLimiter;

            auto& task = taskInternal.Task;
            task.Scope = key.Scope;
            task.QueryId = key.QueryId;

            if (query.Owner) {
                CPS_LOG_T("Task (Query): " << task.QueryId <<  " Lease TIMEOUT, RetryCounterUpdatedAt " << taskInternal.RetryLimiter.RetryCounterUpdatedAt << " LastSeenAt: " << query.LastSeenAt);
                taskInternal.ShouldAbortTask = !taskInternal.RetryLimiter.UpdateOnRetry(query.LastSeenAt, Config->TaskLeaseRetryPolicy, ctx.StartTime);
            }
            task.RetryCount = taskInternal.RetryLimiter.RetryCount;

            CPS_LOG_T("Task (Query): " << task.QueryId <<  " RetryRate: " << taskInternal.RetryLimiter.RetryRate << " RetryCounter: " << taskInternal.RetryLimiter.RetryCount << " At: " << taskInternal.RetryLimiter.RetryCounterUpdatedAt << (taskInternal.ShouldAbortTask ? " ABORTED" : ""));

            if (tasks.size() >= tasksBatchSize) {
                break;
            }
        }

        std::shuffle(tasks.begin(), tasks.end(), std::default_random_engine(TInstant::Now().MicroSeconds()));
        const ui64 numTasksProportion = Config->Proto.GetNumTasksProportion();
        tasks.resize((tasks.size() + numTasksProportion - 1) / numTasksProportion);

        return tasks;
    }

    std::optional<TTask> AssignTask(TCommonRequestContextTEvGetTaskRequest& ctx, TTaskInternal taskInternal) {
        auto& task = taskInternal.Task;

        const auto& query = GetEntity(Queries, {task.Scope, task.QueryId});
        if (!query) {
            ctx.Fail("task build", {NYql::TIssue(TStringBuilder() << "INTERNAL ERROR. Not found query for task in scope " << task.Scope << " with query id " << task.QueryId)});
            return std::nullopt;
        }

        task.Generation = query->Generation + 1;
        task.Query = query->Query;
        task.Internal = query->QueryInternal;
        task.Deadline = TInstant::Now() + (task.Query.content().automatic() ? std::min(Config->AutomaticQueriesTtl, Config->ResultSetsTtl) : Config->ResultSetsTtl);
        *task.Internal.mutable_result_ttl() = NProtoInterop::CastToProto(Config->ResultSetsTtl);

        if (Config->Proto.GetDisableCurrentIam()) {
            task.Internal.clear_token();
        }

        if (taskInternal.ShouldAbortTask) {
            AddTransientIssues(task.Query.mutable_transient_issue(), {NYql::TIssue("Query was aborted by system due to high failure rate")});
            task.Query.mutable_meta()->set_status(FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM);
        }

        if (const auto tenantInfo = ctx.Event.TenantInfo) {
            const auto& mapResult = tenantInfo->Assign(task.Internal.cloud_id(), task.Scope, task.Query.content().type(), taskInternal.TenantName);
            if (mapResult.TenantName != taskInternal.TenantName) {
                UpdateTaskState(ctx, taskInternal, mapResult.TenantName);
                return std::nullopt;
            }
            if (tenantInfo->TenantState.Value(taskInternal.TenantName, TenantState::Active) != TenantState::Active) {
                return std::nullopt;
            }
        }

        UpdateTaskState(ctx, taskInternal);

        return task;
    }

    void UpdateTaskState(const TCommonRequestContextTEvGetTaskRequest& ctx, const TTaskInternal& taskInternal, const TString& newTenant = "") {
        const auto& task = taskInternal.Task;

        const auto queryIt = Queries.Values.find({task.Scope, task.QueryId});
        if (queryIt != Queries.Values.end()) {
            queryIt->second.Query = task.Query;
            queryIt->second.QueryInternal = task.Internal;
            queryIt->second.Generation = task.Generation;
        }

        const auto pendingIt = PendingQueries.Values.find({taskInternal.TenantName, task.Scope, task.QueryId});
        if (pendingIt != PendingQueries.Values.end()) {
            pendingIt->second.Owner = taskInternal.Owner;
            pendingIt->second.RetryLimiter = taskInternal.RetryLimiter;
            pendingIt->second.AssignedUntil = ctx.StartTime + Config->TaskLeaseTtl;
            pendingIt->second.LastSeenAt = ctx.StartTime;
            if (newTenant) {
                const auto value = pendingIt->second;
                PendingQueries.Values.erase(pendingIt);
                AddEntity(PendingQueries, {newTenant, task.Scope, task.QueryId}, value);
            }
        }
    }

    void PingTask(const TCommonRequestContextTEvPingTaskRequest& ctx, const TQueries::TValue& query, const TJobs::TValue& job, const TPendingQueries::TValue& pendingQuery, TDuration backoff, TInstant expireAt) {
        const auto& scope = ctx.Request.scope();
        const auto& queryId = ctx.Request.query_id().value();
        const auto status = query.Query.meta().status();

        const auto pendingIt = PendingQueries.Values.find({ctx.Request.tenant(), scope, queryId});
        if (pendingIt != PendingQueries.Values.end()) {
            if (IsTerminalStatus(status)) {
                PendingQueries.Values.erase(pendingIt);
            } else {
                pendingIt->second = pendingQuery;
                pendingIt->second.AssignedUntil = ctx.StartTime + backoff;
            }
        }

        const bool automaticFinished = IsTerminalStatus(status) && query.Query.content().automatic();
        const auto jobIt = Jobs.Values.find({scope, queryId, job.Job.meta().id()});
        if (jobIt != Jobs.Values.end()) {
            jobIt->second = job;
            jobIt->second.ExpireAt = automaticFinished ? expireAt : TInstant::Zero();
        }

        const auto queryIt = Queries.Values.find({scope, queryId});
        if (queryIt != Queries.Values.end()) {
            queryIt->second = query;
            if (ctx.Request.has_result_id()) {
                queryIt->second.ResultId = ctx.Request.result_id().value();
            }
            queryIt->second.ResultExpireAt = status == FederatedQuery::QueryMeta::COMPLETED ? NProtoInterop::CastFromProto(ctx.Request.deadline()) : TInstant::Zero();
            queryIt->second.ExpireAt = automaticFinished ? expireAt : TInstant::Zero();
        }
    }
};

NActors::TActorId ControlPlaneStorageServiceActorId(ui32 nodeId) {
    constexpr TStringBuf name = "CTRLSTORAGE";
    return NActors::TActorId(nodeId, name);
}

NActors::IActor* CreateInMemoryControlPlaneStorageServiceActor(
    const NConfig::TControlPlaneStorageConfig& config,
    const NYql::TS3GatewayConfig& s3Config,
    const NConfig::TCommonConfig& common,
    const NConfig::TComputeConfig& computeConfig,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const TString& tenantName) {
    return new TInMemoryControlPlaneStorageActor(config, s3Config, common, computeConfig, counters, tenantName);
}

} // NFq
