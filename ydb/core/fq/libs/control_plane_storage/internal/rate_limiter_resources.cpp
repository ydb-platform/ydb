#include "utils.h"

#include <ydb/public/lib/fq/scope.h>
#include <ydb/core/fq/libs/control_plane_storage/request_actor.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>

#include <util/datetime/base.h>
#include <util/string/split.h>

#include <google/protobuf/util/time_util.h>

namespace NFq {

namespace {

template <class TRequest, class TResponse, class TDerived>
class TRateLimiterRequestActor : public TControlPlaneRequestActor<TRequest, TResponse, TDerived> {
    using TRequestActorBase = TControlPlaneRequestActor<TRequest, TResponse, TDerived>;

protected:
    using TRequestActorBase::ReplyWithError;

public:
    TRateLimiterRequestActor(typename TRequest::TPtr&& ev, TRequestCounters requestCounters, TDebugInfoPtr debugInfo, TDbPool::TPtr dbPool, TYdbConnectionPtr ydbConnection, const std::shared_ptr<::NFq::TControlPlaneStorageConfig>& config)
        : TRequestActorBase(std::move(ev), std::move(requestCounters), std::move(debugInfo), std::move(dbPool), std::move(ydbConnection), config)
        , QueryId(this->Request->Get()->Request.query_id().value())
        , OwnerId(this->Request->Get()->Request.owner_id())
    {
    }

    void Start() {
        auto& request = this->Request->Get()->Request;
        const TString& scope = request.scope();
        const TString& tenant = request.tenant();

        if (NYql::TIssues issues = ValidateCreateOrDeleteRateLimiterResource(QueryId, scope, tenant, OwnerId)) {
            CPS_LOG_W(TDerived::RequestTypeName << "Request: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
            ReplyWithError(issues);
            return;
        }

        this->Become(&TDerived::StateFunc);

        TSqlQueryBuilder readQueryBuilder(this->YdbConnection->TablePathPrefix, TDerived::RequestTypeName);
        readQueryBuilder.AddString("query_id", QueryId);
        readQueryBuilder.AddString("scope", scope);
        readQueryBuilder.AddString("tenant", tenant);
        TStringBuilder text;
        text <<
        "SELECT `" SCOPE_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "` FROM " PENDING_SMALL_TABLE_NAME
            " WHERE `" TENANT_COLUMN_NAME "` = $tenant"
                " AND `" SCOPE_COLUMN_NAME "` = $scope"
                " AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"

        "SELECT `" INTERNAL_COLUMN_NAME "`";
        if constexpr (TDerived::IsCreateRequest) {
            text << ", `" QUERY_COLUMN_NAME "`";
        }
        text << " FROM " QUERIES_TABLE_NAME
            " WHERE `" SCOPE_COLUMN_NAME "` = $scope"
                " AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n";
        readQueryBuilder.AddText(text);

        const auto query = readQueryBuilder.Build();
        auto [readStatus, resultSets] = this->Read(query.Sql, query.Params, this->RequestCounters, this->DebugInfo);
        this->Subscribe(readStatus, std::move(resultSets));
    }

    void Handle(TEvControlPlaneStorageInternal::TEvDbRequestResult::TPtr& ev) {
        const auto& status = ev->Get()->Status.GetValueSync();
        CPS_LOG_D(TDerived::RequestTypeName << "Request. Got response from database: " << status.GetStatus());
        if (!status.IsSuccess()) {
            ReplyWithError(status.GetIssues());
            return;
        }

        const TVector<NYdb::TResultSet>& resultSets = *ev->Get()->ResultSets;
        if (resultSets.size() != 2) {
            ReplyWithError(TStringBuilder() << "Result set size is not equal to 2 but equal to " << resultSets.size() << ". Please contact internal support");
            return;
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ReplyWithError(TStringBuilder() << "Query does not exist or permission denied. Please check the id of the query or your access rights");
                return;
            }

            ParseScope(*parser.ColumnParser(SCOPE_COLUMN_NAME).GetOptionalString());
            const TString owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != OwnerId) {
                ReplyWithError(TStringBuilder() << "Owners mismatch");
                return;
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ReplyWithError(TStringBuilder() << "Query does not exist or permission denied. Please check the id of the query or your access rights");
                return;
            }

            FederatedQuery::Internal::QueryInternal internal;
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                this->RequestCounters.Common->ParseProtobufError->Inc();
                const TString error{"Error parsing proto message for query internal. Please contact internal support"};
                CPS_LOG_E(error);
                ReplyWithError(error);
                return;
            }
            CloudId = internal.cloud_id();

            if constexpr (TDerived::IsCreateRequest) {
                FederatedQuery::Query query;
                if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                    this->RequestCounters.Common->ParseProtobufError->Inc();
                    const TString error{"Error parsing proto message for query. Please contact internal support"};
                    CPS_LOG_E(error);
                    ReplyWithError(error);
                    return;
                }
                if (i64 limit = query.content().limits().vcpu_rate_limit()) {
                    QueryLimit = limit;
                }
            }
        }

        if (!CheckPathComponent(CloudId, "Cloud id") || !CheckPathComponent(FolderId, "Folder id") || !CheckPathComponent(QueryId, "Query id")) {
            return;
        }

        static_cast<TDerived*>(this)->OnDbRequestSuccess();
    }

    bool CheckPathComponent(const TString& id, const TStringBuf& name) {
        if (id.empty()) {
            ReplyWithError(TStringBuilder() << name << " is empty");
            return false;
        }
        // If id contains forbidden symbols, they will be replaced with "_" in futher methods.
        return true;
    }

    void ParseScope(const TString& scope) {
        FolderId = NYdb::NFq::TScope(scope).ParseFolder();
    }

protected:
    const TString QueryId;
    const TString OwnerId;
    TString CloudId;
    TString FolderId;
    TMaybe<double> QueryLimit;
};

class TRateLimiterCreateRequest;
using TCreateRequestActorBase = TRateLimiterRequestActor<TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest, TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse, TRateLimiterCreateRequest>;

class TRateLimiterCreateRequest : public TCreateRequestActorBase {
public:
    using TCreateRequestActorBase::TCreateRequestActorBase;

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorageInternal::TEvDbRequestResult, Handle);
        hFunc(TEvRateLimiter::TEvCreateResourceResponse, Handle);
    )

    void OnDbRequestSuccess() {
        Send(RateLimiterControlPlaneServiceId(), new TEvRateLimiter::TEvCreateResource(CloudId, FolderId, QueryId, QueryLimit));
    }

    using TCreateRequestActorBase::Handle;

    void Handle(TEvRateLimiter::TEvCreateResourceResponse::TPtr& ev) {
        CPS_LOG_D("Got create response from rate limiter service");
        if (ev->Get()->Success) {
            Fq::Private::CreateRateLimiterResourceResult proto;
            proto.set_rate_limiter(ev->Get()->RateLimiterPath);
            Reply(proto);
        } else {
            ReplyWithError(ev->Get()->Issues);
        }
    }

    void LwProbe(bool success) {
        LWPROBE(CreateRateLimiterResourceRequest, QueryId, GetRequestDuration(), success);
    }

    static const TString RequestTypeName;
    static constexpr bool IsCreateRequest = true;
};

const TString TRateLimiterCreateRequest::RequestTypeName = "CreateRateLimiterResource";

class TRateLimiterDeleteRequest;
using TDeleteRequestActorBase = TRateLimiterRequestActor<TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest, TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse, TRateLimiterDeleteRequest>;

class TRateLimiterDeleteRequest : public TDeleteRequestActorBase {
public:
    using TDeleteRequestActorBase::TDeleteRequestActorBase;

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorageInternal::TEvDbRequestResult, Handle);
        hFunc(TEvRateLimiter::TEvDeleteResourceResponse, Handle);
    )

    void OnDbRequestSuccess() {
        Send(RateLimiterControlPlaneServiceId(), new TEvRateLimiter::TEvDeleteResource(CloudId, FolderId, QueryId));
    }

    using TDeleteRequestActorBase::Handle;

    void Handle(TEvRateLimiter::TEvDeleteResourceResponse::TPtr& ev) {
        CPS_LOG_D("Got delete response from rate limiter service");
        if (ev->Get()->Success) {
            Fq::Private::DeleteRateLimiterResourceResult proto;
            Reply(proto);
        } else {
            ReplyWithError(ev->Get()->Issues);
        }
    }

    void LwProbe(bool success) {
        LWPROBE(DeleteRateLimiterResourceRequest, QueryId, GetRequestDuration(), success);
    }

    static const TString RequestTypeName;
    static constexpr bool IsCreateRequest = false;
};

const TString TRateLimiterDeleteRequest::RequestTypeName = "DeleteRateLimiterResource";

} // namespace

template <class TEventPtr, class TRequestActor, TYdbControlPlaneStorageActor::ERequestTypeCommon requestType>
void TYdbControlPlaneStorageActor::HandleRateLimiterImpl(TEventPtr& ev) {
    TRequestCounters requestCounters{nullptr, Counters.GetCommonCounters(requestType)};

    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};

    const NActors::TActorId requestActor = Register(new TRequestActor(std::move(ev), std::move(requestCounters), std::move(debugInfo), DbPool, YdbConnection, Config));
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr& ev) {
    HandleRateLimiterImpl<TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr, TRateLimiterCreateRequest, RTC_CREATE_RATE_LIMITER_RESOURCE>(ev);
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr& ev) {
    HandleRateLimiterImpl<TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr, TRateLimiterDeleteRequest, RTC_DELETE_RATE_LIMITER_RESOURCE>(ev);
}

} // NFq
