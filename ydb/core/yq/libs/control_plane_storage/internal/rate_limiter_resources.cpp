#include "utils.h"

#include <ydb/public/lib/yq/scope.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>
#include <ydb/core/yq/libs/quota_manager/quota_manager.h>
#include <ydb/core/yq/libs/quota_manager/events/events.h>
#include <ydb/core/yq/libs/rate_limiter/control_plane_service/rate_limiter_control_plane_service.h>
#include <ydb/core/yq/libs/rate_limiter/events/events.h>

#include <util/datetime/base.h>
#include <util/string/split.h>

#include <google/protobuf/util/time_util.h>

namespace NYq {

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvDbRequestResult = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvDbRequestResult : public NActors::TEventLocal<TEvDbRequestResult, EvDbRequestResult> {
        TEvDbRequestResult(const TAsyncStatus& status, std::shared_ptr<TVector<NYdb::TResultSet>> resultSets)
            : Status(status)
            , ResultSets(std::move(resultSets))
        {
        }

        TAsyncStatus Status;
        std::shared_ptr<TVector<NYdb::TResultSet>> ResultSets;
    };
};

template <class TRequest, class TResponse, class TDerived>
class TRateLimiterRequestActor : public NActors::TActor<TDerived> {
public:
    TRateLimiterRequestActor(TInstant startTime, typename TRequest::TPtr&& ev, TRequestCountersPtr requestCounters, TDebugInfoPtr debugInfo)
        : NActors::TActor<TDerived>(&TDerived::StateFunc)
        , Request(std::move(ev))
        , RequestCounters(std::move(requestCounters))
        , DebugInfo(std::move(debugInfo))
        , QueryId(Request->Get()->Request.query_id().value())
        , OwnerId(Request->Get()->Request.owner_id())
        , StartTime(startTime)
    {
    }

    void Handle(TEvPrivate::TEvDbRequestResult::TPtr& ev) {
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

            YandexQuery::Internal::QueryInternal internal;
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ReplyWithError(TStringBuilder() << "Error parsing proto message for query internal. Please contact internal support");
                return;
            }
            CloudId = internal.cloud_id();

            if constexpr (TDerived::IsCreateRequest) {
                YandexQuery::Query query;
                if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                    ReplyWithError(TStringBuilder() << "Error parsing proto message for query. Please contact internal support");
                    return;
                }
                if (i64 limit = query.content().limits().vcpu_rate_limit()) {
                    QueryLimit = limit;
                }
            }
        }

        static_cast<TDerived*>(this)->OnDbRequestSuccess();
    }

    void ParseScope(const TString& scope) {
        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty(); // yandexcloud://{folder_id}
        if (path.size() == 2 && path.front().StartsWith(NYdb::NYq::TScope::YandexCloudScopeSchema)) {
            FolderId = path.back();
        }
    }

    void ReplyWithError(const TString& msg) {
        NYql::TIssues issues;
        issues.AddIssue(msg);
        ReplyWithError(issues);
    }

    void ReplyWithError(const NYql::TIssues& issues) {
        SendResponseEventAndPassAway(std::make_unique<TResponse>(issues), false);
    }

    void Reply(const typename TResponse::TProto& proto) {
        SendResponseEventAndPassAway(std::make_unique<TResponse>(proto), true);
    }

    void SendResponseEventAndPassAway(std::unique_ptr<TResponse> event, bool success) {
        event->DebugInfo = std::move(DebugInfo);

        RequestCounters->ResponseBytes->Add(event->GetByteSize());
        RequestCounters->InFly->Dec();
        if (success) {
            RequestCounters->Ok->Inc();
        } else {
            RequestCounters->Error->Inc();
        }
        const TDuration duration = TInstant::Now() - StartTime;
        RequestCounters->LatencyMs->Collect(duration.MilliSeconds());

        TDerived::LwProbe(QueryId, duration, success);

        this->Send(Request->Sender, event.release(), 0, Request->Cookie);

        this->PassAway();
    }

protected:
    const typename TRequest::TPtr Request;
    TRequestCountersPtr RequestCounters;
    TDebugInfoPtr DebugInfo;
    const TString QueryId;
    const TString OwnerId;
    const TInstant StartTime;
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
        hFunc(TEvPrivate::TEvDbRequestResult, Handle);
        hFunc(TEvRateLimiter::TEvCreateResourceResponse, Handle);
        hFunc(TEvQuotaService::TQuotaGetResponse, Handle);
    )

    void OnDbRequestSuccess() {
        CPS_LOG_D("Get quota value from quota service");
        Send(MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_CLOUD, CloudId, true));
    }

    void Handle(TEvQuotaService::TQuotaGetResponse::TPtr& ev) {
        CPS_LOG_D("Got response from quota service");
        if (auto quotaIt = ev->Get()->Quotas.find(QUOTA_CPU_LIMIT); quotaIt != ev->Get()->Quotas.end()) {
            CloudLimit = static_cast<double>(quotaIt->second.Limit.Value * 1000);
            Send(RateLimiterControlPlaneServiceId(), new TEvRateLimiter::TEvCreateResource(CloudId, FolderId, QueryId, CloudLimit, QueryLimit));
        } else {
            ReplyWithError("CPU quota for cloud was not found");
        }
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

    static void LwProbe(const TString& queryId, TInstant startTime, bool success) {
        LwProbe(queryId, TInstant::Now() - startTime, success);
    }

    static void LwProbe(const TString& queryId, TDuration duration, bool success) {
        LWPROBE(CreateRateLimiterResourceRequest, queryId, duration, success);
    }

    static const TString RequestTypeName;
    static constexpr bool IsCreateRequest = true;

private:
    double CloudLimit = 0.0;
};

const TString TRateLimiterCreateRequest::RequestTypeName = "CreateRateLimiterResource";

class TRateLimiterDeleteRequest;
using TDeleteRequestActorBase = TRateLimiterRequestActor<TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest, TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse, TRateLimiterDeleteRequest>;

class TRateLimiterDeleteRequest : public TDeleteRequestActorBase {
public:
    using TDeleteRequestActorBase::TDeleteRequestActorBase;

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvDbRequestResult, Handle);
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

    static void LwProbe(const TString& queryId, TInstant startTime, bool success) {
        LwProbe(queryId, TInstant::Now() - startTime, success);
    }

    static void LwProbe(const TString& queryId, TDuration duration, bool success) {
        LWPROBE(DeleteRateLimiterResourceRequest, queryId, duration, success);
    }

    static const TString RequestTypeName;
    static constexpr bool IsCreateRequest = false;
};

const TString TRateLimiterDeleteRequest::RequestTypeName = "DeleteRateLimiterResource";

} // namespace

template <class TEventPtr, class TRequestActor, TYdbControlPlaneStorageActor::ERequestTypeCommon requestType>
void TYdbControlPlaneStorageActor::HandleRateLimiterImpl(TEventPtr& ev) {
    const TInstant startTime = TInstant::Now();
    TRequestCountersPtr requestCounters = Counters.GetCommonCounters(requestType);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(ev->Get()->GetByteSize());

    auto& request = ev->Get()->Request;
    const TString& queryId = request.query_id().value();
    const TString& owner = request.owner_id();

    CPS_LOG_T(TRequestActor::RequestTypeName << "Request: {" << request.DebugString() << "}");

    NYql::TIssues issues = ValidateCreateOrDeleteRateLimiterResource(queryId, owner);
    if (issues) {
        CPS_LOG_W(TRequestActor::RequestTypeName << "Request: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        TRequestActor::LwProbe(queryId, startTime, false);
        return;
    }

    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};

    const NActors::TActorId requestActor = Register(new TRequestActor(startTime, std::move(ev), requestCounters, debugInfo));

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, TRequestActor::RequestTypeName);
    readQueryBuilder.AddString("query_id", request.query_id().value());
    TStringBuilder text;
    text << "SELECT `" SCOPE_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "` FROM " PENDING_SMALL_TABLE_NAME " WHERE `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "SELECT `" INTERNAL_COLUMN_NAME "`";
    if constexpr (TRequestActor::IsCreateRequest) {
        text << ", `" QUERY_COLUMN_NAME "`";
    }
    text << " FROM " QUERIES_TABLE_NAME " WHERE `" QUERY_ID_COLUMN_NAME "` = $query_id;\n";
    readQueryBuilder.AddText(text);

    const auto query = readQueryBuilder.Build();
    auto [readStatus, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    readStatus.Subscribe(
        [resultSets = resultSets, actorSystem = NActors::TActivationContext::ActorSystem(), requestActor] (const TAsyncStatus& status) {
            actorSystem->Send(new IEventHandle(requestActor, requestActor, new TEvPrivate::TEvDbRequestResult(status, std::move(resultSets))));
        }
    );
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr& ev) {
    HandleRateLimiterImpl<TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr, TRateLimiterCreateRequest, RTC_CREATE_RATE_LIMITER_RESOURCE>(ev);
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr& ev) {
    HandleRateLimiterImpl<TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr, TRateLimiterDeleteRequest, RTC_DELETE_RATE_LIMITER_RESOURCE>(ev);
}

} // NYq
