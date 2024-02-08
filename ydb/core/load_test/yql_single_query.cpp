#include "yql_single_query.h"

#include "events.h"

#include <ydb/core/kqp/common/kqp.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {

using namespace NActors;

class TYqlSingleQueryActor : public TActorBootstrapped<TYqlSingleQueryActor> {
public:
    TYqlSingleQueryActor(TActorId parent, TString workingDir, TString query, NKikimrKqp::EQueryType queryType, bool readOnly, TString result, TDuration timeout)
        : Parent(std::move(parent))
        , WorkingDir(std::move(workingDir))
        , Query(std::move(query))
        , QueryType(queryType)
        , ReadOnly(readOnly)
        , Result(std::move(result))
        , Timeout(std::move(timeout))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TYqlSingleQueryActor::StateMain);

        CreateSession(ctx);

        ctx.Schedule(Timeout, new TEvents::TEvPoisonPill);
    }

    STRICT_STFUNC(StateMain,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse)
    )

private:
    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_CRIT_S(ctx, NKikimrServices::KQP_LOAD_TEST, "HandlePoisonPill");
        ReportError(ctx, "Query timed out");
    }

    void ReportResult(const TActorContext& ctx, NKikimrKqp::TQueryResponse response, TMaybe<TString> errorMessage) {
        CloseSession(ctx);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST,
            "Creating event for query response " << (errorMessage.Defined() ? "with error" : "with success"));
        auto* finishEv = new TEvLoad::TEvYqlSingleQueryResponse(
            Result,
            std::move(errorMessage),
            std::move(response)
        );
        ctx.Send(Parent, finishEv);

        PassAway();
    }

    void ReportError(const TActorContext& ctx, TString errorMessage) {
        CloseSession(ctx);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating event for table creation finish");
        auto* finishEv = new TEvLoad::TEvYqlSingleQueryResponse(
            "",
            std::move(errorMessage),
            Nothing()
        );
        ctx.Send(Parent, finishEv);

        PassAway();
    }

    void CreateSession(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating event for session creation");
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.MutableRequest()->SetDatabase(WorkingDir);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        Send(kqp_proxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Session is created: " + Session);
            ExecuteQuery(ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Session creation failed: " + ev->Get()->ToString());
        }
    }

    void ExecuteQuery(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating event for query execution");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();
        request->SetDatabase(WorkingDir);
        request->SetSessionId(Session);
        request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->SetType(QueryType);
        request->SetQuery(Query);

        if (QueryType == NKikimrKqp::QUERY_TYPE_SQL_DML) {
            if (ReadOnly) {
                request->MutableTxControl()->mutable_begin_tx()->mutable_stale_read_only();
            } else {
                request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            }
            request->MutableTxControl()->set_commit_tx(true);
        } else {
            Y_ENSURE(!ReadOnly, "Flag ReadOnly is not applicable for this query type");
        }

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Query is executed successfully");
            ReportResult(ctx, response.GetResponse(), Nothing());
        } else if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SCHEME_ERROR) {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Abort after query execution failed with scheme error: " + ev->Get()->ToString());
            ReportResult(ctx, response.GetResponse(), ev->Get()->ToString());
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Query execution failed: " + ev->Get()->ToString());
            ExecuteQuery(ctx);
        }
    }

    void CloseSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating event for session closing");

        if (!Session.empty()) {
            auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(Session);

            auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

            ctx.Send(kqp_proxy, ev.Release());
            Session.clear();
        }
    }

private:
    const TActorId Parent;
    const TString WorkingDir;
    const TString Query;
    const NKikimrKqp::EQueryType QueryType;
    const bool ReadOnly;
    const TString Result;
    const TDuration Timeout;

    TString Session;
};

IActor *CreateYqlSingleQueryActor(
    TActorId parent,
    TString workingDir,
    TString query,
    NKikimrKqp::EQueryType queryType,
    bool readOnly,
    TString result,
    TDuration timeout
) {
    return new TYqlSingleQueryActor(
        std::move(parent),
        std::move(workingDir),
        std::move(query),
        queryType,
        readOnly,
        std::move(result),
        std::move(timeout)
    );
}

} // namespace NKikimr
