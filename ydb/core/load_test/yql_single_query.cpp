#include "yql_single_query.h"

#include "events.h"

#include <ydb/core/kqp/common/kqp.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr {

using namespace NActors;

class TYqlSingleQueryActor : public TActorBootstrapped<TYqlSingleQueryActor> {
public:
    TYqlSingleQueryActor(TActorId parent, TString workingDir, TString query, TDuration timeout)
        : Parent(std::move(parent))
        , WorkingDir(std::move(workingDir))
        , Query(std::move(query))
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
        DeathReport(ctx, "Query timed out");
    }

    void DeathReport(const TActorContext& ctx, TMaybe<TString> errorMessage) {
        CloseSession(ctx);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating event for table creation finish");
        auto* finishEv = new TEvLoad::TEvYqlSingleQueryResponse(std::move(errorMessage));
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
        ev->Record.MutableRequest()->SetDatabase(WorkingDir);
        ev->Record.MutableRequest()->SetSessionId(Session);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(Query);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Query is executed successfully");
            DeathReport(ctx, /* errorMessage */ {});
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
    const TDuration Timeout;

    TString Session;
};

IActor *CreateYqlSingleQueryActor(TActorId parent, TString workingDir, TString query, TDuration timeout) {
    return new TYqlSingleQueryActor(
        std::move(parent),
        std::move(workingDir),
        std::move(query),
        std::move(timeout)
    );
}

} // namespace NKikimr
