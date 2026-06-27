#include "kqp_run_script_actor_impl.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <exception>

namespace NKikimr::NKqp::NPrivate {

namespace {

using namespace NActors;

#define LOG_T(stream) LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_D(stream) LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_I(stream) LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_N(stream) LOG_NOTICE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_W(stream) LOG_WARN_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_E(stream) LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);

class TScriptLeaseWatcherActor final : public TActorBootstrapped<TScriptLeaseWatcherActor>, IActorExceptionHandler {
    using TBase = TActorBootstrapped<TScriptLeaseWatcherActor>;

    static constexpr ui32 LEASE_UPDATE_FREQUENCY = 2;

public:
    static constexpr char ActorName[] = "KQP_SCRIPT_LEASE_WATCHER_ACTOR";

    explicit TScriptLeaseWatcherActor(TScriptExecutionContext::TPtr ctx)
        : Ctx(std::move(ctx))
    {
        Y_VALIDATE(Ctx && Ctx->UserRequestContext, "Missing script execution context");
    }

    void Bootstrap() {
        LOG_I("Bootstrap");
        Become(&TThis::StateFunc);
        ScheduleLeaseUpdate(TInstant::Now() + Ctx->LeaseDuration);
    }

private:
    void Registered(TActorSystem* sys, const TActorId& owner) final {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    bool OnUnhandledException(const std::exception& e) final {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << e.what());
        return true;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
        sFunc(TEvents::TEvWakeup, StartLeaseUpdate);
        sFunc(TEvents::TEvPoison, Finish);
    )

    void Handle(const TEvScriptLeaseUpdateResponse::TPtr& ev) {
        if (const auto& counters = Ctx->Counters) {
            counters->ReportLeaseUpdateLatency(TInstant::Now() - LeaseUpdateStartTime);
        }

        LeaseUpdateStartTime = TInstant::Zero();

        const auto& issues = ev->Get()->Issues;
        const auto executionEntryExists = ev->Get()->ExecutionEntryExists;
        const auto currentDeadline = ev->Get()->CurrentDeadline;
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Lease update " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString() << ", execution entry exists: " << executionEntryExists);
        } else {
            LOG_D("Lease updated by " << ev->Sender << ", current deadline: " << currentDeadline << ", execution entry exists: " << executionEntryExists);
        }

        if (!executionEntryExists) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script execution entry was lost");
        }

        if (FinishInfo.IsFinished()) {
            Finish();
        } else {
            ScheduleLeaseUpdate(currentDeadline);
        }
    }

    void ScheduleLeaseUpdate(const TInstant currentDeadline) {
        LeaseUpdateScheduleTime = TInstant::Now();
        const auto leaseUpdateTime = currentDeadline - Ctx->LeaseDuration / LEASE_UPDATE_FREQUENCY;
        LOG_D("Scheduling lease update on " << leaseUpdateTime);

        if (TInstant::Now() >= leaseUpdateTime) {
            StartLeaseUpdate();
        } else {
            Schedule(leaseUpdateTime, new TEvents::TEvWakeup());
        }
    }

    void StartLeaseUpdate() {
        const auto& updaterId = Register(CreateScriptLeaseUpdateActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, Ctx->LeaseDuration, Ctx->LeaseGeneration));
        LOG_D("Run lease updater " << updaterId);

        LeaseUpdateStartTime = TInstant::Now();

        if (const auto& counters = Ctx->Counters) {
            counters->ReportRunActorLeaseUpdateBacklog(TInstant::Now() - LeaseUpdateScheduleTime);
        }
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void Finish(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Finish(status, {NYql::TIssue(message)});
    }

    void Finish(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Finish with error " << status << ", issues: " << issues.ToOneLineString());
        } else if (!FinishInfo.IsFailed()) {
            LOG_I("Finish successfully");
        }

        FinishInfo.Update(status, std::move(issues));

        if (!LeaseUpdateStartTime) {
            Send(Owner, new TEvRunScriptPrivate::TEvScriptLeaseWatcherFinished(*FinishInfo.Status, std::move(FinishInfo.Issues)));
            PassAway();
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] " << SelfId() << ". Owner: " << Owner << ". Ctx: " << *Ctx->UserRequestContext << ". LeaseGeneration: " << Ctx->LeaseGeneration << ". ";
    }

    const TScriptExecutionContext::TPtr Ctx;
    TActorId Owner;
    TFinishInfo FinishInfo;
    TInstant LeaseUpdateScheduleTime;
    TInstant LeaseUpdateStartTime;
};

} // anonymous namespace

IActor* CreateScriptLeaseWatcherActor(TScriptExecutionContext::TPtr ctx) {
    return new TScriptLeaseWatcherActor(std::move(ctx));
}

} // namespace NKikimr::NKqp::NPrivate
