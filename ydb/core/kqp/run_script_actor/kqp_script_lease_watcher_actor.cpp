#include "kqp_run_script_actor_impl.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp::NPrivate {

namespace {

using namespace NActors;

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
        YDB_LOG_INFO_CTX(TActivationContext::AsActorContext(), "Bootstrap",
            {"logPrefix", LogPrefix()});
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
            YDB_LOG_ERROR_CTX(TActivationContext::AsActorContext(), "Lease update failed execution entry",
                {"logPrefix", LogPrefix()},
                {"sender", ev->Sender},
                {"status", status},
                {"issues", issues.ToOneLineString()},
                {"exists", executionEntryExists});
        } else {
            YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Lease updated by current execution entry",
                {"logPrefix", LogPrefix()},
                {"sender", ev->Sender},
                {"deadline", currentDeadline},
                {"exists", executionEntryExists});
        }

        if (!executionEntryExists) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, AddRootIssue("Script execution entry was lost", issues));
        }

        if (FinishInfo.IsFinished()) {
            Finish();
        } else {
            ScheduleLeaseUpdate(currentDeadline);
        }
    }

    void ScheduleLeaseUpdate(const TInstant currentDeadline) {
        LeaseUpdateScheduleTime = TInstant::Now();
        const auto leaseUpdateTime = std::max(currentDeadline - Ctx->LeaseDuration / LEASE_UPDATE_FREQUENCY, TInstant::Now() + TDuration::Seconds(1));
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Scheduling lease update",
            {"logPrefix", LogPrefix()},
            {"leaseUpdateTime", leaseUpdateTime});

        Schedule(leaseUpdateTime, new TEvents::TEvWakeup());
    }

    void StartLeaseUpdate() {
        const auto& updaterId = Register(CreateScriptLeaseUpdateActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, Ctx->LeaseDuration, Ctx->LeaseGeneration));
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Run lease updater",
            {"logPrefix", LogPrefix()},
            {"updaterId", updaterId});

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
            YDB_LOG_ERROR_CTX(TActivationContext::AsActorContext(), "Finish with error",
                {"logPrefix", LogPrefix()},
                {"status", status},
                {"issues", issues.ToOneLineString()});
        } else if (!FinishInfo.IsFailed()) {
            YDB_LOG_INFO_CTX(TActivationContext::AsActorContext(), "Finish successfully",
                {"logPrefix", LogPrefix()});
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
