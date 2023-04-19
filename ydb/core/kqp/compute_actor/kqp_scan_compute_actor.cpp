#include "kqp_scan_compute_actor.h"
#include "kqp_scan_common.h"
#include "kqp_compute_actor_impl.h"
#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/kqp/common/kqp_resolve.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

namespace NKikimr::NKqp::NScanPrivate {

namespace {

using namespace NYql;
using namespace NYql::NDq;

static constexpr TDuration RL_MAX_BATCH_DELAY = TDuration::Seconds(50);

} // anonymous namespace

TKqpScanComputeActor::TKqpScanComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask&& task,
    IDqAsyncIoFactory::TPtr asyncIoFactory, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId)
    : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings,
        memoryLimits, /* ownMemoryQuota = */ true, /* passExceptions = */ true, /*taskCounters = */ nullptr, std::move(traceId))
    , ComputeCtx(settings.StatsMode)
{
    YQL_ENSURE(GetTask().GetMeta().UnpackTo(&Meta), "Invalid task meta: " << GetTask().GetMeta().DebugString());
    YQL_ENSURE(!Meta.GetReads().empty());
    YQL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);
}

void TKqpScanComputeActor::ProcessRlNoResourceAndDie() {
    const NYql::TIssue issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_RESOURCE_USAGE_LIMITED,
        "Throughput limit exceeded for query");
    CA_LOG_E("Throughput limit exceeded stream will be terminated");

    State = NDqProto::COMPUTE_STATE_FAILURE;
    ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::OVERLOADED, TIssues({ issue }));
}

bool TKqpScanComputeActor::IsQuotingEnabled() const {
    const auto& rlPath = RuntimeSettings.RlPath;
    return rlPath.Defined();
}

void TKqpScanComputeActor::AcquireRateQuota() {
    const auto& rlPath = RuntimeSettings.RlPath;
    auto selfId = this->SelfId();
    auto as = TActivationContext::ActorSystem();

    auto onSendAllowed = [selfId, as]() mutable {
        as->Send(selfId, new TEvents::TEvWakeup(EEvWakeupTag::RlSendAllowedTag));
    };

    auto onSendTimeout = [selfId, as]() {
        as->Send(selfId, new TEvents::TEvWakeup(EEvWakeupTag::RlNoResourceTag));
    };

    const NRpcService::TRlFullPath rlFullPath{
        .CoordinationNode = rlPath->GetCoordinationNode(),
        .ResourcePath = rlPath->GetResourcePath(),
        .DatabaseName = rlPath->GetDatabase(),
        .Token = rlPath->GetToken()
    };

    auto rlActor = NRpcService::RateLimiterAcquireUseSameMailbox(
        rlFullPath, 0, RL_MAX_BATCH_DELAY,
        std::move(onSendAllowed), std::move(onSendTimeout), TActivationContext::AsActorContext());

    CA_LOG_D("Launch rate limiter actor: " << rlActor);
}

void TKqpScanComputeActor::FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last) {
    if (last && ScanData && dst->TasksSize() > 0) {
        YQL_ENSURE(dst->TasksSize() == 1);

        auto* taskStats = dst->MutableTasks(0);
        auto* tableStats = taskStats->AddTables();

        tableStats->SetTablePath(ScanData->TablePath);

        if (auto* x = ScanData->BasicStats.get()) {
            tableStats->SetReadRows(x->Rows);
            tableStats->SetReadBytes(x->Bytes);
            tableStats->SetAffectedPartitions(x->AffectedShards);
            // TODO: CpuTime
        }

        if (auto* x = ScanData->ProfileStats.get()) {
            NKqpProto::TKqpTaskExtraStats taskExtraStats;
            //                auto scanTaskExtraStats = taskExtraStats.MutableScanTaskExtraStats();
            //                scanTaskExtraStats->SetRetriesCount(TotalRetries);
            taskStats->MutableExtra()->PackFrom(taskExtraStats);
        }
    }
}

void TKqpScanComputeActor::HandleEvWakeup(EEvWakeupTag tag) {
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "HandleEvWakeup")("self_id", SelfId());
    switch (tag) {
        case RlSendAllowedTag:
            DoExecute();
            break;
        case RlNoResourceTag:
            ProcessRlNoResourceAndDie();
            break;
        case TimeoutTag:
            Y_FAIL("TimeoutTag must be handled in base class");
            break;
        case PeriodicStatsTag:
            Y_FAIL("PeriodicStatsTag must be handled in base class");
            break;
    }
}

void TKqpScanComputeActor::Handle(TEvScanExchange::TEvTerminateFromFetcher::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvTerminateFromFetcher: " << ev->Sender << "/" << SelfId();
    TBase::InternalError(ev->Get()->GetStatusCode(), ev->Get()->GetIssues());
    State = ev->Get()->GetState();
}

void TKqpScanComputeActor::Handle(TEvScanExchange::TEvSendData::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvSendData: " << ev->Sender << "/" << SelfId();
    auto& msg = *ev->Get();
    auto guard = TaskRunner->BindAllocator();
    if (!!msg.GetArrowBatch()) {
        ScanData->AddData(*msg.GetArrowBatch(), msg.GetTabletId(), TaskRunner->GetHolderFactory());
    } else {
        ScanData->AddData(std::move(msg.MutableRows()), msg.GetTabletId(), TaskRunner->GetHolderFactory());
    }
    if (IsQuotingEnabled()) {
        AcquireRateQuota();
    } else {
        DoExecute();
    }
}

void TKqpScanComputeActor::Handle(TEvScanExchange::TEvRegisterFetcher::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvRegisterFetcher: " << ev->Sender;
    Y_VERIFY(Fetchers.emplace(ev->Sender).second);
    Send(ev->Sender, new TEvScanExchange::TEvAckData(CalculateFreeSpace()));
}

void TKqpScanComputeActor::Handle(TEvScanExchange::TEvFetcherFinished::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvFetcherFinished: " << ev->Sender;
    Y_VERIFY(Fetchers.erase(ev->Sender) == 1);
    if (Fetchers.size() == 0) {
        ScanData->Finish();
        DoExecute();
    }
}

void TKqpScanComputeActor::PollSources(std::any prev) {
    if (!ScanData || ScanData->IsFinished()) {
        return;
    }
    const auto hasNewMemoryPred = [&]() {
        if (!prev.has_value()) {
            return false;
        }
        const ui64 freeSpace = CalculateFreeSpace();
        const ui64 prevFreeSpace = std::any_cast<ui64>(prev);
        return freeSpace > prevFreeSpace;
    };
    if (!hasNewMemoryPred() && ScanData->GetStoredBytes()) {
        return;
    }
    const ui32 freeSpace = CalculateFreeSpace();
    CA_LOG_D("POLL_SOURCES:START:" << Fetchers.size() << ";fs=" << freeSpace);
    for (auto&& i : Fetchers) {
        Send(i, new TEvScanExchange::TEvAckData(freeSpace));
    }
    CA_LOG_D("POLL_SOURCES:FINISH");
}

void TKqpScanComputeActor::DoBootstrap() {
    CA_LOG_D("EVLOGKQP START");
    NDq::TDqTaskRunnerContext execCtx;
    execCtx.FuncRegistry = AppData()->FunctionRegistry;
    execCtx.ComputeCtx = &ComputeCtx;
    execCtx.ComputationFactory = NMiniKQL::GetKqpActorComputeFactory(&ComputeCtx);
    execCtx.RandomProvider = TAppData::RandomProvider.Get();
    execCtx.TimeProvider = TAppData::TimeProvider.Get();
    execCtx.ApplyCtx = nullptr;
    execCtx.Alloc = nullptr;
    execCtx.TypeEnv = nullptr;
    execCtx.PatternCache = GetKqpResourceManager()->GetPatternCache();

    const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

    NDq::TDqTaskRunnerSettings settings;
    settings.CollectBasicStats = GetStatsMode() >= NYql::NDqProto::DQ_STATS_MODE_BASIC;
    settings.CollectProfileStats = GetStatsMode() >= NYql::NDqProto::DQ_STATS_MODE_PROFILE;
    settings.OptLLVM = TBase::GetUseLLVM() ? "--compile-options=disable-opt" : "OFF";
    settings.UseCacheForLLVM = AppData()->FeatureFlags.GetEnableLLVMCache();
    settings.AllowGeneratorsInUnboxedValues = false;

    for (const auto& [paramsName, paramsValue] : GetTask().GetTaskParams()) {
        settings.TaskParams[paramsName] = paramsValue;
    }

    for (const auto& [paramsName, paramsValue] : GetTask().GetSecureParams()) {
        settings.SecureParams[paramsName] = paramsValue;
    }

    NDq::TLogFunc logger;
    if (IsDebugLogEnabled(actorSystem, NKikimrServices::KQP_TASKS_RUNNER)) {
        logger = [actorSystem, txId = TxId, taskId = GetTask().GetId()](const TString& message) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                << ", task: " << taskId << ": " << message);
        };
    }

    auto taskRunner = CreateKqpTaskRunner(execCtx, settings, logger);
    TBase::SetTaskRunner(taskRunner);

    auto wakeup = [this] { ContinueExecute(); };
    TBase::PrepareTaskRunner(TKqpTaskRunnerExecutionContext(std::get<ui64>(TxId), RuntimeSettings.UseSpilling, std::move(wakeup),
        TlsActivationContext->AsActorContext()));

    ComputeCtx.AddTableScan(0, Meta, GetStatsMode());
    ScanData = &ComputeCtx.GetTableScan(0);

    ScanData->TaskId = GetTask().GetId();
    ScanData->TableReader = CreateKqpTableReader(*ScanData);
    Become(&TKqpScanComputeActor::StateFunc);
}

}
