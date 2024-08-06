#include "kqp_pure_compute_actor.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>

namespace NKikimr {
namespace NKqp {

bool TKqpComputeActor::IsDebugLogEnabled(const TActorSystem* actorSystem) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_TASKS_RUNNER);
}

TKqpComputeActor::TKqpComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask* task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    NWilson::TTraceId traceId, TIntrusivePtr<NActors::TProtoArenaHolder> arena,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings, TComputeActorSchedulingOptions schedulingOptions)
    : TBase(std::move(schedulingOptions), executerId, txId, task, std::move(asyncIoFactory), AppData()->FunctionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ true, /* passExceptions = */ true, /*taskCounters = */ nullptr, std::move(traceId), std::move(arena), GUCSettings)
    , ComputeCtx(settings.StatsMode)
    , FederatedQuerySetup(federatedQuerySetup)
{
    InitializeTask();
    if (GetTask().GetMeta().Is<NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta>()) {
        Meta.ConstructInPlace();
        YQL_ENSURE(GetTask().GetMeta().UnpackTo(Meta.Get()), "Invalid task meta: " << GetTask().GetMeta().DebugString());
        YQL_ENSURE(Meta->GetReads().size() == 1);
        YQL_ENSURE(!Meta->GetReads()[0].GetKeyRanges().empty());
        YQL_ENSURE(!Meta->GetTable().GetSysViewInfo().empty());
    }
}

void TKqpComputeActor::DoBootstrap() {
    const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

    TLogFunc logger;
    if (IsDebugLogEnabled(actorSystem)) {
        logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                << ", task: " << taskId << ": " << message);
        };
    }

    TDqTaskRunnerContext execCtx;

    execCtx.FuncRegistry = TBase::FunctionRegistry;
    execCtx.RandomProvider = TAppData::RandomProvider.Get();
    execCtx.TimeProvider = TAppData::TimeProvider.Get();
    execCtx.ComputeCtx = &ComputeCtx;
    execCtx.ComputationFactory = NMiniKQL::GetKqpActorComputeFactory(&ComputeCtx, FederatedQuerySetup);
    execCtx.ApplyCtx = nullptr;
    execCtx.TypeEnv = nullptr;
    execCtx.PatternCache = GetKqpResourceManager()->GetPatternCache();

    TDqTaskRunnerSettings settings;
    settings.StatsMode = RuntimeSettings.StatsMode;

    settings.OptLLVM = (GetTask().HasUseLlvm() && GetTask().GetUseLlvm()) ? "--compile-options=disable-opt" : "OFF";
    settings.UseCacheForLLVM = AppData()->FeatureFlags.GetEnableLLVMCache();

    for (const auto& [paramsName, paramsValue] : GetTask().GetTaskParams()) {
        settings.TaskParams[paramsName] = paramsValue;
    }

    for (const auto& [paramsName, paramsValue] : GetTask().GetSecureParams()) {
        settings.SecureParams[paramsName] = paramsValue;
    }

    for (const auto& readRange : GetTask().GetReadRanges()) {
        settings.ReadRanges.push_back(readRange);
    }

    auto taskRunner = MakeDqTaskRunner(TBase::GetAllocatorPtr(), execCtx, settings, logger);
    SetTaskRunner(taskRunner);

    auto wakeup = [this]{ ContinueExecute(); };
    try {
        PrepareTaskRunner(TKqpTaskRunnerExecutionContext(std::get<ui64>(TxId), RuntimeSettings.UseSpilling, std::move(wakeup)));
    } catch (const NMiniKQL::TKqpEnsureFail& e) {
        InternalError((TIssuesIds::EIssueCode) e.GetCode(), e.GetMessage());
        return;
    }

    TSmallVec<NMiniKQL::TKqpScanComputeContext::TColumn> columns;

    TVector<TSerializedTableRange> ranges;
    if (Meta) {
        YQL_ENSURE(ComputeCtx.GetTableScans().empty());

        ComputeCtx.AddTableScan(0, *Meta, GetStatsMode());
        ScanData = &ComputeCtx.GetTableScan(0);

        columns.reserve(Meta->ColumnsSize());
        for (const auto& column : ScanData->GetColumns()) {
            NMiniKQL::TKqpScanComputeContext::TColumn c;
            c.Tag = column.Tag;
            c.Type = column.Type;
            columns.emplace_back(std::move(c));
        }

        const auto& protoRanges = Meta->GetReads()[0].GetKeyRanges();
        for (auto& range : protoRanges) {
            ranges.emplace_back(range);
        }
    }

    if (ScanData) {
        ScanData->TaskId = GetTask().GetId();
        ScanData->TableReader = CreateKqpTableReader(*ScanData);

        auto scanActor = NSysView::CreateSystemViewScan(SelfId(), 0, ScanData->TableId, ranges, columns);

        if (!scanActor) {
            InternalError(TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                << "Failed to create system view scan, table id: " << ScanData->TableId);
            return;
        }

        SysViewActorId = Register(scanActor.Release());
        Send(SysViewActorId, new TEvKqpCompute::TEvScanDataAck(MemoryLimits.ChannelBufferSize));
    }

    ContinueExecute();
    Become(&TKqpComputeActor::StateFunc);

    TBase::DoBoostrap();
}

STFUNC(TKqpComputeActor::StateFunc) {
    CA_LOG_D("CA StateFunc " << ev->GetTypeRewrite());
    try {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpCompute::TEvScanInitActor, HandleExecute);
            hFunc(TEvKqpCompute::TEvScanData, HandleExecute);
            hFunc(TEvKqpCompute::TEvScanError, HandleExecute);
            default:
                BaseStateFuncBody(ev);
        }
    } catch (const TMemoryLimitExceededException& e) {
        TBase::OnMemoryLimitExceptionHandler();
    } catch (const NMiniKQL::TKqpEnsureFail& e) {
        InternalError((TIssuesIds::EIssueCode) e.GetCode(), e.GetMessage());
    } catch (const yexception& e) {
        InternalError(TIssuesIds::DEFAULT_ERROR, e.what());
    }

    ReportEventElapsedTime();
}

ui64 TKqpComputeActor::CalcMkqlMemoryLimit() {
    return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ChannelBufferSize;
}

void TKqpComputeActor::CheckRunStatus() {
    ProcessOutputsState.LastPopReturnedNoData = !ProcessOutputsState.DataWasSent;
    TBase::CheckRunStatus();
}

void TKqpComputeActor::FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last) {
    if (last && SysViewActorId && ScanData && dst->TasksSize() > 0) {
        YQL_ENSURE(dst->TasksSize() == 1);

        auto* taskStats = dst->MutableTasks(0);
        auto* tableStats = taskStats->AddTables();

        tableStats->SetTablePath(ScanData->TablePath);

        if (auto* x = ScanData->BasicStats.get()) {
            tableStats->SetReadRows(x->Rows);
            tableStats->SetReadBytes(x->Bytes);
            // TODO: CpuTime
        }

        if (auto* x = ScanData->ProfileStats.get()) {
            // save your profile stats here
        }
    }
}

void TKqpComputeActor::PassAway() {
    if (SysViewActorId) {
        Send(SysViewActorId, new TEvents::TEvPoison);
    }

    if (TaskRunner) {
        if (TaskRunner->IsAllocatorAttached()) {
            ComputeCtx.Clear();
        } else {
            auto guard = TaskRunner->BindAllocator(GetMkqlMemoryLimit());
            ComputeCtx.Clear();
        }
    }

    TBase::PassAway();
}

void TKqpComputeActor::HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    Y_DEBUG_ABORT_UNLESS(ScanData);

    auto& msg = ev->Get()->Record;

    Y_DEBUG_ABORT_UNLESS(SysViewActorId == ActorIdFromProto(msg.GetScanActorId()));

    CA_LOG_D("Got sysview scan initial event, scan actor: " << SysViewActorId << ", scanId: 0");
    Send(ev->Sender, new TEvKqpCompute::TEvScanDataAck(GetMemoryLimits().ChannelBufferSize));
    return;
}

void TKqpComputeActor::HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
    Y_DEBUG_ABORT_UNLESS(ScanData);
    Y_DEBUG_ABORT_UNLESS(SysViewActorId == ev->Sender);

    auto& msg = *ev->Get();

    ui64 bytes = 0;
    ui64 rowsCount = 0;
    {
        auto guard = TaskRunner->BindAllocator();
        switch (msg.GetDataFormat()) {
            case NKikimrDataEvents::FORMAT_UNSPECIFIED:
            case NKikimrDataEvents::FORMAT_CELLVEC: {
                if (!msg.Rows.empty()) {
                    bytes = ScanData->AddData(msg.Rows, {}, TaskRunner->GetHolderFactory());
                    rowsCount = msg.Rows.size();
                }
                break;
            }
            case NKikimrDataEvents::FORMAT_ARROW: {
                if(msg.ArrowBatch != nullptr) {
                    bytes = ScanData->AddData(NMiniKQL::TBatchDataAccessor(msg.ArrowBatch), {}, TaskRunner->GetHolderFactory());
                    rowsCount = msg.ArrowBatch->num_rows();
                }
                break;
            }
        }
    }

    CA_LOG_D("Got sysview scandata, rows: " << rowsCount << ", bytes: " << bytes
        << ", finished: " << msg.Finished << ", from: " << SysViewActorId);

    if (msg.Finished) {
        CA_LOG_D("Finishing rows buffer");
        ScanData->Finish();
    }

    if (Y_UNLIKELY(ScanData->ProfileStats)) {
        ScanData->ProfileStats->Messages++;
        ScanData->ProfileStats->ScanCpuTime += msg.CpuTime; // TODO: not implemented yet
        ScanData->ProfileStats->ScanWaitTime += msg.WaitTime;
        if (msg.PageFault) {
            ScanData->ProfileStats->PageFaults += msg.PageFaults;
            ScanData->ProfileStats->MessagesByPageFault++;
        }
    }

    ui64 freeSpace = GetMemoryLimits().ChannelBufferSize > ScanData->GetStoredBytes()
        ? GetMemoryLimits().ChannelBufferSize - ScanData->GetStoredBytes()
        : 0;

    if (freeSpace > 0) {
        CA_LOG_D("Send scan data ack, freeSpace: " << freeSpace);

        Send(SysViewActorId, new TEvKqpCompute::TEvScanDataAck(freeSpace));
    }

    DoExecute();
}

void TKqpComputeActor::HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
    Y_DEBUG_ABORT_UNLESS(ScanData);

    TIssues issues;
    Ydb::StatusIds::StatusCode status = ev->Get()->Record.GetStatus();
    IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);

    State = NDqProto::COMPUTE_STATE_FAILURE;
    ReportStateAndMaybeDie(YdbStatusToDqStatus(status), issues);
}

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask* task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    NWilson::TTraceId traceId, TIntrusivePtr<NActors::TProtoArenaHolder> arena,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
    const TGUCSettings::TPtr& GUCSettings, TComputeActorSchedulingOptions cpuOptions)
{
    return new TKqpComputeActor(executerId, txId, task, std::move(asyncIoFactory),
        settings, memoryLimits, std::move(traceId), std::move(arena), federatedQuerySetup, GUCSettings, std::move(cpuOptions));
}

} // namespace NKqp
} // namespace NKikimr
