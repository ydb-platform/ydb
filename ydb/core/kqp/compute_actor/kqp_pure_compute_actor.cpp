#include "kqp_compute_actor.h"
#include "kqp_compute_actor_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/core/util/yverify_stream.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>


namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql;
using namespace NYql::NDq;

inline bool IsDebugLogEnabled(const TActorSystem* actorSystem) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_TASKS_RUNNER);
}

class TKqpComputeActor : public TDqComputeActorBase<TKqpComputeActor> {
    using TBase = TDqComputeActorBase<TKqpComputeActor>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPUTE_ACTOR;
    }

    TKqpComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask&& task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        NWilson::TTraceId traceId)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ true, /* passExceptions = */ true, /*taskCounters = */ nullptr, std::move(traceId))
        , ComputeCtx(settings.StatsMode)
    {
        if (GetTask().GetMeta().Is<NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta>()) {
            Meta.ConstructInPlace();
            YQL_ENSURE(GetTask().GetMeta().UnpackTo(Meta.Get()), "Invalid task meta: " << GetTask().GetMeta().DebugString());
            YQL_ENSURE(Meta->GetReads().size() == 1);
            YQL_ENSURE(!Meta->GetReads()[0].GetKeyRanges().empty());
            YQL_ENSURE(!Meta->GetTable().GetSysViewInfo().empty());
        }
    }

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        TDqTaskRunnerContext execCtx;

        execCtx.FuncRegistry = AppData()->FunctionRegistry;
        execCtx.RandomProvider = TAppData::RandomProvider.Get();
        execCtx.TimeProvider = TAppData::TimeProvider.Get();
        execCtx.ComputeCtx = &ComputeCtx;
        execCtx.ComputationFactory = NMiniKQL::GetKqpActorComputeFactory(&ComputeCtx);
        execCtx.ApplyCtx = nullptr;
        execCtx.Alloc = nullptr;
        execCtx.TypeEnv = nullptr;
        execCtx.PatternCache = GetKqpResourceManager()->GetPatternCache();

        TDqTaskRunnerSettings settings;
        settings.CollectBasicStats = RuntimeSettings.StatsMode >= NYql::NDqProto::DQ_STATS_MODE_BASIC;
        settings.CollectProfileStats = RuntimeSettings.StatsMode >= NYql::NDqProto::DQ_STATS_MODE_PROFILE;

        settings.OptLLVM = (GetTask().HasUseLlvm() && GetTask().GetUseLlvm()) ? "--compile-options=disable-opt" : "OFF";
        settings.UseCacheForLLVM = AppData()->FeatureFlags.GetEnableLLVMCache();

        for (const auto& [paramsName, paramsValue] : GetTask().GetTaskParams()) {
            settings.TaskParams[paramsName] = paramsValue;
        }

        for (const auto& [paramsName, paramsValue] : GetTask().GetSecureParams()) {
            settings.SecureParams[paramsName] = paramsValue;
        }

        auto taskRunner = MakeDqTaskRunner(execCtx, settings, logger);
        SetTaskRunner(taskRunner);

        auto wakeup = [this]{ ContinueExecute(); };
        try {
            PrepareTaskRunner(TKqpTaskRunnerExecutionContext(std::get<ui64>(TxId), RuntimeSettings.UseSpilling,
                std::move(wakeup), TlsActivationContext->AsActorContext()));
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
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanError, HandleExecute);
                default:
                    BaseStateFuncBody(ev);
            }
        } catch (const TMemoryLimitExceededException& e) {
            InternalError(TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName()
                << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const NMiniKQL::TKqpEnsureFail& e) {
            InternalError((TIssuesIds::EIssueCode) e.GetCode(), e.GetMessage());
        } catch (const yexception& e) {
            InternalError(TIssuesIds::DEFAULT_ERROR, e.what());
        }

        ReportEventElapsedTime();
    }

protected:
    ui64 CalcMkqlMemoryLimit() override {
        return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ChannelBufferSize;
    }

public:
    void FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last) {
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

private:
    void PassAway() override {
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

private:
    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);

        auto& msg = ev->Get()->Record;

        Y_VERIFY_DEBUG(SysViewActorId == ActorIdFromProto(msg.GetScanActorId()));

        CA_LOG_D("Got sysview scan initial event, scan actor: " << SysViewActorId << ", scanId: 0");
        Send(ev->Sender, new TEvKqpCompute::TEvScanDataAck(GetMemoryLimits().ChannelBufferSize));
        return;
    }

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(SysViewActorId == ev->Sender);

        auto& msg = *ev->Get();

        ui64 bytes = 0;
        ui64 rowsCount = 0;
        {
            auto guard = TaskRunner->BindAllocator();
            switch (msg.GetDataFormat()) {
                case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED:
                case NKikimrTxDataShard::EScanDataFormat::CELLVEC: {
                    if (!msg.Rows.empty()) {
                        bytes = ScanData->AddData(msg.Rows, {}, TaskRunner->GetHolderFactory());
                        rowsCount = msg.Rows.size();
                    }
                    break;
                }
                case NKikimrTxDataShard::EScanDataFormat::ARROW: {
                    if(msg.ArrowBatch != nullptr) {
                        bytes = ScanData->AddData(*msg.ArrowBatch, {}, TaskRunner->GetHolderFactory());
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

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);

        TIssues issues;
        Ydb::StatusIds::StatusCode status = ev->Get()->Record.GetStatus();
        IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);

        State = NDqProto::COMPUTE_STATE_FAILURE;
        ReportStateAndMaybeDie(YdbStatusToDqStatus(status), issues);
    }

private:
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    TMaybe<NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta> Meta;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    TActorId SysViewActorId;
};

} // anonymous namespace

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask&& task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    NWilson::TTraceId traceId)
{
    return new TKqpComputeActor(executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, std::move(traceId));
}

} // namespace NKqp
} // namespace NKikimr
