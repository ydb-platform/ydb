#include "kqp_compute_actor.h"
#include "kqp_compute_actor_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/runtime/kqp_channel_storage.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/core/tx/datashard/datashard_kqp_compute.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>

#include <util/generic/deque.h>

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr::NKqp::NComputeActor;

bool IsDebugLogEnabled(const TActorSystem* actorSystem, NActors::NLog::EComponent component) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, component);
}

static constexpr ui64 MAX_SHARD_RETRIES = 5; // retry after: 0, 250, 500, 1000, 2000
static constexpr ui64 MAX_TOTAL_SHARD_RETRIES = 20;
static constexpr ui64 MAX_SHARD_RESOLVES = 3;
static constexpr TDuration RL_MAX_BATCH_DELAY = TDuration::Seconds(50);


struct TScannedDataStats {
    std::map<ui64, std::pair<ui64, ui64>> ReadShardInfo;
    ui64 CompletedShards = 0;
    ui64 TotalReadRows = 0;
    ui64 TotalReadBytes = 0;

    TScannedDataStats()
    {}

    void AddReadStat(ui64 tabletId, ui64 rows, ui64 bytes) {
        auto [it, success] = ReadShardInfo.emplace(tabletId, std::make_pair(rows, bytes));
        if (!success) {
            auto& [currentRows, currentBytes] = it->second;
            currentRows += rows;
            currentBytes += bytes;
        }
    }

    void CompleteShard(ui64 tabletId) {
        auto it = ReadShardInfo.find(tabletId);
        YQL_ENSURE(it != ReadShardInfo.end());
        auto& [currentRows, currentBytes] = it->second;
        TotalReadRows += currentRows;
        TotalReadBytes += currentBytes;
        ++CompletedShards;
        ReadShardInfo.erase(it);
    }

    ui64 AverageReadBytes() const {
        return (CompletedShards == 0) ? 0 : TotalReadBytes / CompletedShards;
    }

    ui64 AverageReadRows() const {
        return (CompletedShards == 0) ? 0 : TotalReadRows / CompletedShards;
    }
};


class TKqpScanComputeActor : public TDqComputeActorBase<TKqpScanComputeActor> {
    using TBase = TDqComputeActorBase<TKqpScanComputeActor>;

    struct TEvPrivate {
        enum EEv {
            EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryShard : public TEventLocal<TEvRetryShard, EvRetryShard> {
            ui64 TabletId;

            TEvRetryShard(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_COMPUTE_ACTOR;
    }

    TKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
        NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, TIntrusivePtr<TKqpCounters> counters, 
        NWilson::TTraceId traceId)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ true, /* passExceptions = */ true, /*taskCounters = */ nullptr, std::move(traceId))
        , ComputeCtx(settings.StatsMode)
        , Snapshot(snapshot)
        , Counters(counters)
    {
        YQL_ENSURE(GetTask().GetMeta().UnpackTo(&Meta), "Invalid task meta: " << GetTask().GetMeta().DebugString());
        YQL_ENSURE(!Meta.GetReads().empty());
        YQL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);

        KeyColumnTypes.assign(Meta.GetKeyColumnTypes().begin(), Meta.GetKeyColumnTypes().end());
    }

    void DoBootstrap() {
        NDq::TDqTaskRunnerContext execCtx;
        execCtx.FuncRegistry = AppData()->FunctionRegistry;
        execCtx.ComputeCtx = &ComputeCtx;
        execCtx.ComputationFactory = GetKqpActorComputeFactory(&ComputeCtx);
        execCtx.RandomProvider = TAppData::RandomProvider.Get();
        execCtx.TimeProvider = TAppData::TimeProvider.Get();
        execCtx.ApplyCtx = nullptr;
        execCtx.Alloc = nullptr;
        execCtx.TypeEnv = nullptr;

        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        NDq::TDqTaskRunnerSettings settings;
        settings.CollectBasicStats = GetStatsMode() >= NYql::NDqProto::DQ_STATS_MODE_BASIC;
        settings.CollectProfileStats = GetStatsMode() >= NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        settings.OptLLVM = GetUseLLVM() ? "--compile-options=disable-opt" : "OFF";
        settings.AllowGeneratorsInUnboxedValues = false;

        NDq::TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem, NKikimrServices::KQP_TASKS_RUNNER)) {
            logger = [actorSystem, txId = GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        auto taskRunner = CreateKqpTaskRunner(execCtx, settings, logger);
        SetTaskRunner(taskRunner);

        auto wakeup = [this]{ ContinueExecute(); };
        PrepareTaskRunner(TKqpTaskRunnerExecutionContext(std::get<ui64>(TxId), RuntimeSettings.UseSpilling, std::move(wakeup),
                                                         TlsActivationContext->AsActorContext()));

        ComputeCtx.AddTableScan(0, Meta, GetStatsMode());
        ScanData = &ComputeCtx.GetTableScan(0);

        ScanData->TaskId = GetTask().GetId();
        ScanData->TableReader = CreateKqpTableReader(*ScanData);

        for (const auto& read : Meta.GetReads()) {
            auto& state = PendingShards.emplace_back(TShardState(read.GetShardId()));
            state.Ranges.reserve(read.GetKeyRanges().size());
            for (const auto& range : read.GetKeyRanges()) {
                auto& sr = state.Ranges.emplace_back(TSerializedTableRange(range));
                if (!range.HasTo()) {
                    sr.To = sr.From;
                    sr.FromInclusive = sr.ToInclusive = true;
                }
            }

            Y_VERIFY_DEBUG(!state.Ranges.empty());
        }

        StartTableScan();

        ContinueExecute();
        Become(&TKqpScanComputeActor::StateFunc);
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleExecute);
                hFunc(TEvPrivate::TEvRetryShard, HandleExecute);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleExecute);
                hFunc(TEvents::TEvUndelivered, HandleExecute);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleExecute);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    BaseStateFuncBody(ev, ctx);
            }
        } catch (const TMemoryLimitExceededException& e) {
            InternalError(NYql::NDqProto::StatusIds::PRECONDITION_FAILED, TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                    << ", host: " << HostName() << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const yexception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, e.what());
        }

        ReportEventElapsedTime();
    }

    void HandleEvWakeup(EEvWakeupTag tag) {
        switch (tag) {
            case RlSendAllowedTag:
                ProcessScanData();
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

    void FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last) {
        if (last && ScanData && dst->TasksSize() > 0) {
            YQL_ENSURE(dst->TasksSize() == 1);

            // NKqpProto::TKqpComputeActorExtraStats extraStats;

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
                NKqpProto::TKqpScanTableExtraStats tableExtraStats;
                // protoScanStats->SetIScanStartTimeMs()
                // protoScanStats->SetIScanFinishTimeMs();
                tableExtraStats.SetIScanCpuTimeUs(x->ScanCpuTime.MicroSeconds());
                tableExtraStats.SetIScanWaitTimeUs(x->ScanWaitTime.MicroSeconds());
                tableExtraStats.SetIScanPageFaults(x->PageFaults);

                tableExtraStats.SetMessages(x->Messages);
                tableExtraStats.SetMessagesByPageFault(x->MessagesByPageFault);

                tableStats->MutableExtra()->PackFrom(tableExtraStats);
            }

            // dst->MutableExtra()->PackFrom(extraStats);
        }
    }

protected:
    ui64 CalcMkqlMemoryLimit() override {
        return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ScanBufferSize;
    }

private:
    void ProcessRlNoResourceAndDie() {
        const NYql::TIssue issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_RESOURCE_USAGE_LIMITED,
            "Throughput limit exceeded for query");
        CA_LOG_E("Throughput limit exceeded, we got "
             << PendingScanData.size() << " pending messages,"
             << " stream will be terminated");

        State = NDqProto::COMPUTE_STATE_FAILURE;
        ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::OVERLOADED, TIssues({issue}));
    }

    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        YQL_ENSURE(ScanData);
        auto& msg = ev->Get()->Record;
        auto scanActorId = ActorIdFromProto(msg.GetScanActorId());
        auto* state = GetShardState(msg, scanActorId);
        if (!state)
            return;

        CA_LOG_D("Got EvScanInitActor from " << scanActorId << ", gen: " << msg.GetGeneration()
            << ", state: " << EShardStateToString(state->State) << ", stateGen: " << state->Generation
            << ", tabletId: " << state->TabletId);

        YQL_ENSURE(state->Generation == msg.GetGeneration());

        if (state->State == EShardState::Starting) {
            state->State = EShardState::Running;
            state->ActorId = scanActorId;
            state->ResetRetry();
            AffectedShards.insert(state->TabletId);
            SendScanDataAck(state);
        } else {
            TerminateExpiredScan(scanActorId, "Got unexpected/expired EvScanInitActor, terminate it");
        }
    }

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
        YQL_ENSURE(ScanData);
        auto& msg = *ev->Get();
        auto* state = GetShardState(msg, ev->Sender);
        if (!state)
            return;

        YQL_ENSURE(state->Generation == msg.Generation);
        if (state->State != EShardState::Running) {
            return TerminateExpiredScan(ev->Sender, "Cancel expired scan");
        }

        YQL_ENSURE(state->ActorId == ev->Sender, "expected: " << state->ActorId << ", got: " << ev->Sender);
        TInstant startTime = TActivationContext::Now();
        if (ev->Get()->Finished) {
            state->State = EShardState::PostRunning;
        }
        PendingScanData.emplace_back(std::make_pair(ev, startTime));

        if (IsQuotingEnabled()) {
            AcquireRateQuota();
        } else {
            ProcessScanData();
        }
    }

    void ProcessPendingScanDataItem(TEvKqpCompute::TEvScanData::TPtr& ev, const TInstant& enqueuedAt) {
        auto& msg = *ev->Get();
        auto* state = GetShardState(msg, ev->Sender);
        if (!state)
            return;

        TDuration latency;
        if (enqueuedAt != TInstant::Zero()) {
            latency = TActivationContext::Now() - enqueuedAt;
            Counters->ScanQueryRateLimitLatency->Collect(latency.MilliSeconds());
        }

        YQL_ENSURE(state->ActorId == ev->Sender, "expected: " << state->ActorId << ", got: " << ev->Sender);

        state->LastKey = std::move(msg.LastKey);
        ui64 bytes = 0;
        ui64 rowsCount = 0;
        {
            auto guard = TaskRunner->BindAllocator();
            switch (msg.GetDataFormat()) {
                case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
                case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED: {
                    if (!msg.Rows.empty()) {
                        bytes = ScanData->AddRows(msg.Rows, state->TabletId, TaskRunner->GetHolderFactory());
                        rowsCount = msg.Rows.size();
                    }
                    break;
                }
                case NKikimrTxDataShard::EScanDataFormat::ARROW: {
                    if (msg.ArrowBatch != nullptr) {
                        bytes = ScanData->AddRows(*msg.ArrowBatch, state->TabletId, TaskRunner->GetHolderFactory());
                        rowsCount = msg.ArrowBatch->num_rows();
                    }
                    break;
                }
            }
        }

        Stats.AddReadStat(state->TabletId, rowsCount, bytes);

        CA_LOG_D("Got EvScanData, rows: " << rowsCount << ", bytes: " << bytes << ", finished: " << msg.Finished
                << ", from: " << ev->Sender << ", shards remain: " << PendingShards.size()
                << ", in flight shards " << InFlightShards.size()
                << ", delayed for: " << latency.SecondsFloat() << " seconds by ratelimiter"
                << ", tabletId: " << state->TabletId);

        if (rowsCount == 0 && !msg.Finished && state->State != EShardState::PostRunning) {
            SendScanDataAck(state);
        }

        if (msg.Finished) {
            CA_LOG_D("Tablet " << state->TabletId << " scan finished, unlink");
            Stats.CompleteShard(state->TabletId);
            StopReadFromTablet(state);

            if (!StartTableScan()) {
                CA_LOG_D("Finish scans");
                ScanData->Finish();

                if (ScanData->BasicStats) {
                    ScanData->BasicStats->AffectedShards = AffectedShards.size();
                }
            }
        }

        if (Y_UNLIKELY(ScanData->ProfileStats)) {
            ScanData->ProfileStats->Messages++;
            ScanData->ProfileStats->ScanCpuTime += msg.CpuTime;
            ScanData->ProfileStats->ScanWaitTime += msg.WaitTime;
            if (msg.PageFault) {
                ScanData->ProfileStats->PageFaults += msg.PageFaults;
                ScanData->ProfileStats->MessagesByPageFault++;
            }
        }
    }

    void ProcessScanData() {
        YQL_ENSURE(ScanData);
        YQL_ENSURE(!PendingScanData.empty());

        auto ev = std::move(PendingScanData.front().first);
        auto enqueuedAt = std::move(PendingScanData.front().second);
        PendingScanData.pop_front();

        auto& msg = *ev->Get();
        auto* state = GetShardState(msg, ev->Sender);
        if (!state)
            return;

        if (state->State == EShardState::Running || state->State == EShardState::PostRunning) {
            ProcessPendingScanDataItem(ev, enqueuedAt);
            DoExecute();
        } else {
            TerminateExpiredScan(ev->Sender, "Cancel expired scan");
        }
    }

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
        YQL_ENSURE(ScanData);
        auto& msg = ev->Get()->Record;

        Ydb::StatusIds::StatusCode status = msg.GetStatus();
        TIssues issues;
        IssuesFromMessage(msg.GetIssues(), issues);

        auto* state = GetShardState(msg, TActorId());
        if (!state)
            return;

        CA_LOG_W("Got EvScanError scan state: " << EShardStateToString(state->State)
            << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
            << ", reason: " << issues.ToString()
            << ", tablet id: " << state->TabletId);

        YQL_ENSURE(state->Generation == msg.GetGeneration());

        if (state->State == EShardState::Starting) {
            if (FindSchemeErrorInIssues(status, issues)) {
                return EnqueueResolveShard(state);
            }

            State = NDqProto::COMPUTE_STATE_FAILURE;
            return ReportStateAndMaybeDie(YdbStatusToDqStatus(status), issues);
        }

        if (state->State == EShardState::PostRunning || state->State == EShardState::Running) {
            state->State = EShardState::Initial;
            state->ActorId = {};
            state->ResetRetry();
            return StartReadShard(state);
        }
    }

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        YQL_ENSURE(ScanData);
        auto& msg = *ev->Get();

        auto stateIt = InFlightShards.find(msg.TabletId);
        if (stateIt == InFlightShards.end()) {
            CA_LOG_E("Broken pipe with unknown tablet " << msg.TabletId);
            return;
        }

        auto* state = &(stateIt->second);
        CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered << ", " << EShardStateToString(state->State));
        if (state->State == EShardState::Starting || state->State == EShardState::Running) {
            return RetryDeliveryProblem(state);
        }
    }

    void HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev) {
        ui64 tabletId = ev->Get()->TabletId;
        auto stateIt = InFlightShards.find(tabletId);
        if (stateIt == InFlightShards.end()) {
            CA_LOG_E("Received retry shard for unexpected tablet " << tabletId);
            return;
        }

        auto* state = &(stateIt->second);
        SendStartScanRequest(state, state->Generation);
    }

    void HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        YQL_ENSURE(ScanData);
        YQL_ENSURE(!PendingResolveShards.empty());
        auto state = std::move(PendingResolveShards.front());
        PendingResolveShards.pop_front();
        ResolveNextShard();

        StopReadFromTablet(&state);

        YQL_ENSURE(state.State == EShardState::Resolving);
        CA_LOG_D("Received TEvResolveKeySetResult update for table '" << ScanData->TablePath << "'");

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            CA_LOG_E("Resolve request failed for table '" << ScanData->TablePath << "', ErrorCount# " << request->ErrorCount);

            auto statusCode = NDqProto::StatusIds::UNAVAILABLE;
            auto issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
            TString error;

            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    // invalidate table
                    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanData->TableId, {}));

                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_ERROR;
                            error = TStringBuilder() << "Table '" << ScanData->TablePath << "' not exists.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                        statusCode = NDqProto::StatusIds::ABORTED;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                            error = TStringBuilder() << "Table '" << ScanData->TablePath << "' scheme changed.";
                            break;
                        default:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_ERROR;
                            error = TStringBuilder() << "Unresolved table '" << ScanData->TablePath << "'. Status: " << x.Status;
                            break;
                    }
                }
            }

            return InternalError(statusCode, issueCode, error);
        }

        auto keyDesc = std::move(request->ResultSet[0].KeyDescription);

        if (keyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << ScanData->TablePath << "'";
            CA_LOG_E(error);
            InternalError(NDqProto::StatusIds::SCHEME_ERROR, TIssuesIds::KIKIMR_SCHEME_ERROR, error);
            return;
        }

        const auto& tr = *AppData()->TypeRegistry;

        TVector<TShardState> newShards;
        newShards.reserve(keyDesc->GetPartitions().size());

        for (ui64 idx = 0, i = 0; idx < keyDesc->GetPartitions().size(); ++idx) {
            const auto& partition = keyDesc->GetPartitions()[idx];

            TTableRange partitionRange{
                idx == 0 ? state.Ranges.front().From.GetCells() : keyDesc->GetPartitions()[idx - 1].Range->EndKeyPrefix.GetCells(),
                idx == 0 ? state.Ranges.front().FromInclusive : !keyDesc->GetPartitions()[idx - 1].Range->IsInclusive,
                keyDesc->GetPartitions()[idx].Range->EndKeyPrefix.GetCells(),
                keyDesc->GetPartitions()[idx].Range->IsInclusive
            };

            CA_LOG_D("Processing resolved ShardId# " << partition.ShardId
                << ", partition range: " << DebugPrintRange(KeyColumnTypes, partitionRange, tr)
                << ", i: " << i << ", state ranges: " << state.Ranges.size());

            auto newShard = TShardState(partition.ShardId);

            for (ui64 j = i; j < state.Ranges.size(); ++j) {
                CA_LOG_D("Intersect state range #" << j << " " << DebugPrintRange(KeyColumnTypes, state.Ranges[j].ToTableRange(), tr)
                    << " with partition range " << DebugPrintRange(KeyColumnTypes, partitionRange, tr));

                auto intersection = Intersect(KeyColumnTypes, partitionRange, state.Ranges[j].ToTableRange());

                if (!intersection.IsEmptyRange(KeyColumnTypes)) {
                    CA_LOG_D("Add range to new shardId: " << partition.ShardId
                        << ", range: " << DebugPrintRange(KeyColumnTypes, intersection, tr));

                    newShard.Ranges.emplace_back(TSerializedTableRange(intersection));
                } else {
                    CA_LOG_D("empty intersection");
                    if (j > i) {
                        i = j - 1;
                    }
                    break;
                }
            }

            if (!newShard.Ranges.empty()) {
                newShards.emplace_back(std::move(newShard));
            }
        }

        YQL_ENSURE(!newShards.empty());

        for (int i = newShards.ysize() - 1; i >= 0; --i) {
            PendingShards.emplace_front(std::move(newShards[i]));
        }

        if (!state.LastKey.empty()) {
            PendingShards.front().LastKey = std::move(state.LastKey);
        }

        if (IsDebugLogEnabled(TlsActivationContext->ActorSystem(), NKikimrServices::KQP_COMPUTE)
            && PendingShards.size() + InFlightShards.size() > 0)
        {
            TStringBuilder sb;
            if (!PendingShards.empty()) {
                sb << "Pending shards States: ";
                for (auto& st : PendingShards) {
                    sb << st.ToString(KeyColumnTypes) << "; ";
                }
            }

            if (!InFlightShards.empty()) {
                sb << "In Flight shards States: ";
                for(auto& [_, st] : InFlightShards) {
                    sb << st.ToString(KeyColumnTypes) << "; ";
                }
            }
            CA_LOG_D(sb);
        }

        StartTableScan();
    }

    void HandleExecute(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvDataShard::TEvKqpScan::EventType:
                // handled by TEvPipeCache::TEvDeliveryProblem event
                return;
            case TEvKqpCompute::TEvScanDataAck::EventType:
                ui64 tabletId = ev->Cookie;
                auto it = InFlightShards.find(tabletId);
                if (it == InFlightShards.end()) {
                    CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", " << tabletId);
                    return;
                }

                auto& shard = it->second;
                if (shard.State == EShardState::Running && ev->Sender == shard.ActorId) {
                    CA_LOG_E("TEvScanDataAck lost while running scan, terminate execution. DataShard actor: "
                        << shard.ActorId);
                    InternalError(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                        "Delivery problem: EvScanDataAck lost.");
                } else {
                    CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", active scan actor: " << shard.ActorId);
                }
                return;
        }
        TBase::HandleExecuteBase(ev);
    }

    void HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        auto nodeId = ev->Get()->NodeId;
        CA_LOG_N("Disconnected node " << nodeId);

        TrackingNodes.erase(nodeId);
        for(auto& [tabletId, state] : InFlightShards) {
            if (state.ActorId && state.ActorId.NodeId() == nodeId) {
                InternalError(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Connection with node " << nodeId << " lost.");
            }
        }
    }

private:

    bool IsSortedOutput() const {
        return Meta.HasSorted() ? Meta.GetSorted() : true;
    }

    bool StartTableScan() {
        // allow reading from multiple shards if data is not sorted
        const ui32 maxAllowedInFlight = IsSortedOutput() ? 1 : PendingShards.size();

        while (!PendingShards.empty() && InFlightShards.size() + PendingResolveShards.size() + 1 <= maxAllowedInFlight) {
            ui64 tabletId = PendingShards.front().TabletId;
            auto [it, success] = InFlightShards.emplace(tabletId, std::move(PendingShards.front()));
            PendingShards.pop_front();
            StartReadShard(&(it->second));
        }

        CA_LOG_D("Scheduled table scans, in flight: " << InFlightShards.size() << " shards. "
            << "pending shards to read: " << PendingShards.size() << ", "
            << "pending resolve shards: " << PendingResolveShards.size() << ", "
            << "average read rows: " << Stats.AverageReadRows() << ", "
            << "average read bytes: " << Stats.AverageReadBytes() << ", ");

        return InFlightShards.size() + PendingShards.size() + PendingResolveShards.size() > 0;
    }

    void StartReadShard(TShardState* state) {
        YQL_ENSURE(state->State == EShardState::Initial);
        state->State = EShardState::Starting;
        state->Generation = AllocateGeneration(state);
        state->ActorId = {};
        SendStartScanRequest(state, state->Generation);
    }

    void SendScanDataAck(TShardState* state) {
        ui64 freeSpace = CalculateFreeSpace();
        CA_LOG_D("Send EvScanDataAck to " << state->ActorId << ", freeSpace: " << freeSpace << ", gen: " << state->Generation);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (TrackingNodes.insert(state->ActorId.NodeId()).second) {
            flags |= IEventHandle::FlagSubscribeOnSession;
        }
        Send(state->ActorId, new TEvKqpCompute::TEvScanDataAck(freeSpace, state->Generation), flags, state->TabletId);
    }

    void SendStartScanRequest(TShardState* state, ui32 gen) {
        YQL_ENSURE(state->State == EShardState::Starting);

        auto ev = MakeHolder<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(ScanData->TableId.PathId.LocalPathId);
        for (auto& column: ScanData->GetColumns()) {
            ev->Record.AddColumnTags(column.Tag);
            ev->Record.AddColumnTypes(column.Type);
        }
        ev->Record.MutableSkipNullKeys()->CopyFrom(Meta.GetSkipNullKeys());

        CA_LOG_D("Start scan request, " << state->ToString(KeyColumnTypes));
        auto ranges = state->GetScanRanges(KeyColumnTypes);
        auto protoRanges = ev->Record.MutableRanges();
        protoRanges->Reserve(ranges.size());

        for (auto& range: ranges) {
            auto newRange = protoRanges->Add();
            range.Serialize(*newRange);
        }

        ev->Record.MutableSnapshot()->CopyFrom(Snapshot);
        if (RuntimeSettings.Timeout) {
            ev->Record.SetTimeoutMs(RuntimeSettings.Timeout.Get()->MilliSeconds());
        }
        ev->Record.SetStatsMode(RuntimeSettings.StatsMode);
        ev->Record.SetScanId(0);
        ev->Record.SetTxId(std::get<ui64>(TxId));
        ev->Record.SetTablePath(ScanData->TablePath);
        ev->Record.SetSchemaVersion(ScanData->TableId.SchemaVersion);

        ev->Record.SetGeneration(gen);

        ev->Record.SetReverse(Meta.GetReverse());
        ev->Record.SetItemsLimit(Meta.GetItemsLimit());

        if (Meta.HasOlapProgram()) {
            TString programBytes;
            TStringOutput stream(programBytes);
            Meta.GetOlapProgram().SerializeToArcadiaStream(&stream);
            ev->Record.SetOlapProgram(programBytes);
            ev->Record.SetOlapProgramType(
                NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS
            );
        }

        ev->Record.SetDataFormat(Meta.GetDataFormat());

        bool subscribed = std::exchange(state->SubscribedOnTablet, true);

        CA_LOG_D("Send EvKqpScan to shardId: " << state->TabletId << ", tablePath: " << ScanData->TablePath
            << ", gen: " << gen << ", subscribe: " << (!subscribed)
            << ", range: " << DebugPrintRanges(KeyColumnTypes, ranges, *AppData()->TypeRegistry));

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), state->TabletId, !subscribed),
            IEventHandle::FlagTrackDelivery);
    }

    void RetryDeliveryProblem(TShardState* state) {
        Counters->ScanQueryShardDisconnect->Inc();

        if (state->TotalRetries >= MAX_TOTAL_SHARD_RETRIES) {
            CA_LOG_E("TKqpScanComputeActor: broken pipe with tablet " << state->TabletId
                << ", retries limit exceeded (" << state->TotalRetries << ")");
            return InternalError(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Retries limit with shard " << state->TabletId << " exceeded.");
        }

        // note: it might be possible that shard is already removed after successful split/merge operation and cannot be found
        // in this case the next TEvKqpScan request will receive the delivery problem response.
        // so after several consecutive delivery problem responses retry logic should
        // resolve shard details again.
        if (state->RetryAttempt >= MAX_SHARD_RETRIES) {
            return ResolveShard(state);
        }

        state->RetryAttempt++;
        state->TotalRetries++;
        state->Generation = AllocateGeneration(state);
        state->ActorId = {};
        state->State = EShardState::Starting;
        state->SubscribedOnTablet = false;
        auto retryDelay = state->CalcRetryDelay();
        CA_LOG_W("TKqpScanComputeActor: broken pipe with tablet " << state->TabletId
            << ", restarting scan from last received key " << state->PrintLastKey(KeyColumnTypes)
            << ", attempt #" << state->RetryAttempt << " (total " << state->TotalRetries << ")"
            << " schedule after " << retryDelay);

        state->RetryTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), retryDelay,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryShard(state->TabletId)));
    }

    bool IsQuotingEnabled() const {
        const auto& rlPath = GetRlPath();
        return rlPath.Defined();
    }

    void AcquireRateQuota() {
        const auto& rlPath = GetRlPath();
        auto selfId = this->SelfId();
        auto as = TActivationContext::ActorSystem();

        auto onSendAllowed = [selfId, as]() mutable {
            as->Send(selfId, new TEvents::TEvWakeup(EEvWakeupTag::RlSendAllowedTag));
        };

        auto onSendTimeout = [selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup(EEvWakeupTag::RlNoResourceTag));
        };

        const NRpcService::TRlFullPath rlFullPath {
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

    void TerminateExpiredScan(const TActorId& actorId, TStringBuf msg) {
        CA_LOG_W(msg);

        auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Cancel unexpected/expired scan");
        Send(actorId, abortEv.Release());
    }

    void ResolveNextShard() {
        if (!PendingResolveShards.empty()) {
            auto& state = PendingResolveShards.front();
            ResolveShard(&state);
        }
    }

    void EnqueueResolveShard(TShardState* state) {
        auto it = InFlightShards.find(state->TabletId);
        YQL_ENSURE(it != InFlightShards.end());
        PendingResolveShards.emplace_back(std::move(it->second));
        InFlightShards.erase(it);
        if (PendingResolveShards.size() == 1) {
            ResolveNextShard();
        }
    }

    void StopReadFromTablet(TShardState* state) {
        CA_LOG_D("Unlink from tablet " << state->TabletId << " and stop reading from it.");
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(state->TabletId));
        InFlightShards.erase(state->TabletId);
    }

    void ResolveShard(TShardState* state) {
        if (state->ResolveAttempt >= MAX_SHARD_RESOLVES) {
            InternalError(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Table '" << ScanData->TablePath << "' resolve limit exceeded");
            return;
        }

        Counters->ScanQueryShardResolve->Inc();

        state->State = EShardState::Resolving;
        state->ResolveAttempt++;
        state->SubscribedOnTablet = false;

        auto range = TTableRange(state->Ranges.front().From.GetCells(), state->Ranges.front().FromInclusive,
                                 state->Ranges.back().To.GetCells(), state->Ranges.back().ToInclusive);

        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(ScanData->GetColumns().size());
        for (const auto& column : ScanData->GetColumns()) {
            TKeyDesc::TColumnOp op;
            op.Column = column.Tag;
            op.Operation = TKeyDesc::EColumnOperation::Read;
            op.ExpectedType = column.Type;
            columns.emplace_back(std::move(op));
        }

        auto keyDesc = MakeHolder<TKeyDesc>(ScanData->TableId, range, TKeyDesc::ERowOperation::Read,
                                            KeyColumnTypes, columns);

        CA_LOG_D("Sending TEvResolveKeySet update for table '" << ScanData->TablePath << "'"
            << ", range: " << DebugPrintRange(KeyColumnTypes, range, *AppData()->TypeRegistry)
            << ", attempt #" << state->ResolveAttempt);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(std::move(keyDesc));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanData->TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

private:
    ui64 CalculateFreeSpace() const {
        return GetMemoryLimits().ScanBufferSize > ScanData->GetStoredBytes()
                ? GetMemoryLimits().ScanBufferSize - ScanData->GetStoredBytes()
                : 0ul;
    }

    std::any GetSourcesState() override {
        if (ScanData) {
            return CalculateFreeSpace();
        }
        return {};
    }

    void PollSources(std::any prev) override {
        if (!prev.has_value() || !ScanData) {
            return;
        }

        for (auto it = InFlightShards.begin(); it != InFlightShards.end(); ++it) {
            auto* state = &(it->second);
            const ui64 freeSpace = CalculateFreeSpace();
            const ui64 prevFreeSpace = std::any_cast<ui64>(prev);

            CA_LOG_T("Scan over tablet " << state->TabletId << " finished: " << ScanData->IsFinished()
                << ", prevFreeSpace: " << prevFreeSpace << ", freeSpace: " << freeSpace << ", peer: " << state->ActorId);

            if (!ScanData->IsFinished() && state->State != EShardState::PostRunning
                && prevFreeSpace < freeSpace && state->ActorId)
            {
                SendScanDataAck(state);
            }
        }
    }

    void TerminateSources(const TIssues& issues, bool success) override {
        if (!ScanData) {
            return;
        }

        auto prio = success ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
        for(auto it = InFlightShards.begin(); it != InFlightShards.end(); ++it) {
            auto* state = &(it->second);
            if (state->ActorId) {
                CA_LOG(prio, "Send abort execution event to scan over tablet: " << state->TabletId << ", table: "
                    << ScanData->TablePath << ", scan actor: " << state->ActorId << ", message: " << issues.ToOneLineString());

                Send(state->ActorId, new TEvKqp::TEvAbortExecution(
                    success ? NYql::NDqProto::StatusIds::SUCCESS : NYql::NDqProto::StatusIds::ABORTED, issues));
            } else {
                CA_LOG(prio, "Table: " << ScanData->TablePath << ", scan has not been started yet");
            }
        }
    }

    void PassAway() override {
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        for (ui32 nodeId : TrackingNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
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

    template<class TMessage>
    TShardState* GetShardState(const TMessage& msg, const TActorId& scanActorId) {
        ui32 generation;
        if constexpr(std::is_same_v<TMessage, NKikimrKqp::TEvScanError>) {
            generation = msg.GetGeneration();
        } else if constexpr(std::is_same_v<TMessage, NKikimrKqp::TEvScanInitActor>) {
            generation = msg.GetGeneration();
        } else {
            generation = msg.Generation;
        }

        auto it = AllocatedGenerations.find(generation);
        YQL_ENSURE(it != AllocatedGenerations.end(), "Received message from unknown scan or request.");
        ui64 tabletId = it->second;
        auto stateIt = InFlightShards.find(tabletId);
        if (stateIt == InFlightShards.end()) {
            TString error = TStringBuilder() << "Received message from scan shard which is not currently in flight, tablet" << tabletId;
            CA_LOG_W(error);
            if (scanActorId) {
                TerminateExpiredScan(scanActorId, error);
            }

            return nullptr;
        }

        auto& state = stateIt->second;
        if (state.Generation != generation) {
            TString error = TStringBuilder() << "Received message from expired scan, generation mistmatch, "
                << "expected: " << state.Generation << ", received: " << generation;
            CA_LOG_W(error);
            if (scanActorId) {
                TerminateExpiredScan(scanActorId, error);
            }

            return nullptr;
        }

        return &state;
    }

    ui32 AllocateGeneration(TShardState* state) {
        ui32 nextGeneration = ++LastGeneration;
        auto[it, success] = AllocatedGenerations.emplace(nextGeneration, state->TabletId);
        YQL_ENSURE(success, "Found duplicated while allocating next generation id for scan request: "
            << nextGeneration << ", tablet " << state->TabletId  << ", allocated for tablet " << it->second);
        return nextGeneration;
    }

private:
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    NKikimrKqp::TKqpSnapshot Snapshot;
    TIntrusivePtr<TKqpCounters> Counters;
    TScannedDataStats Stats;
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    TVector<NScheme::TTypeId> KeyColumnTypes;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    std::deque<std::pair<TEvKqpCompute::TEvScanData::TPtr, TInstant>> PendingScanData;
    std::deque<TShardState> PendingShards;
    std::deque<TShardState> PendingResolveShards;

    std::map<ui32, ui64> AllocatedGenerations;
    std::map<ui64, TShardState> InFlightShards;

    ui32 LastGeneration = 0;
    std::set<ui64> AffectedShards;
    std::set<ui32> TrackingNodes;
};

} // anonymous namespace

IActor* CreateKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
    NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, TIntrusivePtr<TKqpCounters> counters,
    NWilson::TTraceId traceId)
{
    return new TKqpScanComputeActor(snapshot, executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, counters, std::move(traceId));
}

} // namespace NKqp
} // namespace NKikimr
