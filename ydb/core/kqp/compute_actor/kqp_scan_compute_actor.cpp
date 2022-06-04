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


class TKqpScanComputeActor : public TDqComputeActorBase<TKqpScanComputeActor> {
    using TBase = TDqComputeActorBase<TKqpScanComputeActor>;

    struct TEvPrivate {
        enum EEv {
            EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryShard : public TEventLocal<TEvRetryShard, EvRetryShard> {};
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_COMPUTE_ACTOR;
    }

    TKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
        NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, TIntrusivePtr<TKqpCounters> counters)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings, memoryLimits)
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
            auto& state = Shards.emplace_back(TShardState(read.GetShardId()));
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
                    StateFuncBase(ev, ctx);
            }
        } catch (const TMemoryLimitExceededException& e) {
            InternalError(TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName() << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const yexception& e) {
            InternalError(TIssuesIds::DEFAULT_ERROR, e.what());
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
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(!Shards.empty());

        auto& msg = ev->Get()->Record;

        auto& state = Shards.front();
        auto scanActorId = ActorIdFromProto(msg.GetScanActorId());

        CA_LOG_D("Got EvScanInitActor from " << scanActorId << ", gen: " << msg.GetGeneration()
            << ", state: " << EShardStateToString(state.State) << ", stateGen: " << state.Generation);

        switch (state.State) {
            case EShardState::Starting: {
                if (state.Generation == msg.GetGeneration()) {
                    state.State = EShardState::Running;
                    state.ActorId = scanActorId;

                    state.ResetRetry();

                    AffectedShards.insert(state.TabletId);

                    SendScanDataAck(state, GetMemoryLimits().ScanBufferSize);
                    return;
                }

                if (state.Generation > msg.GetGeneration()) {
                    TerminateExpiredScan(scanActorId, "Got expired EvScanInitActor, terminate it");
                    return;
                }

                YQL_ENSURE(false, "Got EvScanInitActor from the future, gen: " << msg.GetGeneration()
                    << ", expected: " << state.Generation);
            }

            case EShardState::Initial:
            case EShardState::Running:
            case EShardState::PostRunning:
            case EShardState::Resolving: {
                TerminateExpiredScan(scanActorId, "Got unexpected/expired EvScanInitActor, terminate it");
                return;
            }
        }
    }

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(!Shards.empty());

        auto& msg = *ev->Get();
        auto& state = Shards.front();

        switch (state.State) {
            case EShardState::Running: {
                if (state.Generation == msg.Generation) {
                    YQL_ENSURE(state.ActorId == ev->Sender, "expected: " << state.ActorId << ", got: " << ev->Sender);

                    TInstant startTime = TActivationContext::Now();
                    if (ev->Get()->Finished) {
                        state.State = EShardState::PostRunning;
                    }
                    PendingScanData.emplace_back(std::make_pair(ev, startTime));

                    if (IsQuotingEnabled()) {
                        AcquireRateQuota();
                    } else {
                        ProcessScanData();
                    }

                } else if (state.Generation > msg.Generation) {
                    TerminateExpiredScan(ev->Sender, "Cancel expired scan");
                } else {
                    YQL_ENSURE(false, "EvScanData from the future, expected: " << state.Generation << ", got: " << msg.Generation);
                }
                break;
            }
            case EShardState::PostRunning:
                TerminateExpiredScan(ev->Sender, "Unexpected data after finish");
                break;
            case EShardState::Initial:
            case EShardState::Starting:
            case EShardState::Resolving:
                TerminateExpiredScan(ev->Sender, "Cancel unexpected scan");
                break;
        }
    }

    void ProcessScanData() {
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(!Shards.empty());
        Y_VERIFY(!PendingScanData.empty());

        auto& ev = PendingScanData.front().first;

        TDuration latency;
        if (PendingScanData.front().second != TInstant::Zero()) {
            latency = TActivationContext::Now() - PendingScanData.front().second;
            Counters->ScanQueryRateLimitLatency->Collect(latency.MilliSeconds());
        }

        auto& msg = *ev->Get();
        auto& state = Shards.front();

        switch (state.State) {
            case EShardState::Running:
            case EShardState::PostRunning: {
                if (state.Generation == msg.Generation) {
                    YQL_ENSURE(state.ActorId == ev->Sender, "expected: " << state.ActorId << ", got: " << ev->Sender);

                    LastKey = std::move(msg.LastKey);

                    ui64 bytes = 0;
                    ui64 rowsCount = 0;
                    {
                        auto guard = TaskRunner->BindAllocator();
                        switch (msg.GetDataFormat()) {
                            case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
                            case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED: {
                                if (!msg.Rows.empty()) {
                                    bytes = ScanData->AddRows(msg.Rows, state.TabletId, TaskRunner->GetHolderFactory());
                                    rowsCount = msg.Rows.size();
                                }
                                break;
                            }
                            case NKikimrTxDataShard::EScanDataFormat::ARROW: {
                                if (msg.ArrowBatch != nullptr) {
                                    bytes = ScanData->AddRows(*msg.ArrowBatch, state.TabletId, TaskRunner->GetHolderFactory());
                                    rowsCount = msg.ArrowBatch->num_rows();
                                }
                                break;
                            }
                        }
                    }

                    CA_LOG_D("Got EvScanData, rows: " << rowsCount << ", bytes: " << bytes << ", finished: " << msg.Finished
                        << ", from: " << ev->Sender << ", shards remain: " << Shards.size()
                        << ", delayed for: " << latency.SecondsFloat() << " seconds by ratelimitter");

                    if (rowsCount == 0 && !msg.Finished && state.State != EShardState::PostRunning) {
                        ui64 freeSpace = GetMemoryLimits().ScanBufferSize > ScanData->GetStoredBytes()
                            ? GetMemoryLimits().ScanBufferSize - ScanData->GetStoredBytes()
                            : 0ul;
                        SendScanDataAck(state, freeSpace);
                    }

                    if (msg.Finished) {
                        CA_LOG_D("Tablet " << state.TabletId << " scan finished, unlink");
                        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(state.TabletId));

                        Shards.pop_front();

                        if (!Shards.empty()) {
                            CA_LOG_D("Starting next scan");
                            StartTableScan();
                        } else {
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

                    DoExecute();

                } else if (state.Generation > msg.Generation) {
                    TerminateExpiredScan(ev->Sender, "Cancel expired scan");
                } else {
                    YQL_ENSURE(false, "EvScanData from the future, expected: " << state.Generation << ", got: " << msg.Generation);
                }
                break;
            }

            case EShardState::Initial:
            case EShardState::Starting:
            case EShardState::Resolving: {
                TerminateExpiredScan(ev->Sender, "Cancel unexpected scan");
                break;
            }
        }
        PendingScanData.pop_front();
    }

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(!Shards.empty());

        auto& msg = ev->Get()->Record;

        Ydb::StatusIds::StatusCode status = msg.GetStatus();
        TIssues issues;
        IssuesFromMessage(msg.GetIssues(), issues);

        auto& state = Shards.front();

        switch (state.State) {
            case EShardState::Starting: {
                if (state.Generation == msg.GetGeneration()) {
                    CA_LOG_W("Got EvScanError while starting scan, status: " << Ydb::StatusIds_StatusCode_Name(status)
                        << ", reason: " << issues.ToString());

                    bool schemeError = false;

                    if (status == Ydb::StatusIds::SCHEME_ERROR) {
                        schemeError = true;
                    } else if (status == Ydb::StatusIds::ABORTED) {
                        for (auto& issue : issues) {
                            WalkThroughIssues(issue, false, [&schemeError](const TIssue& x, ui16) {
                                if (x.IssueCode == TIssuesIds::KIKIMR_SCHEME_MISMATCH) {
                                    schemeError = true;
                                }
                            });
                            if (schemeError) {
                                break;
                            }
                        }
                    }

                    if (schemeError) {
                        ResolveShard(state);
                        return;
                    }

                    State = NDqProto::COMPUTE_STATE_FAILURE;
                    ReportStateAndMaybeDie(YdbStatusToDqStatus(status), issues);
                    return;
                }

                if (state.Generation > msg.GetGeneration()) {
                    // expired message
                    return;
                }

                YQL_ENSURE(false, "Got EvScanError from the future, expected: " << state.Generation
                    << ", got: " << msg.GetGeneration());
                break;
            }

            case EShardState::PostRunning:
            case EShardState::Running: {
                if (state.Generation == msg.GetGeneration()) {
                    CA_LOG_W("Got EvScanError while running scan, status: " << Ydb::StatusIds_StatusCode_Name(status)
                        << ", reason: " << issues.ToString() << ", restart");

                    state.State = EShardState::Initial;
                    state.ActorId = {};
                    state.ResetRetry();
                    StartTableScan();
                    return;
                }

                if (state.Generation > msg.GetGeneration()) {
                    // expired message
                    return;
                }

                YQL_ENSURE(false, "Got EvScanError from the future, expected: " << state.Generation
                    << ", got: " << msg.GetGeneration());
            }

            case EShardState::Initial:
            case EShardState::Resolving: {
                // do nothing
                return;
            }
        }
    }

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto& msg = *ev->Get();
        CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered);

        if (Shards.empty()) {
            return;
        }

        Y_VERIFY_DEBUG(ScanData);
        auto& state = Shards.front();

        if (state.TabletId != msg.TabletId) {
            CA_LOG_E("Unknown tablet " << msg.TabletId << ", expected " << state.TabletId);
            return;
        }

        switch (state.State) {
            case EShardState::Starting:
            case EShardState::Running: {
                RetryDeliveryProblem(state);
                return;
            }

            case EShardState::Initial:
            case EShardState::Resolving:
            case EShardState::PostRunning: {
                CA_LOG_W("TKqpScanComputeActor: broken pipe with tablet " << state.TabletId
                    << ", state: " << (int) state.State);
                return;
            }
        }
    }

    void HandleExecute(TEvPrivate::TEvRetryShard::TPtr&) {
        Y_VERIFY_DEBUG(!Shards.empty());
        auto& state = Shards.front();
        SendStartScanRequest(state, state.Generation);
    }

    void HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        Y_VERIFY_DEBUG(ScanData);
        Y_VERIFY_DEBUG(!Shards.empty());

        auto state = std::move(Shards.front());
        Shards.pop_front();

        CA_LOG_D("Get resolve result, unlink from tablet " << state.TabletId);
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(state.TabletId));

        YQL_ENSURE(state.State == EShardState::Resolving);

        CA_LOG_D("Received TEvResolveKeySetResult update for table '" << ScanData->TablePath << "'");

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            CA_LOG_E("Resolve request failed for table '" << ScanData->TablePath << "', ErrorCount# " << request->ErrorCount);

            TString error;
            TIssuesIds::EIssueCode issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;

            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    // invalidate table
                    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanData->TableId, {}));

                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            issueCode = TIssuesIds::KIKIMR_SCHEME_ERROR;
                            error = TStringBuilder() << "Table '" << ScanData->TablePath << "' not exists.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                            issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                            error = TStringBuilder() << "Table '" << ScanData->TablePath << "' scheme changed.";
                            break;
                        default:
                            issueCode = TIssuesIds::KIKIMR_SCHEME_ERROR;
                            error = TStringBuilder() << "Unresolved table '" << ScanData->TablePath << "'. Status: " << x.Status;
                            break;
                    }
                }
            }

            return InternalError(issueCode, error);
        }

        auto keyDesc = std::move(request->ResultSet[0].KeyDescription);

        if (keyDesc->Partitions.empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << ScanData->TablePath << "'";
            CA_LOG_E(error);
            InternalError(TIssuesIds::KIKIMR_SCHEME_ERROR, error);
            return;
        }

        const auto& tr = *AppData()->TypeRegistry;

        TVector<TShardState> newShards;
        newShards.reserve(keyDesc->Partitions.size());

        for (ui64 idx = 0, i = 0; idx < keyDesc->Partitions.size(); ++idx) {
            const auto& partition = keyDesc->Partitions[idx];

            TTableRange partitionRange{
                idx == 0 ? state.Ranges.front().From.GetCells() : keyDesc->Partitions[idx - 1].Range->EndKeyPrefix.GetCells(),
                idx == 0 ? state.Ranges.front().FromInclusive : !keyDesc->Partitions[idx - 1].Range->IsInclusive,
                keyDesc->Partitions[idx].Range->EndKeyPrefix.GetCells(),
                keyDesc->Partitions[idx].Range->IsInclusive
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
            Shards.emplace_front(std::move(newShards[i]));
        }

        if (IsDebugLogEnabled(TlsActivationContext->ActorSystem(), NKikimrServices::KQP_COMPUTE)) {
            TStringBuilder sb;
            sb << "States: ";
            for (auto& st : Shards) {
                sb << st.ToString(KeyColumnTypes) << "; ";
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
                if (Shards.empty()) {
                    return;
                }
                auto& shard = Shards.front();
                if (shard.State == EShardState::Running && ev->Sender == shard.ActorId) {
                    CA_LOG_E("TEvScanDataAck lost while running scan, terminate execution. DataShard actor: "
                        << shard.ActorId);
                    InternalError(TIssuesIds::DEFAULT_ERROR, "Delivery problem: EvScanDataAck lost.");
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

        auto& state = Shards.front();
        if (state.ActorId && state.ActorId.NodeId() == nodeId) {
            InternalError(TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Connection with node " << nodeId << " lost.");
        }
    }

private:
    void StartTableScan() {
        YQL_ENSURE(!Shards.empty());

        auto& state = Shards.front();

        YQL_ENSURE(state.State == EShardState::Initial);
        state.State = EShardState::Starting;
        state.Generation = ++LastGeneration;
        state.ActorId = {};

        CA_LOG_D("StartTableScan: '" << ScanData->TablePath << "', shardId: " << state.TabletId << ", gen: " << state.Generation
            << ", ranges: " << DebugPrintRanges(KeyColumnTypes, state.GetScanRanges(KeyColumnTypes, LastKey), *AppData()->TypeRegistry));

        SendStartScanRequest(state, state.Generation);
    }

    void SendScanDataAck(TShardState& state, ui64 freeSpace) {
        CA_LOG_D("Send EvScanDataAck to " << state.ActorId << ", freeSpace: " << freeSpace << ", gen: " << state.Generation);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (TrackingNodes.insert(state.ActorId.NodeId()).second) {
            flags |= IEventHandle::FlagSubscribeOnSession;
        }
        Send(state.ActorId, new TEvKqpCompute::TEvScanDataAck(freeSpace, state.Generation), flags);
    }

    void SendStartScanRequest(TShardState& state, ui32 gen) {
        YQL_ENSURE(state.State == EShardState::Starting);

        auto ev = MakeHolder<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(ScanData->TableId.PathId.LocalPathId);
        for (auto& column: ScanData->GetColumns()) {
            ev->Record.AddColumnTags(column.Tag);
            ev->Record.AddColumnTypes(column.Type);
        }
        ev->Record.MutableSkipNullKeys()->CopyFrom(Meta.GetSkipNullKeys());

        auto ranges = state.GetScanRanges(KeyColumnTypes, LastKey);
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

        bool subscribed = std::exchange(state.SubscribedOnTablet, true);

        CA_LOG_D("Send EvKqpScan to shardId: " << state.TabletId << ", tablePath: " << ScanData->TablePath
            << ", gen: " << gen << ", subscribe: " << (!subscribed)
            << ", range: " << DebugPrintRanges(KeyColumnTypes, ranges, *AppData()->TypeRegistry));

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), state.TabletId, !subscribed),
            IEventHandle::FlagTrackDelivery);
    }

    void RetryDeliveryProblem(TShardState& state) {
        Counters->ScanQueryShardDisconnect->Inc();

        if (state.TotalRetries >= MAX_TOTAL_SHARD_RETRIES) {
            CA_LOG_E("TKqpScanComputeActor: broken pipe with tablet " << state.TabletId
                << ", retries limit exceeded (" << state.TotalRetries << ")");
            return InternalError(TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                << "Retries limit with shard " << state.TabletId << " exceeded.");
        }

        // note: it might be possible that shard is already removed after successful split/merge operation and cannot be found
        // in this case the next TEvKqpScan request will receive the delivery problem response.
        // so after several consecutive delivery problem responses retry logic should
        // resolve shard details again.
        if (state.RetryAttempt >= MAX_SHARD_RETRIES) {
            return ResolveShard(state);
        }

        state.RetryAttempt++;
        state.TotalRetries++;
        state.Generation = ++LastGeneration;
        state.ActorId = {};
        state.State = EShardState::Starting;
        state.SubscribedOnTablet = false;

        auto retryDelay = state.CalcRetryDelay();
        if (retryDelay) {
            CA_LOG_W("TKqpScanComputeActor: broken pipe with tablet " << state.TabletId
                << ", restarting scan from last received key " << PrintLastKey()
                << ", attempt #" << state.RetryAttempt << " (total " << state.TotalRetries << ")"
                << " schedule after " << retryDelay);

            state.RetryTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), retryDelay,
                new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryShard));
        } else {
            CA_LOG_W("TKqpScanComputeActor: broken pipe with tablet " << state.TabletId
                << ", restarting scan from last received key " << PrintLastKey()
                << ", attempt #" << state.RetryAttempt << " (total " << state.TotalRetries << ")");

            SendStartScanRequest(state, state.Generation);
        }
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

    void ResolveShard(TShardState& state) {
        // resolve shards
        if (state.ResolveAttempt >= MAX_SHARD_RESOLVES) {
            InternalError(TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder()
                << "Table '" << ScanData->TablePath << "' resolve limit exceeded");
            return;
        }

        Counters->ScanQueryShardResolve->Inc();

        state.State = EShardState::Resolving;
        state.ResolveAttempt++;
        state.SubscribedOnTablet = false;

        auto range = TTableRange(state.Ranges.front().From.GetCells(), state.Ranges.front().FromInclusive,
                                 state.Ranges.back().To.GetCells(), state.Ranges.back().ToInclusive);

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
            << ", attempt #" << state.ResolveAttempt);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        // Avoid setting DomainOwnerId to reduce possible races with schemeshard migration
        // TODO: request->DatabaseName = ...;
        // TODO: request->UserToken = ...;
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
        if (!prev.has_value() || !ScanData || Shards.empty()) {
            return;
        }

        auto& state = Shards.front();

        const ui64 freeSpace = CalculateFreeSpace();
        const ui64 prevFreeSpace = std::any_cast<ui64>(prev);

        CA_LOG_T("Scan over tablet " << state.TabletId << " finished: " << ScanData->IsFinished()
            << ", prevFreeSpace: " << prevFreeSpace << ", freeSpace: " << freeSpace << ", peer: " << state.ActorId);

        if (!ScanData->IsFinished() && state.State != EShardState::PostRunning
            && prevFreeSpace < freeSpace && state.ActorId)
        {
            CA_LOG_T("[poll] Send EvScanDataAck to " << state.ActorId << ", gen: " << state.Generation
                << ", freeSpace: " << freeSpace);
            SendScanDataAck(state, freeSpace);
        }
    }

    void TerminateSources(const TIssues& issues, bool success) override {
        if (!ScanData || Shards.empty()) {
            return;
        }

        auto prio = success ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
        auto& state = Shards.front();
        if (state.ActorId) {
            CA_LOG(prio, "Send abort execution event to scan over tablet: " << state.TabletId << ", table: "
                << ScanData->TablePath << ", scan actor: " << state.ActorId << ", message: " << issues.ToOneLineString());

            Send(state.ActorId, new TEvKqp::TEvAbortExecution(
                success ? NYql::NDqProto::StatusIds::SUCCESS : NYql::NDqProto::StatusIds::ABORTED, issues));
        } else {
            CA_LOG(prio, "Table: " << ScanData->TablePath << ", scan has not been started yet");
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

    TString PrintLastKey() const {
        if (LastKey.empty()) {
            return "<none>";
        }
        return DebugPrintPoint(KeyColumnTypes, LastKey, *AppData()->TypeRegistry);
    }

private:
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    NKikimrKqp::TKqpSnapshot Snapshot;
    TIntrusivePtr<TKqpCounters> Counters;
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    TVector<NScheme::TTypeId> KeyColumnTypes;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    TOwnedCellVec LastKey;
    std::deque<std::pair<TEvKqpCompute::TEvScanData::TPtr, TInstant>> PendingScanData;
    std::deque<TShardState> Shards;
    ui32 LastGeneration = 0;
    std::set<ui64> AffectedShards;
    THashSet<ui32> TrackingNodes;
};

} // anonymous namespace

IActor* CreateKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
    NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpScanComputeActor(snapshot, executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, counters);
}

} // namespace NKqp
} // namespace NKikimr
