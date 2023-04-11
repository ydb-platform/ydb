#include "kqp_compute_actor.h"
#include "kqp_compute_actor_impl.h"
#include "kqp_compute_state.h"
#include "kqp_scan_compute_manager.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_channel_storage.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/kqp/runtime/kqp_scan_data_meta.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/core/tx/datashard/datashard_kqp_compute.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/wilson/wilson_profile_span.h>

#include <util/generic/deque.h>

namespace NKikimr::NKqp {

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

    TScannedDataStats() = default;

    void AddReadStat(const ui32 scannerIdx, const ui64 rows, const ui64 bytes) {
        auto [it, success] = ReadShardInfo.emplace(scannerIdx, std::make_pair(rows, bytes));
        if (!success) {
            auto& [currentRows, currentBytes] = it->second;
            currentRows += rows;
            currentBytes += bytes;
        }
    }

    void CompleteShard(TShardState::TPtr state) {
        auto it = ReadShardInfo.find(state->ScannerIdx);
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

struct TEvScanExchange {

    enum EEvents {
        EvSendData = EventSpaceBegin(TKikimrEvents::ES_KQP_SCAN_EXCHANGE),
        EvAckData,
        EvTerminateFromFetcher,
        EvTerminateFromCompute,
        EvRegisterFetcher,
        EvFetcherFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_KQP_SCAN_EXCHANGE), "expected EvEnd < EventSpaceEnd");

    class TEvRegisterFetcher: public NActors::TEventLocal<TEvRegisterFetcher, EvRegisterFetcher> {
    public:
    };

    class TEvFetcherFinished: public NActors::TEventLocal<TEvFetcherFinished, EvFetcherFinished> {
    public:
    };

    class TEvSendData: public NActors::TEventLocal<TEvSendData, EvSendData> {
    private:
        YDB_ACCESSOR_DEF(std::shared_ptr<arrow::RecordBatch>, ArrowBatch);
        YDB_ACCESSOR_DEF(TVector<TOwnedCellVec>, Rows);
        YDB_ACCESSOR(ui64, TabletId, 0);
    public:
        TEvSendData(TEvKqpCompute::TEvScanData& msg, const ui64 tabletId)
            : TabletId(tabletId) {
            switch (msg.GetDataFormat()) {
                case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
                case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED:
                    Rows = std::move(msg.Rows);
                    Y_VERIFY(Rows.size());
                    break;
                case NKikimrTxDataShard::EScanDataFormat::ARROW:
                    ArrowBatch = msg.ArrowBatch;
                    Y_VERIFY(ArrowBatch);
                    Y_VERIFY(ArrowBatch->num_rows());
                    break;
            }

        }

        TEvSendData(std::shared_ptr<arrow::RecordBatch> arrowBatch, const ui64 tabletId)
            : ArrowBatch(arrowBatch)
            , TabletId(tabletId) {
            Y_VERIFY(ArrowBatch->num_rows());
        }

        TEvSendData(TVector<TOwnedCellVec>&& rows, const ui64 tabletId)
            : Rows(std::move(rows))
            , TabletId(tabletId) {
            Y_VERIFY(Rows.size());
        }
    };

    class TEvAckData: public NActors::TEventLocal<TEvAckData, EvAckData> {
    private:
        YDB_READONLY(ui32, FreeSpace, 0)
    public:
        TEvAckData(const ui32 freeSpace)
            : FreeSpace(freeSpace)
        {

        }
    };

    class TEvTerminateFromFetcher: public NActors::TEventLocal<TEvTerminateFromFetcher, EvTerminateFromFetcher> {
    private:
        YDB_READONLY(NDqProto::EComputeState, State, NDqProto::COMPUTE_STATE_FAILURE);
        YDB_READONLY(NYql::NDqProto::StatusIds::StatusCode, StatusCode, NYql::NDqProto::StatusIds::UNSPECIFIED);
        YDB_READONLY_DEF(TIssues, Issues);
    public:
        TEvTerminateFromFetcher(TIssuesIds::EIssueCode issueCode, const TString& message) {
            TIssue issue(message);
            SetIssueCode(issueCode, issue);
            Issues = { issue };
            StatusCode = NYql::NDqProto::StatusIds::PRECONDITION_FAILED;
        }

        TEvTerminateFromFetcher(NYql::NDqProto::StatusIds::StatusCode statusCode, TIssuesIds::EIssueCode issueCode, const TString& message) {
            TIssue issue(message);
            SetIssueCode(issueCode, issue);
            Issues = { issue };
            StatusCode = statusCode;
        }

        TEvTerminateFromFetcher(const NDqProto::EComputeState state, const NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues)
            : State(state)
            , StatusCode(statusCode)
            , Issues(issues)
        {

        }
    };

    class TEvTerminateFromCompute: public NActors::TEventLocal<TEvTerminateFromCompute, EvTerminateFromCompute> {
    private:
        YDB_READONLY_FLAG(Success, false);
        YDB_READONLY_DEF(TIssues, Issues);
    public:
        TEvTerminateFromCompute(const bool success, const TIssues& issues)
            : SuccessFlag(success)
            , Issues(issues)
        {

        }
    };
};

class TKqpScanComputeActor: public TDqComputeActorBase<TKqpScanComputeActor> {
private:
    using TBase = TDqComputeActorBase<TKqpScanComputeActor>;
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    using TBase::TaskRunner;
    using TBase::MemoryLimits;
    using TBase::GetStatsMode;
    using TBase::TxId;
    using TBase::GetTask;
    using TBase::RuntimeSettings;
    using TBase::ContinueExecute;
    std::set<NActors::TActorId> Fetchers;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    ui64 CalcMkqlMemoryLimit() override {
        return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ChannelBufferSize;
    }
public:
    TKqpScanComputeActor(const TActorId& executerId, ui64 txId,
        NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings,
            memoryLimits, /* ownMemoryQuota = */ true, /* passExceptions = */ true, /*taskCounters = */ nullptr, std::move(traceId))
        , ComputeCtx(settings.StatsMode)
    {
        YQL_ENSURE(GetTask().GetMeta().UnpackTo(&Meta), "Invalid task meta: " << GetTask().GetMeta().DebugString());
        YQL_ENSURE(!Meta.GetReads().empty());
        YQL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvScanExchange::TEvSendData, Handle);
                hFunc(TEvScanExchange::TEvRegisterFetcher, Handle);
                hFunc(TEvScanExchange::TEvFetcherFinished, Handle);
                hFunc(TEvScanExchange::TEvTerminateFromFetcher, Handle)
                default:
                    BaseStateFuncBody(ev, ctx);
            }
        } catch (const TMemoryLimitExceededException& e) {
            const TString sInfo = TStringBuilder() << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName() << ", canAllocateExtraMemory: " << CanAllocateExtraMemory;
            CA_LOG_E("ERROR:" + sInfo);
            InternalError(NYql::NDqProto::StatusIds::PRECONDITION_FAILED, TIssuesIds::KIKIMR_PRECONDITION_FAILED, sInfo);
        } catch (const yexception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, e.what());
        }

        TBase::ReportEventElapsedTime();
    }

    void ProcessRlNoResourceAndDie() {
        const NYql::TIssue issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_RESOURCE_USAGE_LIMITED,
            "Throughput limit exceeded for query");
        CA_LOG_E("Throughput limit exceeded stream will be terminated");

        State = NDqProto::COMPUTE_STATE_FAILURE;
        ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::OVERLOADED, TIssues({ issue }));
    }

    bool IsQuotingEnabled() const {
        const auto& rlPath = RuntimeSettings.RlPath;
        return rlPath.Defined();
    }

    void AcquireRateQuota() {
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

    void FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last) {
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

    void HandleEvWakeup(EEvWakeupTag tag) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "HandleEvWakeup for " << SelfId();
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

    void Handle(TEvScanExchange::TEvTerminateFromFetcher::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvTerminateFromFetcher: " << ev->Sender;
        TBase::InternalError(ev->Get()->GetStatusCode(), ev->Get()->GetIssues());
        State = ev->Get()->GetState();
    }

    void Handle(TEvScanExchange::TEvSendData::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvSendData: " << ev->Sender;
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

    void Handle(TEvScanExchange::TEvRegisterFetcher::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvRegisterFetcher: " << ev->Sender;
        Y_VERIFY(Fetchers.emplace(ev->Sender).second);
        Send(ev->Sender, new TEvScanExchange::TEvAckData(CalculateFreeSpace()));
    }

    void Handle(TEvScanExchange::TEvFetcherFinished::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvFetcherFinished: " << ev->Sender;
        Y_VERIFY(Fetchers.erase(ev->Sender) == 1);
        if (Fetchers.size() == 0) {
            ScanData->Finish();
            DoExecute();
        }
    }

    ui64 CalculateFreeSpace() const {
        return GetMemoryLimits().ChannelBufferSize > ScanData->GetStoredBytes()
            ? GetMemoryLimits().ChannelBufferSize - ScanData->GetStoredBytes()
            : 0ul;
    }

    std::any GetSourcesState() override {
        if (!ScanData) {
            return 0;
        }
        return CalculateFreeSpace();
    }

    void PollSources(std::any prev) override {
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

    void PassAway() override {
        if (TaskRunner) {
            if (TaskRunner->IsAllocatorAttached()) {
                ComputeCtx.Clear();
            } else {
                auto guard = TaskRunner->BindAllocator(TBase::GetMkqlMemoryLimit());
                ComputeCtx.Clear();
            }
        }

        TBase::PassAway();
    }

    void TerminateSources(const TIssues& issues, bool success) override {
        if (!ScanData) {
            return;
        }

        for (auto&& i : Fetchers) {
            Send(i, new TEvScanExchange::TEvTerminateFromCompute(success, issues));
        }
    }

    void DoBootstrap() {
        CA_LOG_D("EVLOGKQP START");
        NDq::TDqTaskRunnerContext execCtx;
        execCtx.FuncRegistry = AppData()->FunctionRegistry;
        execCtx.ComputeCtx = &ComputeCtx;
        execCtx.ComputationFactory = GetKqpActorComputeFactory(&ComputeCtx);
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

};

class TKqpScanFetcherActor: public TActorBootstrapped<TKqpScanFetcherActor> {
private:
    using TBase = TActorBootstrapped<TKqpScanFetcherActor>;
    struct TEvPrivate {
        enum EEv {
            EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryShard: public TEventLocal<TEvRetryShard, EvRetryShard> {
        private:
            explicit TEvRetryShard(const ui64 tabletId)
                : TabletId(tabletId)
                , IsCostsRequest(true) {
            }
        public:
            ui64 TabletId = 0;
            ui32 Generation = 0;
            bool IsCostsRequest = false;

            static THolder<TEvRetryShard> CostsProblem(const ui64 tabletId) {
                return THolder<TEvRetryShard>(new TEvRetryShard(tabletId));
            }

            TEvRetryShard(const ui64 tabletId, const ui32 generation)
                : TabletId(tabletId)
                , Generation(generation) {
            }
        };
    };
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    const NMiniKQL::TScanDataMetaFull ScanDataMeta;
    const TComputeRuntimeSettings RuntimeSettings;
    const TTxId TxId;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_COMPUTE_ACTOR;
    }

    TKqpScanFetcherActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TComputeRuntimeSettings& settings, std::vector<NActors::TActorId>&& computeActors, const ui64 txId,
        const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta, const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId)
        : Meta(meta)
        , ScanDataMeta(Meta)
        , RuntimeSettings(settings)
        , TxId(txId)
        , ComputeActorIds(std::move(computeActors))
        , Snapshot(snapshot)
        , ShardsScanningPolicy(shardsScanningPolicy)
        , Counters(counters)
        , KqpComputeActorSpan(NKikimr::TWilsonKqp::ComputeActor, std::move(traceId), "KqpScanActor")
        , InFlightShards(ShardsScanningPolicy, KqpComputeActorSpan)
    {
        KqpComputeActorSpan.SetEnabled(IS_DEBUG_LOG_ENABLED(NKikimrServices::KQP_COMPUTE) || KqpComputeActorSpan.GetTraceId());
        YQL_ENSURE(!Meta.GetReads().empty());
        YQL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "META:" << meta.DebugString();
        KeyColumnTypes.reserve(Meta.GetKeyColumnTypes().size());
        for (size_t i = 0; i < Meta.KeyColumnTypesSize(); i++) {
            auto typeId = Meta.GetKeyColumnTypes().at(i);
            KeyColumnTypes.push_back(NScheme::TTypeInfo(
                (NScheme::TTypeId)typeId,
                (typeId == NScheme::NTypeIds::Pg) ?
                    NPg::TypeDescFromPgTypeId(
                        Meta.GetKeyColumnTypeInfos().at(i).GetPgTypeId()
                    ) : nullptr
            ));
        }
    }

    void Bootstrap() {
        auto gTime = KqpComputeActorSpan.StartStackTimeGuard("bootstrap");
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ". ";
        CA_LOG_D("EVLOGKQP START");

        ShardsScanningPolicy.FillRequestScanFeatures(Meta, MaxInFlight, IsAggregationRequest);
        for (const auto& read : Meta.GetReads()) {
            auto& state = PendingShards.emplace_back(TShardState(read.GetShardId(), ++ScansCounter));
            state.Ranges = TShardCostsState::BuildSerializedTableRanges(read);
        }
        for (auto&& c: ComputeActorIds) {
            Sender<TEvScanExchange::TEvRegisterFetcher>().SendTo(c);
        }
        StartTableScan();
        Become(&TKqpScanFetcherActor::StateFunc);
    }

    STATEFN(StateFunc) {
        auto gTimeFull = KqpComputeActorSpan.StartStackTimeGuard("processing");
        auto gTime = KqpComputeActorSpan.StartStackTimeGuard("event_" + ::ToString(ev->GetTypeRewrite()));
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
                hFunc(TEvScanExchange::TEvTerminateFromCompute, HandleExecute);
                hFunc(TEvScanExchange::TEvAckData, HandleExecute);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    Y_FAIL("unexpected message");
            }
        } catch (const yexception& e) {
            Y_FAIL("UNEXPECTED EXCEPTION: %s", e.what());
        }
    }

    void HandleExecute(TEvScanExchange::TEvAckData::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): " << ev->Sender;
        if (!InFlightComputes.OnComputeAck(ev->Sender, ev->Get()->GetFreeSpace())) {
            ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): " << ev->Sender << " IGNORED";
            return;
        }
        DoAckAvailableWaiting();
    }

    void HandleExecute(TEvScanExchange::TEvTerminateFromCompute::TPtr& ev) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "TEvTerminateFromCompute: " << ev->Sender;
        for (auto&& itTablet : InFlightShards) {
            for (auto&& it : itTablet.second) {
                auto state = it.second;
                TStringBuilder sb;
                if (state->ActorId) {
                    sb << "Send abort execution event to scan over tablet: " << state->TabletId <<
                        ", scan actor: " << state->ActorId << ", message: " << ev->Get()->GetIssues().ToOneLineString();
                    Send(state->ActorId, new TEvKqp::TEvAbortExecution(
                        ev->Get()->IsSuccess() ? NYql::NDqProto::StatusIds::SUCCESS : NYql::NDqProto::StatusIds::ABORTED, ev->Get()->GetIssues()));
                } else {
                    sb << "Tablet: " << state->TabletId << ", scan has not been started yet";
                }
                if (ev->Get()->IsSuccess()) {
                    CA_LOG_D(sb);
                } else {
                    CA_LOG_W(sb);
                }
            }
        }
    }

private:

    std::vector<NActors::TActorId> ComputeActorIds;

    bool SendGlobalFail(const NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssuesIds::EIssueCode issueCode, const TString& message) {
        for (auto&& i : ComputeActorIds) {
            Send(i, new TEvScanExchange::TEvTerminateFromFetcher(statusCode, issueCode, message));
        }
        return true;
    }

    bool SendGlobalFail(const NDqProto::EComputeState state, NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues) {
        for (auto&& i : ComputeActorIds) {
            Send(i, new TEvScanExchange::TEvTerminateFromFetcher(state, statusCode, issues));
        }
        return true;
    }

    bool ProvideDataToCompute(TEvKqpCompute::TEvScanData& msg, TShardState::TPtr state) {
        if (msg.IsEmpty()) {
            InFlightComputes.OnEmptyDataReceived(state->TabletId);
        } else {
            ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "PROVIDING (FROM " << SelfId() << "): used free compute " << InFlightComputes.DebugString();
            auto computeActorInfo = InFlightComputes.OnDataReceived(state->TabletId, msg.RequestedBytesLimitReached);
            Send(computeActorInfo.GetActorId(), new TEvScanExchange::TEvSendData(msg, state->TabletId));
        }
        return true;
    }

    bool SendScanFinished() {
        for (auto&& i : ComputeActorIds) {
            Sender<TEvScanExchange::TEvFetcherFinished>().SendTo(i);
        }
        return true;
    }

    THolder<TEvDataShard::TEvKqpScan> BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const {
        auto ev = MakeHolder<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(ScanDataMeta.TableId.PathId.LocalPathId);
        for (auto& column: ScanDataMeta.GetColumns()) {
            ev->Record.AddColumnTags(column.Tag);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
            ev->Record.AddColumnTypes(columnType.TypeId);
            if (columnType.TypeInfo) {
                *ev->Record.AddColumnTypeInfos() = *columnType.TypeInfo;
            } else {
                *ev->Record.AddColumnTypeInfos() = NKikimrProto::TTypeInfo();
            }
        }
        ev->Record.MutableSkipNullKeys()->CopyFrom(Meta.GetSkipNullKeys());

        auto protoRanges = ev->Record.MutableRanges();
        protoRanges->Reserve(ranges.size());

        for (auto& range : ranges) {
            auto newRange = protoRanges->Add();
            range.Serialize(*newRange);
        }

        ev->Record.MutableSnapshot()->CopyFrom(Snapshot);
        if (RuntimeSettings.Timeout) {
            ev->Record.SetTimeoutMs(RuntimeSettings.Timeout.Get()->MilliSeconds());
        }
        ev->Record.SetStatsMode(RuntimeSettings.StatsMode);
        ev->Record.SetScanId(scanId);
        ev->Record.SetTxId(std::get<ui64>(TxId));
        ev->Record.SetTablePath(ScanDataMeta.TablePath);
        ev->Record.SetSchemaVersion(ScanDataMeta.TableId.SchemaVersion);

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
        return ev;
    }

    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        auto& msg = ev->Get()->Record;
        auto scanActorId = ActorIdFromProto(msg.GetScanActorId());
        auto state = GetShardState(msg, scanActorId);
        if (!state)
            return;

        CA_LOG_D("Got EvScanInitActor from " << scanActorId << ", gen: " << msg.GetGeneration()
            << ", state: " << state->State << ", stateGen: " << state->Generation
            << ", tabletId: " << state->TabletId);

        YQL_ENSURE(state->Generation == msg.GetGeneration());

        if (state->State == EShardState::Starting) {
            state->State = EShardState::Running;
            state->ActorId = scanActorId;
            state->ResetRetry();

            InFlightShards.NeedAck(state);
            SendScanDataAck(state);
        } else {
            TerminateExpiredScan(scanActorId, "Got unexpected/expired EvScanInitActor, terminate it");
        }
    }

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        auto& msg = *ev->Get();
        auto state = GetShardState(msg, ev->Sender);
        if (!state) {
            return;
        }
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

        ProcessScanData();
    }

    void ProcessPendingScanDataItem(TEvKqpCompute::TEvScanData::TPtr& ev, const TInstant& enqueuedAt) {
        auto& msg = *ev->Get();

        auto state = GetShardState(msg, ev->Sender);
        if (!state) {
            return;
        }

        TDuration latency;
        if (enqueuedAt != TInstant::Zero()) {
            latency = TActivationContext::Now() - enqueuedAt;
            Counters->ScanQueryRateLimitLatency->Collect(latency.MilliSeconds());
        }

        YQL_ENSURE(state->ActorId == ev->Sender, "expected: " << state->ActorId << ", got: " << ev->Sender);

        state->LastKey = std::move(msg.LastKey);
        const ui64 rowsCount = msg.GetRowsCount();
        CA_LOG_D("action=got EvScanData;rows=" << rowsCount << ";finished=" << msg.Finished
            << ";from=" << ev->Sender << ";shards remain=" << PendingShards.size()
            << ";in flight scans=" << InFlightShards.GetScansCount()
            << ";in flight shards=" << InFlightShards.GetShardsCount()
            << ";delayed_for=" << latency.SecondsFloat() << " seconds by ratelimiter"
            << ";tabletId=" << state->TabletId);
        if (!msg.Finished || NKqp::ETableKind(Meta.GetTable().GetTableKind()) == NKqp::ETableKind::Datashard) {
            ProvideDataToCompute(msg, state);
        }
        InFlightShards.MutableStatistics(state->TabletId).AddPack(rowsCount, 0);

        Stats.AddReadStat(state->ScannerIdx, rowsCount, 0);

        bool stopFinally = false;
        CA_LOG_T("EVLOGKQP:" << IsAggregationRequest << "/" << Meta.GetItemsLimit() << "/" << InFlightShards.GetTotalRowsCount() << "/" << rowsCount);
        if (!msg.Finished) {
            if (msg.RequestedBytesLimitReached) {
                InFlightShards.NeedAck(state);
                SendScanDataAck(state);
            }
        } else {
            CA_LOG_D("Chunk " << state->TabletId << "/" << state->ScannerIdx << " scan finished");
            Stats.CompleteShard(state);
            StopReadChunk(*state);
            CA_LOG_T("TRACE:" << InFlightShards.TraceToString());
            stopFinally = !StartTableScan();
        }
        if (stopFinally) {
            SendScanFinished();
            InFlightShards.Stop();
            CA_LOG_D("EVLOGKQP(scans_count:" << ScansCounter << ";max_in_flight:" << MaxInFlight << ")"
                << Endl << InFlightShards.GetDurationStats()
                << Endl << InFlightShards.StatisticsToString()
                << KqpComputeActorSpan.ProfileToString()
            );
            PassAway();
        }

        CA_LOG_T("TRACE:" << InFlightShards.TraceToString() << ":" << rowsCount);

    }

    void ProcessScanData() {
        YQL_ENSURE(!PendingScanData.empty());

        auto ev = std::move(PendingScanData.front().first);
        auto enqueuedAt = std::move(PendingScanData.front().second);
        PendingScanData.pop_front();

        auto& msg = *ev->Get();
        auto state = GetShardState(msg, ev->Sender);
        if (!state)
            return;

        if (state->State == EShardState::Running || state->State == EShardState::PostRunning) {
            ProcessPendingScanDataItem(ev, enqueuedAt);
        } else {
            TerminateExpiredScan(ev->Sender, "Cancel expired scan");
        }
    }

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        auto& msg = ev->Get()->Record;

        Ydb::StatusIds::StatusCode status = msg.GetStatus();
        TIssues issues;
        IssuesFromMessage(msg.GetIssues(), issues);

        auto state = GetShardState(msg, TActorId());
        if (!state)
            return;
        InFlightComputes.OnScanError(state->TabletId);

        CA_LOG_W("Got EvScanError scan state: " << state->State
            << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
            << ", reason: " << issues.ToString()
            << ", tablet id: " << state->TabletId);

        YQL_ENSURE(state->Generation == msg.GetGeneration());


        if (state->State == EShardState::Starting) {
            if (FindSchemeErrorInIssues(status, issues)) {
                return EnqueueResolveShard(state);
            }
            SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, YdbStatusToDqStatus(status), issues);
            return PassAway();
        }

        if (state->State == EShardState::PostRunning || state->State == EShardState::Running) {
            state->State = EShardState::Initial;
            state->ActorId = {};
            InFlightShards.ClearAckState(state);
            state->ResetRetry();
            ++TotalRetries;
            return StartReadShard(state);
        }
    }

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        auto& msg = *ev->Get();

        auto* states = InFlightShards.MutableByTabletId(msg.TabletId);
        if (!states) {
            CA_LOG_E("Broken pipe with unknown tablet " << msg.TabletId);
            return;
        }
        InFlightComputes.OnScanError(msg.TabletId);

        for (auto& [_, state] : *states) {
            const auto shardState = state->State;
            CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered << ", " << shardState);
            if (state->State == EShardState::Starting || state->State == EShardState::Running) {
                return RetryDeliveryProblem(state);
            }
        }
    }

    void HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        const ui32 scannerIdx = InFlightShards.GetIndexByGeneration(ev->Get()->Generation);
        auto state = InFlightShards.GetStateByIndex(scannerIdx);
        if (!state) {
            CA_LOG_E("Received retry shard for unexpected tablet " << ev->Get()->TabletId << " / " << ev->Get()->Generation);
            return;
        }

        SendStartScanRequest(state, state->Generation);
    }

    void HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        if (!InFlightShards.IsActive()) {
            return;
        }
        YQL_ENSURE(!PendingResolveShards.empty());
        auto state = std::move(PendingResolveShards.front());
        PendingResolveShards.pop_front();
        ResolveNextShard();

        Y_VERIFY(!InFlightShards.GetStateByIndex(state.ScannerIdx));

        YQL_ENSURE(state.State == EShardState::Resolving);
        CA_LOG_D("Received TEvResolveKeySetResult update for table '" << ScanDataMeta.TablePath << "'");

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            CA_LOG_E("Resolve request failed for table '" << ScanDataMeta.TablePath << "', ErrorCount# " << request->ErrorCount);

            auto statusCode = NDqProto::StatusIds::UNAVAILABLE;
            auto issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
            TString error;

            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32)NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    // invalidate table
                    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanDataMeta.TableId, {}));

                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                            error = TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' not exists.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                            error = TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' scheme changed.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::LookupError:
                            statusCode = NDqProto::StatusIds::UNAVAILABLE;
                            issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
                            error = TStringBuilder() << "Failed to resolve table '" << ScanDataMeta.TablePath << "'.";
                            break;
                        default:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                            error = TStringBuilder() << "Unresolved table '" << ScanDataMeta.TablePath << "'. Status: " << x.Status;
                            break;
                    }
                }
            }

            SendGlobalFail(statusCode, issueCode, error);
            return;
        }

        auto keyDesc = std::move(request->ResultSet[0].KeyDescription);

        if (keyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << ScanDataMeta.TablePath << "'";
            CA_LOG_E(error);
            SendGlobalFail(NDqProto::StatusIds::SCHEME_ERROR, TIssuesIds::KIKIMR_SCHEME_MISMATCH, error);
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

            auto newShard = TShardState(partition.ShardId, ++ScansCounter);

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
            && PendingShards.size() + InFlightShards.GetScansCount() > 0) {
            TStringBuilder sb;
            if (!PendingShards.empty()) {
                sb << "Pending shards States: ";
                for (auto& st : PendingShards) {
                    sb << st.ToString(KeyColumnTypes) << "; ";
                }
            }

            if (!InFlightShards.empty()) {
                sb << "In Flight shards States: ";
                for (auto& [_, st] : InFlightShards) {
                    for (auto&& [_, i] : st) {
                        sb << i->ToString(KeyColumnTypes) << "; ";
                    }
                }
            }
            CA_LOG_D(sb);
        }
        StartTableScan();
    }

    void HandleExecute(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvDataShard::TEvKqpScan::EventType:
                // Handled by TEvPipeCache::TEvDeliveryProblem event.
                // CostData request is KqpScan request too.
                return;
            case TEvKqpCompute::TEvScanDataAck::EventType:
                ui64 tabletId = ev->Cookie;
                const auto& shards = InFlightShards.GetByTabletId(tabletId);
                if (shards.empty()) {
                    CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", " << tabletId);
                    return;
                }

                for (auto& [_, state] : shards) {
                    const auto actorId = state->ActorId;
                    if (state->State == EShardState::Running && ev->Sender == actorId) {
                        CA_LOG_E("TEvScanDataAck lost while running scan, terminate execution. DataShard actor: " << actorId);
                        SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                            "Delivery problem: EvScanDataAck lost.");
                    } else {
                        CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", active scan actor: " << actorId);
                    }
                }
                return;
        }
        Y_FAIL("UNEXPECTED EVENT TYPE");
    }

    void HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        auto nodeId = ev->Get()->NodeId;
        CA_LOG_N("Disconnected node " << nodeId);

        TrackingNodes.erase(nodeId);
        for (auto& [tabletId, states] : InFlightShards) {
            for (auto&& [_, state] : states) {
                if (state->ActorId && state->ActorId.NodeId() == nodeId) {
                    SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                        TStringBuilder() << "Connection with node " << nodeId << " lost.");
                }
            }
        }
    }

private:

    bool StartTableScan() {
        const ui32 maxAllowedInFlight = MaxInFlight;
        bool isFirst = true;
        while (!PendingShards.empty() && InFlightShards.GetScansCount() + PendingResolveShards.size() + 1 <= maxAllowedInFlight) {
            if (isFirst) {
                CA_LOG_D("BEFORE: " << PendingShards.size() << "." << InFlightShards.GetScansCount() << "." << PendingResolveShards.size());
                isFirst = false;
            }
            auto state = InFlightShards.Put(std::move(PendingShards.front()));
            PendingShards.pop_front();
            StartReadShard(state);
        }
        if (!isFirst) {
            CA_LOG_D("AFTER: " << PendingShards.size() << "." << InFlightShards.GetScansCount() << "." << PendingResolveShards.size());
        }

        CA_LOG_D("Scheduled table scans, in flight: " << InFlightShards.GetScansCount() << " shards. "
            << "pending shards to read: " << PendingShards.size() << ", "
            << "pending resolve shards: " << PendingResolveShards.size() << ", "
            << "average read rows: " << Stats.AverageReadRows() << ", "
            << "average read bytes: " << Stats.AverageReadBytes() << ", ");

        return InFlightShards.GetScansCount() + PendingShards.size() + PendingResolveShards.size() > 0;
    }

    void StartReadShard(TShardState::TPtr state) {
        YQL_ENSURE(state->State == EShardState::Initial);
        state->State = EShardState::Starting;
        state->Generation = InFlightShards.AllocateGeneration(state);
        state->ActorId = {};
        SendStartScanRequest(state, state->Generation);
    }

    bool SendScanDataAck(TShardState::TPtr state) {
        ui64 freeSpace;
        if (!InFlightComputes.PrepareShardAck(state, freeSpace)) {
            CA_LOG_D("Send EvScanDataAck denied: no free actors: " << InFlightComputes.DebugString());
            return false;
        } else {
            CA_LOG_D("Send EvScanDataAck allow: has free actors: " << InFlightComputes.DebugString());
        }
        CA_LOG_D("Send EvScanDataAck to " << state->ActorId << ", gen: " << state->Generation);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (TrackingNodes.insert(state->ActorId.NodeId()).second) {
            flags |= IEventHandle::FlagSubscribeOnSession;
        }
        Send(state->ActorId, new TEvKqpCompute::TEvScanDataAck(freeSpace, state->Generation), flags, state->TabletId);
        InFlightShards.AckSent(state);
        return true;
    }

    void SendStartScanRequest(TShardState::TPtr state, ui32 gen) {
        YQL_ENSURE(state->State == EShardState::Starting);

        auto ranges = state->GetScanRanges(KeyColumnTypes);
        CA_LOG_D("Start scan request, " << state->ToString(KeyColumnTypes));
        THolder<TEvDataShard::TEvKqpScan> ev = BuildEvKqpScan(0, gen, ranges);

        bool subscribed = std::exchange(state->SubscribedOnTablet, true);

        CA_LOG_D("Send EvKqpScan to shardId: " << state->TabletId << ", tablePath: " << ScanDataMeta.TablePath
            << ", gen: " << gen << ", subscribe: " << (!subscribed)
            << ", range: " << DebugPrintRanges(KeyColumnTypes, ranges, *AppData()->TypeRegistry));

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), state->TabletId, !subscribed),
            IEventHandle::FlagTrackDelivery);
    }

    void RetryDeliveryProblem(TShardState::TPtr state) {
        Counters->ScanQueryShardDisconnect->Inc();

        if (state->TotalRetries >= MAX_TOTAL_SHARD_RETRIES) {
            CA_LOG_E("TKqpScanFetcherActor: broken pipe with tablet " << state->TabletId
                << ", retries limit exceeded (" << state->TotalRetries << ")");
            SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Retries limit with shard " << state->TabletId << " exceeded.");
            return;
        }

        // note: it might be possible that shard is already removed after successful split/merge operation and cannot be found
        // in this case the next TEvKqpScan request will receive the delivery problem response.
        // so after several consecutive delivery problem responses retry logic should
        // resolve shard details again.
        if (state->RetryAttempt >= MAX_SHARD_RETRIES) {
            return ResolveShard(*state);
        }

        ++TotalRetries;

        InFlightShards.ClearAckState(state);
        state->RetryAttempt++;
        state->TotalRetries++;
        state->Generation = InFlightShards.AllocateGeneration(state);
        state->ActorId = {};
        state->State = EShardState::Starting;
        state->SubscribedOnTablet = false;
        auto retryDelay = state->CalcRetryDelay();
        CA_LOG_W("TKqpScanFetcherActor: broken pipe with tablet " << state->TabletId
            << ", restarting scan from last received key " << state->PrintLastKey(KeyColumnTypes)
            << ", attempt #" << state->RetryAttempt << " (total " << state->TotalRetries << ")"
            << " schedule after " << retryDelay);

        state->RetryTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), retryDelay,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryShard(state->TabletId, state->Generation)));
    }

    void TerminateExpiredScan(const TActorId& actorId, TStringBuf msg) {
        CA_LOG_W(msg);

        auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Cancel unexpected/expired scan");
        Send(actorId, abortEv.Release());
    }

    void TerminateChunk(TShardState::TPtr sState) {
        if (!sState->ActorId) {
            return;
        }
        auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Cancel non actual scan");
        Send(sState->ActorId, abortEv.Release());
    }

    void ResolveNextShard() {
        if (!PendingResolveShards.empty()) {
            auto& state = PendingResolveShards.front();
            ResolveShard(state);
        }
    }

    void EnqueueResolveShard(TShardState::TPtr state) {
        CA_LOG_D("Enqueue for resolve " << state->TabletId << " chunk " << state->ScannerIdx);
        YQL_ENSURE(StopReadChunk(*state));
        PendingResolveShards.emplace_back(*state);
        if (PendingResolveShards.size() == 1) {
            ResolveNextShard();
        }
    }

    void DoAckAvailableWaiting() {
        std::optional<TInFlightComputes::TWaitingShard> ev;
        if (InFlightComputes.ExtractWaitingForProvide(ev)) {
            if (!ev) {
                ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): no waiting events";
            } else {
                Y_VERIFY(SendScanDataAck(ev->GetShardState()));
            }
        } else {
            ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): no available computes for waiting events usage";
        }
    }

    bool StopReadChunk(const TShardState& state) {
        CA_LOG_D("Unlink from tablet " << state.TabletId << " chunk " << state.ScannerIdx << " and stop reading from it.");
        const ui64 tabletId = state.TabletId;
        const ui32 scannerIdx = state.ScannerIdx;

        if (InFlightComputes.StopReadShard(state.TabletId)) {
            DoAckAvailableWaiting();
        }

        Y_VERIFY(InFlightShards.RemoveIfExists(scannerIdx));

        const size_t remainChunksCount = InFlightShards.GetByTabletId(tabletId).size();
        if (remainChunksCount == 0) {
            CA_LOG_D("Unlink fully for tablet " << state.TabletId);
            Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(tabletId));
        } else {
            CA_LOG_D("Tablet " << state.TabletId << " not ready for unlink. Ramained chunks count: " << remainChunksCount);
        }
        return true;
    }

    void ResolveShard(TShardState& state) {
        if (state.ResolveAttempt >= MAX_SHARD_RESOLVES) {
            SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' resolve limit exceeded");
            return;
        }

        Counters->ScanQueryShardResolve->Inc();

        state.State = EShardState::Resolving;
        state.ResolveAttempt++;
        state.SubscribedOnTablet = false;

        auto range = TTableRange(state.Ranges.front().From.GetCells(), state.Ranges.front().FromInclusive,
            state.Ranges.back().To.GetCells(), state.Ranges.back().ToInclusive);

        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(ScanDataMeta.GetColumns().size());
        for (const auto& column : ScanDataMeta.GetColumns()) {
            TKeyDesc::TColumnOp op;
            op.Column = column.Tag;
            op.Operation = TKeyDesc::EColumnOperation::Read;
            op.ExpectedType = column.Type;
            columns.emplace_back(std::move(op));
        }

        auto keyDesc = MakeHolder<TKeyDesc>(ScanDataMeta.TableId, range, TKeyDesc::ERowOperation::Read,
            KeyColumnTypes, columns);

        CA_LOG_D("Sending TEvResolveKeySet update for table '" << ScanDataMeta.TablePath << "'"
            << ", range: " << DebugPrintRange(KeyColumnTypes, range, *AppData()->TypeRegistry)
            << ", attempt #" << state.ResolveAttempt);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(std::move(keyDesc));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanDataMeta.TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

private:
    void PassAway() override {
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        for (ui32 nodeId : TrackingNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }

        TBase::PassAway();
    }

    template<class TMessage>
    TShardState::TPtr GetShardState(const TMessage& msg, const TActorId& scanActorId) {
        if (!InFlightShards.IsActive()) {
            return nullptr;
        }
        ui32 generation;
        if constexpr (std::is_same_v<TMessage, NKikimrKqp::TEvScanError>) {
            generation = msg.GetGeneration();
        } else if constexpr (std::is_same_v<TMessage, NKikimrKqp::TEvScanInitActor>) {
            generation = msg.GetGeneration();
        } else {
            generation = msg.Generation;
        }

        const ui32 scannerIdx = InFlightShards.GetIndexByGeneration(generation);
        YQL_ENSURE(scannerIdx, "Received message from unknown scan or request. Generation: " << generation);

        TShardState::TPtr statePtr = InFlightShards.GetStateByIndex(scannerIdx);
        if (!statePtr) {
            TString error = TStringBuilder() << "Received message from scan shard which is not currently in flight, scannerIdx " << scannerIdx;
            CA_LOG_W(error);
            if (scanActorId) {
                TerminateExpiredScan(scanActorId, error);
            }

            return nullptr;
        }

        auto& state = *statePtr;
        if (state.Generation != generation) {
            TString error = TStringBuilder() << "Received message from expired scan, generation mistmatch, "
                << "expected: " << state.Generation << ", received: " << generation;
            CA_LOG_W(error);
            if (scanActorId) {
                TerminateExpiredScan(scanActorId, error);
            }

            return nullptr;
        }

        return statePtr;
    }

private:
    TString LogPrefix;
    NKikimrKqp::TKqpSnapshot Snapshot;
    TShardsScanningPolicy ShardsScanningPolicy;
    TIntrusivePtr<TKqpCounters> Counters;
    TScannedDataStats Stats;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    std::deque<std::pair<TEvKqpCompute::TEvScanData::TPtr, TInstant>> PendingScanData;
    std::deque<TShardState> PendingShards;
    std::deque<TShardState> PendingResolveShards;

    NWilson::TProfileSpan KqpComputeActorSpan;
    TInFlightShards InFlightShards;
    TInFlightComputes InFlightComputes;
    ui32 ScansCounter = 0;
    ui32 TotalRetries = 0;

    std::set<ui32> TrackingNodes;
    ui32 MaxInFlight = 1024;
    bool IsAggregationRequest = false;
};

} // anonymous namespace

IActor* CreateKqpScanComputeActor(const TActorId& executerId, ui64 txId,
    NDqProto::TDqTask&& task, IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId)
{
    return new TKqpScanComputeActor(executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, std::move(traceId));
}

IActor* CreateKqpScanFetcher(const NKikimrKqp::TKqpSnapshot& snapshot, std::vector<NActors::TActorId>&& computeActors,
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const TComputeRuntimeSettings& settings,
    const ui64 txId, const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId)
{
    return new TKqpScanFetcherActor(snapshot, settings, std::move(computeActors), txId, meta, shardsScanningPolicy, counters, std::move(traceId));
}

}
