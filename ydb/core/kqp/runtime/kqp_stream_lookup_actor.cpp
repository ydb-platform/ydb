#include "kqp_stream_lookup_actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_worker.h>
#include <ydb/core/kqp/runtime/kqp_stream_lock_worker.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/data_events/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {
namespace NKqp {

namespace {

static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(10);
NActors::TActorId MainPipeCacheId = NKikimr::MakePipePerNodeCacheID(false);
NActors::TActorId FollowersPipeCacheId = NKikimr::MakePipePerNodeCacheID(true);

class TKqpStreamLookupActor : public NActors::TActorBootstrapped<TKqpStreamLookupActor>, public NYql::NDq::IDqComputeActorAsyncInput {
public:
    TKqpStreamLookupActor(NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args, NKikimrKqp::TKqpStreamLookupSettings&& settings,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "StreamLookupActor, inputIndex: " << args.InputIndex << ", CA Id " << args.ComputeActorId)
        , InputIndex(args.InputIndex)
        , Input(args.TransformInput)
        , ComputeActorId(args.ComputeActorId)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , Snapshot(settings.GetSnapshot().GetStep(), settings.GetSnapshot().GetTxId())
        , AllowInconsistentReads(settings.GetAllowInconsistentReads())
        , UseFollowers(settings.GetAllowUseFollowers())
        , IsTableImmutable(settings.GetIsTableImmutable())
        , HasVectorTopK(settings.HasVectorTopK())
        , PipeCacheId(UseFollowers ? FollowersPipeCacheId : MainPipeCacheId)
        , LockTxId(settings.HasLockTxId() ? settings.GetLockTxId() : TMaybe<ui64>())
        , NodeLockId(settings.HasLockNodeId() ? settings.GetLockNodeId() : TMaybe<ui32>())
        , LockMode(settings.HasLockMode() ? settings.GetLockMode() : TMaybe<NKikimrDataEvents::ELockMode>())
        , QuerySpanId(settings.HasQuerySpanId() ? settings.GetQuerySpanId() : 0)
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , LookupStrategy(settings.GetLookupStrategy())
        , IsolationLevel(settings.GetIsolationLevel())
        , Database(settings.GetDatabase())
        , MaxTotalBytesQuota(MaxTotalBytesQuotaStreamLookup())
        , MaxRowsProcessing(MaxRowsProcessingStreamLookup())
        , MaxInFlightReads(MaxInFlightReadsStreamLookup())
        , MaxBytesPerFetch(MaxBytesPerFetchStreamLookup())
        , Counters(counters)
        , LookupActorSpan(TWilsonKqp::LookupActor, std::move(args.TraceId), "LookupActor")
    {
        IngressStats.Level = args.StatsLevel;

        if (LookupStrategy == NKqpProto::EStreamLookupStrategy::LOOKUP_AND_LOCK) {
            AFL_ENSURE(IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_COMMITTED_RW);
            AFL_ENSURE(LockMode);
            AFL_ENSURE(*LockMode == NKikimrDataEvents::ELockMode::PESSIMISTIC_NONE);
            TKqpStreamLockSettings lockSettings(args.HolderFactory);
            lockSettings.Table = settings.GetTable();
            for (const auto& col : settings.GetKeyColumns()) {
                lockSettings.KeyColumns.push_back(col);
            }
            for (const auto& col : settings.GetColumns()) {
                lockSettings.Columns.push_back(col);
            }
            lockSettings.LockTxId = LockTxId ? *LockTxId : 0;
            lockSettings.LockNodeId = NodeLockId ? *NodeLockId : 0;
            lockSettings.LockMode = (IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_COMMITTED_RW)
                ? NKikimrDataEvents::ELockMode::PESSIMISTIC_EXCLUSIVE
                : *LockMode;
            lockSettings.Database = Database;
            lockSettings.Snapshot.SetStep(settings.GetSnapshot().GetStep());
            lockSettings.Snapshot.SetTxId(settings.GetSnapshot().GetTxId());
            lockSettings.QuerySpanId = QuerySpanId;

            StreamLockWorker = CreateStreamLockWorker(
                std::move(lockSettings)
            );
        }

        StreamLookupWorker = CreateStreamLookupWorker(
            std::move(settings),
            args.TaskId,
            args.TypeEnv,
            args.HolderFactory,
            args.InputDesc);
    }

    virtual ~TKqpStreamLookupActor() {
        if (Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();
            StreamLookupWorker.reset();
            StreamLockWorker.reset();
        }
    }

    void Bootstrap() {
        CA_LOG_D("Start stream lookup actor");

        Counters->StreamLookupActorsCount->Inc();
        ResolveTableShards();
        Become(&TKqpStreamLookupActor::StateFunc);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_STREAM_LOOKUP_ACTOR;
    }

    void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats , bool last, const NYql::NDq::TDqMeteringStats* mstats) override {
        if (last) {
            NYql::NDqProto::TDqTableStats* tableStats = nullptr;
            for (auto& table : *stats->MutableTables()) {
                if (table.GetTablePath() == StreamLookupWorker->GetTablePath()) {
                    tableStats = &table;
                }
            }

            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(StreamLookupWorker->GetTablePath());
            }

            ui64 rowsReadEstimate = ReadRowsCount;
            ui64 bytesReadEstimate = ReadBytesCount;

            if (mstats && !HasVectorTopK) {
                switch(LookupStrategy) {
                    case NKqpProto::EStreamLookupStrategy::LOOKUP:
                    case NKqpProto::EStreamLookupStrategy::UNIQUE:
                    case NKqpProto::EStreamLookupStrategy::LOOKUP_AND_LOCK: {
                        // in lookup case without top-K pushdown we return as result actual data, that we read from the datashard.
                        rowsReadEstimate = mstats->Inputs[InputIndex]->RowsConsumed;
                        bytesReadEstimate = mstats->Inputs[InputIndex]->BytesConsumed;
                        break;
                    }
                    default:
                        ;
                }
            }

            auto affectedShards = Reads.AffectedShards();
            // TODO: use evread statistics after KIKIMR-16924
            tableStats->SetReadRows(tableStats->GetReadRows() + rowsReadEstimate);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + bytesReadEstimate);
            tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + affectedShards.size());

            NKqpProto::TKqpTableExtraStats tableExtraStats;
            auto readActorTableAggrExtraStats = tableExtraStats.MutableReadActorTableAggrExtraStats();
            for (const auto& shardId : affectedShards) {
                readActorTableAggrExtraStats->AddAffectedShards(shardId);
            }

            tableStats->MutableExtra()->PackFrom(tableExtraStats);

            // Add lock stats for broken locks from stream lookup operations
            if (!BrokenLocks.empty()) {
                NKqpProto::TKqpTaskExtraStats extraStats;
                if (stats->HasExtra()) {
                    stats->GetExtra().UnpackTo(&extraStats);
                }
                extraStats.MutableLockStats()->SetBrokenAsVictim(
                    extraStats.GetLockStats().GetBrokenAsVictim() + BrokenLocks.size());
                stats->MutableExtra()->PackFrom(extraStats);
            }
        }
    }

private:
    enum class EReadState {
        Initial,
        Running,
        Blocked, // Read can't accept new data, but not finished yet
        Finished,
    };

    std::string_view ReadStateToString(EReadState state) {
        switch (state) {
            case EReadState::Initial: return "Initial"sv;
            case EReadState::Running: return "Running"sv;
            case EReadState::Blocked: return "Blocked"sv;
            case EReadState::Finished: return "Finished"sv;
        }
    }

    struct TReadState {
        TReadState(ui64 id, ui64 shardId)
            : Id(id)
            , ShardId(shardId)
            , State(EReadState::Initial) {}

        bool Finished() const {
            return (State == EReadState::Finished);
        }

        void SetBlocked() {
            State = EReadState::Blocked;
        }

        const ui64 Id;
        const ui64 ShardId;
        EReadState State;
        ui64 LastSeqNo = 0;
    };

    struct TLockState {
        enum class EState {
            Initial,
            Running,
            Blocked,
            Finished,
        };

        TLockState(ui64 id, ui64 shardId)
            : Id(id)
            , ShardId(shardId) {}

        bool Finished() const {
            return (State == EState::Finished);
        }

        void SetBlocked() {
            State = EState::Blocked;
        }

        const ui64 Id;
        const ui64 ShardId;
        EState State = EState::Initial;
    };

    struct TShardState {
        ui64 RetryAttempts = 0;
        std::unordered_set<ui64> Reads;
        std::unordered_set<ui64> Locks;
        bool HasPipe = false;
    };

    struct TReads {
        std::unordered_map<ui64, TReadState> Reads;
        std::unordered_map<ui64, TLockState> Locks;
        std::unordered_map<ui64, TShardState> ShardsState;

        std::unordered_map<ui64, TReadState>::iterator begin() { return Reads.begin(); }

        std::unordered_map<ui64, TReadState>::iterator end() { return Reads.end(); }

        std::unordered_map<ui64, TReadState>::iterator find(ui64 readId) {
            return Reads.find(readId);
        }

        std::unordered_map<ui64, TLockState>::iterator findLock(ui64 lockId) {
            return Locks.find(lockId);
        }

        std::unordered_map<ui64, TLockState>::iterator endLocks() { return Locks.end(); }

        void insertRead(ui64 readId, TReadState&& read) {
            const auto [readIt, succeeded] = Reads.insert({readId, std::move(read)});
            YQL_ENSURE(succeeded);
            ShardsState[readIt->second.ShardId].Reads.emplace(readIt->second.Id);
        }

        size_t InFlightReads() const {
            return Reads.size();
        }

        size_t InFlightLocks() const {
            return Locks.size();
        }

        std::vector<ui64> AffectedShards() const {
            std::vector<ui64> result;
            result.reserve(ShardsState.size());
            for(const auto& [shard, _]: ShardsState) {
                result.push_back(shard);
            }
            return result;
        }

        bool CheckShardRetriesExeeded(TReadState& failedRead) {
            return CheckShardRetriesExeededImpl(failedRead.ShardId);
        }

        bool CheckShardRetriesExeededImpl(ui64 shardId) {
            const auto& shardState = ShardsState[shardId];
            return shardState.RetryAttempts + 1 > MaxShardRetries();
        }

        bool CheckShardRetriesExeededLock(const TLockState& failedLock) {
            return CheckShardRetriesExeededImpl(failedLock.ShardId);
        }

        TDuration CalcDelayForShardImpl(ui64 shardId, bool allowInstantRetry) {
            auto& shardState = ShardsState[shardId];
            ++shardState.RetryAttempts;
            return CalcDelay(shardState.RetryAttempts, allowInstantRetry);
        }

        TDuration CalcDelayForShard(TReadState& failedRead, bool allowInstantRetry) {
            return CalcDelayForShardImpl(failedRead.ShardId, allowInstantRetry);
        }

        TDuration CalcDelayForShardLock(const TLockState& failedLock, bool allowInstantRetry) {
            return CalcDelayForShardImpl(failedLock.ShardId, allowInstantRetry);
        }

        void eraseRead(TReadState& read) {
            ShardsState[read.ShardId].Reads.erase(read.Id);
            Reads.erase(read.Id);
        }

        std::vector<TReadState*> GetShardReads(ui64 shardId) {
            auto it = ShardsState.find(shardId);
            std::vector<TReadState*> result;
            if (it == ShardsState.end()) {
                return result;
            }

            for(ui64 readId: it->second.Reads) {
                auto it = Reads.find(readId);
                YQL_ENSURE(it != Reads.end());
                result.push_back(&it->second);
            }

            return result;
        }

        bool NeedToCreatePipe(ui64 shardId) {
            return !ShardsState[shardId].HasPipe;
        }

        void SetPipeCreated(ui64 shardId) {
            ShardsState[shardId].HasPipe = true;
        }

        void SetPipeDestroyed(ui64 shardId) {
            ShardsState[shardId].HasPipe = false;
        }

        void insertLock(ui64 lockId, TLockState&& lock) {
            const auto [lockIt, succeeded] = Locks.insert({lockId, std::move(lock)});
            YQL_ENSURE(succeeded);
            ShardsState[lockIt->second.ShardId].Locks.emplace(lockIt->second.Id);
        }

        void eraseLock(const TLockState& lock) {
            ShardsState[lock.ShardId].Locks.erase(lock.Id);
            Locks.erase(lock.Id);
        }

        std::vector<TLockState*> GetShardLocks(ui64 shardId) {
            auto it = ShardsState.find(shardId);
            std::vector<TLockState*> result;
            if (it == ShardsState.end()) {
                return result;
            }

            for(ui64 lockId: it->second.Locks) {
                auto it = Locks.find(lockId);
                YQL_ENSURE(it != Locks.end());
                result.push_back(&it->second);
            }

            return result;
        }
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryRead = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvRetryLock,
            EvSchemeCacheRequestTimeout
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
        };

        struct TEvRetryRead : public TEventLocal<TEvRetryRead, EvRetryRead> {
            explicit TEvRetryRead(ui64 readId, ui64 lastSeqNo, bool instantStart = false)
                : ReadId(readId)
                , LastSeqNo(lastSeqNo)
                , InstantStart(instantStart) {
            }

            const ui64 ReadId;
            const ui64 LastSeqNo;
            const bool InstantStart;
        };

        struct TEvRetryLock : public TEventLocal<TEvRetryLock, EvRetryLock> {
            explicit TEvRetryLock(ui64 lockId, bool instantStart = false)
                : LockId(lockId)
                , InstantStart(instantStart) {
            }

            const ui64 LockId;
            const bool InstantStart;
        };
    };

private:
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) final {}
    void LoadState(const NYql::NDq::TSourceState&) final {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    void PassAway() final {
        Counters->StreamLookupActorsCount->Dec();
        {
            auto alloc = BindAllocator();
            Input.Clear();
            StreamLookupWorker.reset();
            StreamLockWorker.reset();
            for (auto& [id, state] : Reads) {
                Counters->SentIteratorCancels->Inc();
                auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
                cancel->Record.SetReadId(id);
                Send(PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
            }
        }

        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpStreamLookupActor>::PassAway();

        LookupActorSpan.End();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");
        SentResultsAvailable = false;

        if (ResolveShardsInProgress) {
            finished = false;
            return 0;
        }

        auto replyResultStats = StreamLookupWorker->ReplyResult(batch, freeSpace);
        ReadRowsCount += replyResultStats.ReadRowsCount;
        ReadBytesCount += replyResultStats.ReadBytesCount;

        while (!UnmodifiedOutputRows.empty() && (i64)replyResultStats.ResultBytesCount <= freeSpace) {
            auto row = std::move(UnmodifiedOutputRows.front());
            UnmodifiedOutputRows.pop_front();
            const i64 rowSize = 0; // TODO: calculate row size
            batch.push_back(std::move(row));
            replyResultStats.ResultRowsCount += 1;
            replyResultStats.ResultBytesCount += rowSize;
        }

        // Fetch input rows if we have less than max in flight reads in the scheduled queue.
        if (StreamLookupWorker->ScheduledRequestsCount() < MaxInFlightReads) {
            FetchInputRows();
        }

        if (Partitioning) {
            ProcessInputRows();
        }

        const bool inputRowsFinished = LastFetchStatus == NUdf::EFetchStatus::Finish;
        const bool allReadsFinished = AllReadsFinished();
        const bool allRowsProcessed = StreamLookupWorker->AllRowsProcessed();
        const bool hasPendingResults = StreamLookupWorker->HasPendingResults();

        // If we have no new reads and no pending results, we can fetch input rows again.
        bool noNewReads = (
            Partitioning && Reads.InFlightReads() + StreamLookupWorker->ScheduledRequestsCount() == 0
            && LastFetchStatus == NUdf::EFetchStatus::Ok);
        if (hasPendingResults || noNewReads) {
            // has more results
            if (!SentResultsAvailable) {
                Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
                SentResultsAvailable = true;
            }
        }

        finished = inputRowsFinished && allReadsFinished && allRowsProcessed;

        CA_LOG_D("Returned " << replyResultStats.ResultBytesCount << " bytes, " << replyResultStats.ResultRowsCount
            << " rows, finished: " << finished);

        return replyResultStats.ResultBytesCount;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
        for (auto& lock : Locks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }

        for (auto& lock : BrokenLocks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }

        if (DeferredVictimQuerySpanId) {
            resultInfo.SetDeferredVictimQuerySpanId(DeferredVictimQuerySpanId);
        }

        result.PackFrom(resultInfo);
        return result;
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvSchemeCacheRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvRetryRead, Handle);
                hFunc(TEvPrivate::TEvRetryLock, Handle);
                hFunc(NEvents::TDataEvents::TEvLockRowsResult, Handle);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    RuntimeError(TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite(),
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            RuntimeError("Memory limit exceeded at stream lookup", NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        CA_LOG_D("TEvResolveKeySetResult was received for table: " << StreamLookupWorker->GetTablePath());
        if (!ResolveShardsInProgress) {
            return;
        }
        ResolveShardsInProgress = false;
        if (ev->Get()->Request->ErrorCount > 0) {
            TString errorMsg = TStringBuilder() << "Failed to get partitioning for table: "
                << StreamLookupWorker->GetTablePath();
            LookupActorStateSpan.EndError(errorMsg);

            return RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        if (IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SNAPSHOT_RO) {
            YQL_ENSURE(!LockTxId, "SnapshotReadOnly should not take locks");
        }
        if (LockTxId && IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SNAPSHOT_RW && LookupStrategy == NKqpProto::EStreamLookupStrategy::UNIQUE) {
            YQL_ENSURE(LockMode == NKikimrDataEvents::OPTIMISTIC);
        } else if (LockTxId && IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SNAPSHOT_RW) {
            YQL_ENSURE(LockMode == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
        } else if (LockTxId && IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SERIALIZABLE) {
            YQL_ENSURE(LockMode == NKikimrDataEvents::OPTIMISTIC);
        } else if (LockTxId && IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_COMMITTED_RW) {
            YQL_ENSURE(LockMode == NKikimrDataEvents::PESSIMISTIC_NONE);
        }
        LookupActorStateSpan.EndOk();

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessInputRows();

        if (!SentResultsAvailable) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
            SentResultsAvailable = true;
        }
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto readIt = Reads.find(record.GetReadId());
        if (readIt == Reads.end() || readIt->second.State != EReadState::Running) {
            CA_LOG_D("Drop read with readId: " << record.GetReadId() << ", because it's already completed or blocked");
            return;
        }

        auto& read = readIt->second;
        ui64 shardId = read.ShardId;

        CA_LOG_D("Recv TEvReadResult (stream lookup) from ShardID=" << read.ShardId
            << ", Table = " << StreamLookupWorker->GetTablePath()
            << ", ReadId=" << record.GetReadId() << " (current OperationId=" << OperationId << ")"
            << ", SeqNo=" << record.GetSeqNo()
            << ", Status=" << Ydb::StatusIds::StatusCode_Name(record.GetStatus().GetCode())
            << ", Finished=" << record.GetFinished()
            << ", RowCount=" << record.GetRowCount()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", BrokenTxLocks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetBrokenTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        for (auto& lock : record.GetBrokenTxLocks()) {
            BrokenLocks.push_back(lock);
        }

        for (auto& lock : record.GetTxLocks()) {
            Locks.push_back(lock);
        }

        if (record.HasDeferredVictimQuerySpanId() && DeferredVictimQuerySpanId == 0) {
            DeferredVictimQuerySpanId = record.GetDeferredVictimQuerySpanId();
        }

        if (UseFollowers) {
            YQL_ENSURE(Locks.empty());
            if (!record.GetFinished() && !IsTableImmutable) {
                RuntimeError("read from follower returned partial data.", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                return;
            }
        }

        TotalBytesQuota -= MaxBytesDefaultQuota;
        Counters->StreamLookupIteratorTotalQuotaBytesInFlight->Sub(MaxBytesDefaultQuota);

        if (!Snapshot.IsValid() && LookupStrategy != NKqpProto::EStreamLookupStrategy::LOOKUP_AND_LOCK) {
            Snapshot = IKqpGateway::TKqpSnapshot(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
        }

        Counters->DataShardIteratorMessages->Inc();
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Counters->DataShardIteratorFails->Inc();
        }

        auto getIssues = [&record]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetStatus().GetIssues(), issues);
            return issues;
        };

        auto replyError = [&](auto message, auto status) {
            return RuntimeError(message, status, getIssues());
        };

        switch (record.GetStatus().GetCode()) {
            case Ydb::StatusIds::SUCCESS:
                break;
            case Ydb::StatusIds::NOT_FOUND:
            {
                StreamLookupWorker->ResetRowsProcessing(read.Id);
                CA_LOG_D("NOT_FOUND was received from tablet: " << read.ShardId << ". "
                    << getIssues().ToOneLineString());
                Reads.eraseRead(read);
                return ResolveTableShards();
            }
            case Ydb::StatusIds::OVERLOADED: {
                const bool isThrottled = record.HasThrottled() && record.GetThrottled();
                if (!isThrottled && (CheckTotalRetriesExeeded() || Reads.CheckShardRetriesExeeded(read))) {
                    return replyError(
                        TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' retry limit exceeded.",
                        NYql::NDqProto::StatusIds::OVERLOADED);
                }
                CA_LOG_D("OVERLOADED was received from tablet: " << read.ShardId << "."
                    << getIssues().ToOneLineString());
                read.SetBlocked();
                return RetryTableRead(read, /*allowInstantRetry = */false, isThrottled);
            }
            case Ydb::StatusIds::INTERNAL_ERROR: {
                if (CheckTotalRetriesExeeded() || Reads.CheckShardRetriesExeeded(read)) {
                    return replyError(
                        TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' retry limit exceeded.",
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                }
                CA_LOG_D("INTERNAL_ERROR was received from tablet: " << read.ShardId << "."
                    << getIssues().ToOneLineString());
                read.SetBlocked();
                return RetryTableRead(read);
            }
            default: {
                return replyError("Read request aborted", NYql::NDqProto::StatusIds::ABORTED);
            }
        }


        YQL_ENSURE(read.LastSeqNo < record.GetSeqNo());
        read.LastSeqNo = record.GetSeqNo();

        if (record.GetFinished()) {
            Reads.eraseRead(read);
        } else {
            Counters->SentIteratorAcks->Inc();
            THolder<TEvDataShard::TEvReadAck> request(new TEvDataShard::TEvReadAck());
            request->Record.SetReadId(record.GetReadId());
            request->Record.SetSeqNo(record.GetSeqNo());

            auto defaultSettings = GetDefaultReadAckSettings()->Record;
            request->Record.SetMaxRows(MaxRowsDefaultQuota);
            request->Record.SetMaxBytes(MaxBytesDefaultQuota);

            TotalBytesQuota += MaxBytesDefaultQuota;
            Counters->StreamLookupIteratorTotalQuotaBytesInFlight->Add(MaxBytesDefaultQuota);
            if (TotalBytesQuota > MaxTotalBytesQuota) {
                Counters->StreamLookupIteratorTotalQuotaBytesExceeded->Inc();
            }

            const bool needToCreatePipe = Reads.NeedToCreatePipe(read.ShardId);

            Send(PipeCacheId,
                new TEvPipeCache::TEvForward(
                    request.Release(), read.ShardId, TEvPipeCache::TEvForwardOptions{
                        .AutoConnect = needToCreatePipe,
                        .Subscribe = needToCreatePipe,
                    }),
                IEventHandle::FlagTrackDelivery);

            Reads.SetPipeCreated(read.ShardId);

            CA_LOG_D("TEvReadAck was sent to shard: " << read.ShardId);

            if (auto delay = ShardTimeout()) {
                TlsActivationContext->Schedule(
                    *delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(read.Id, read.LastSeqNo))
                );
            }
        }

        auto guard = BindAllocator();
        StreamLookupWorker->AddResult(TStreamLookupShardReadResult(
            shardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release()),
            &guard.GetMutex()->Ref()
        ));
        if (!SentResultsAvailable) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
            SentResultsAvailable = true;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        const auto& tabletId = ev->Get()->TabletId;

        Reads.SetPipeDestroyed(tabletId);

        TVector<TReadState*> toRetryReads;
        for (auto* read : Reads.GetShardReads(tabletId)) {
            if (read->State == EReadState::Running) {
                Counters->IteratorDeliveryProblems->Inc();
                toRetryReads.push_back(read);
            }
        }
        for (auto* read : toRetryReads) {
            read->SetBlocked();
            RetryTableRead(*read);
        }

        TVector<TLockState*> toRetryLocks;
        for (auto* lock : Reads.GetShardLocks(tabletId)) {
            if (lock->State == TLockState::EState::Running) {
                toRetryLocks.push_back(lock);
            }
        }
        for (auto* lock : toRetryLocks) {
            lock->SetBlocked();
            RetryLock(*lock);
        }
    }

    void Handle(TEvPrivate::TEvSchemeCacheRequestTimeout::TPtr&) {
        CA_LOG_D("TEvSchemeCacheRequestTimeout was received, shards for table " << StreamLookupWorker->GetTablePath()
            << " was resolved: " << !!Partitioning);

        if (!Partitioning) {
            LookupActorStateSpan.EndError("timeout exceeded");
            CA_LOG_D("Retry attempt to resolve shards for table: " << StreamLookupWorker->GetTablePath());
            ResolveShardsInProgress = false;
            ResolveTableShards();
        }
    }

    void Handle(TEvPrivate::TEvRetryRead::TPtr& ev) {
        auto readIt = Reads.find(ev->Get()->ReadId);
        if (readIt == Reads.end()) {
            CA_LOG_D("received retry request for already finished/non-existing read, read_id: " << ev->Get()->ReadId);
            return;
        }

        auto& read = readIt->second;

        YQL_ENSURE(read.State != EReadState::Blocked || read.LastSeqNo <= ev->Get()->LastSeqNo);

        if ((read.State == EReadState::Running && read.LastSeqNo <= ev->Get()->LastSeqNo) || read.State == EReadState::Blocked) {
            if (ev->Get()->InstantStart) {
                auto guard = BindAllocator();
                StreamLookupWorker->RebuildRequest(read.ShardId, read.Id, ReadId);
                Reads.eraseRead(read);
                ScheduleNextReads();
            } else {
                RetryTableRead(read);
            }
        }
    }

    void Handle(TEvPrivate::TEvRetryLock::TPtr& ev) {
        auto lockIt = Reads.findLock(ev->Get()->LockId);
        if (lockIt == Reads.endLocks()) {
            CA_LOG_D("received retry request for already finished/non-existing lock, lock_id: " << ev->Get()->LockId);
            return;
        }

        auto& lock = lockIt->second;

        if (lock.State == TLockState::EState::Running || lock.State == TLockState::EState::Blocked) {
            if (ev->Get()->InstantStart) {
                auto guard = BindAllocator();
                auto requests = StreamLockWorker->RebuildLockRequest(lock.Id, OperationId);
                for (auto& [shardId, request] : requests) {
                    SendLockRequest(shardId, std::move(request));
                }
                Reads.eraseLock(lock);
            } else {
                RetryLock(lock);
            }
        }
    }

    void FetchInputRows() {
        auto guard = BindAllocator();

        NUdf::TUnboxedValue row;
        auto allocState = &guard.GetMutex()->Ref();

        YQL_ENSURE(!Input.IsInvalid());
        if (Input.IsFinish() || !Input.HasValue()) {
            LastFetchStatus = NUdf::EFetchStatus::Finish;
            return;
        }

        size_t fetchCount = 0;
        i64 bytesBefore = allocState->GetAllocated();
        while ((LastFetchStatus = Input.Fetch(row)) == NUdf::EFetchStatus::Ok) {
            if (StreamLockWorker) {
                AFL_ENSURE(LockMode && (*LockMode == NKikimrDataEvents::ELockMode::PESSIMISTIC_EXCLUSIVE
                    || *LockMode == NKikimrDataEvents::ELockMode::PESSIMISTIC_NONE));
                StreamLockWorker->AddInputRow(std::move(row));
            } else {
                StreamLookupWorker->AddInputRow(std::move(row));
                ++fetchCount;
                // Avoid fetching too many rows at once: limit both the number of rows and
                // the allocator growth since the start of this fetch loop. GetAllocated()
                // is only a heuristic for memory pressure here, not a precise retained-memory metric.
                if (fetchCount >= MaxRowsProcessing || static_cast<i64>(allocState->GetAllocated()) - bytesBefore > static_cast<i64>(MaxBytesPerFetch)) {
                    break;
                }
            }
        }
    }

    void ProcessInputRows() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        auto guard = BindAllocator();

        StreamLookupWorker->BuildRequests(Partitioning, ReadId);
        ScheduleNextReads();
    }

    void ScheduleNextReads() {
        while(Reads.InFlightReads() < MaxInFlightReads) {
            auto [shardId, request] = StreamLookupWorker->PopNextRequest();
            if (!request) {
                break;
            }

            StartTableRead(shardId, std::move(request));
        }

        if (StreamLockWorker) {
            AFL_ENSURE(LockMode && (*LockMode == NKikimrDataEvents::ELockMode::PESSIMISTIC_EXCLUSIVE
                || (*LockMode == NKikimrDataEvents::ELockMode::PESSIMISTIC_NONE // TODO: delete
                    && IsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_COMMITTED_RW)));
            auto lockRequests = StreamLockWorker->BuildLockRequests(Partitioning, OperationId);
            for (auto& [shardId, request] : lockRequests) {
                SendLockRequest(shardId, std::move(request));
            }
        }
    }

    void SendLockRequest(ui64 shardId, THolder<NEvents::TDataEvents::TEvLockRows> request) {
        CA_LOG_D("Send lock request to shard: " << shardId);

        ui64 requestId = request->Record.GetRequestId();

        const bool needToCreatePipe = Reads.NeedToCreatePipe(shardId);

        Send(PipeCacheId,
            new TEvPipeCache::TEvForward(
                request.Release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery);

        Reads.SetPipeCreated(shardId);
        auto lockState = TLockState(requestId, shardId);
        lockState.State = TLockState::EState::Running;
        Reads.insertLock(requestId, std::move(lockState));
    }

    void Handle(NEvents::TDataEvents::TEvLockRowsResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        CA_LOG_D("Received lock result, requestId: " << record.GetRequestId() 
            << ", status: " << record.GetStatus());

        auto getIssues = [&record]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            return issues;
        };

        switch (record.GetStatus()) {
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS:
                break;
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN: {
                CA_LOG_D("STATUS_LOCKS_BROKEN from shard: " << record.GetTabletId());
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. Locks Invalidated.",
                    NYql::NDqProto::StatusIds::ABORTED,
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED: {
                CA_LOG_D("STATUS_OVERLOADED from shard: " << record.GetTabletId());
                auto lockIt = Reads.findLock(record.GetRequestId());
                AFL_ENSURE(lockIt != Reads.endLocks());
                return RetryLock(lockIt->second, false);
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK: {
                CA_LOG_D("STATUS_DEADLOCK from shard: " << record.GetTabletId());
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Deadlock detected",
                    NYql::NDqProto::StatusIds::ABORTED,
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_ERROR:
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_CHANGED: {
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Scheme error",
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR: {
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Internal error",
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST: {
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Bad request",
                    NYql::NDqProto::StatusIds::BAD_REQUEST,
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_WRONG_SHARD_STATE: {
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Wrong shard state.",
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    getIssues());
            }
            default: {
                return RuntimeError(
                    TStringBuilder() << "Table: `" << StreamLookupWorker->GetTablePath() << "`. " << "Lock request aborted",
                    NYql::NDqProto::StatusIds::ABORTED,
                    getIssues());
            }
        }

        for (const auto& lock : record.GetLocks()) {
            AFL_ENSURE(lock.GetCounter() != NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken
                    && lock.GetCounter() != NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken);
            Locks.push_back(lock);
        }

        ui64 requestId = record.GetRequestId();
        StreamLockWorker->AddLockResult(requestId, ev->Get());

        bool hasModifiedRows = false;
        bool hasUnmodifiedRows = false;
        StreamLockWorker->ProcessRowsByLockResult(requestId, 
            [&](NUdf::TUnboxedValue row, bool modified) {
                if (modified) {
                    StreamLookupWorker->AddInputRow(std::move(row));
                    hasModifiedRows = true;
                    AFL_ENSURE(false);
                } else {
                    UnmodifiedOutputRows.emplace_back(std::move(row));
                    hasUnmodifiedRows = true;
                }
            });

        if (hasModifiedRows) {
            ProcessInputRows();
        }
        
        if (hasUnmodifiedRows) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }

        auto lockIt = Reads.findLock(requestId);
        if (lockIt != Reads.endLocks()) {
            Reads.eraseLock(lockIt->second);
        }
    }

    void StartTableRead(ui64 shardId, THolder<TEvDataShard::TEvRead> request) {
        Counters->CreatedIterators->Inc();
        auto& record = request->Record;

        CA_LOG_D("Start reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << record.GetReadId()
            << ", shardId: " << shardId);

        TReadState read(record.GetReadId(), shardId);

        if (Snapshot.IsValid() && LookupStrategy != NKqpProto::EStreamLookupStrategy::LOOKUP_AND_LOCK) {
            record.MutableSnapshot()->SetStep(Snapshot.Step);
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
        } else {
            YQL_ENSURE(AllowInconsistentReads, "Expected valid snapshot or enabled inconsistent read mode");
        }

        if (LockTxId && BrokenLocks.empty()) {
            record.SetLockTxId(*LockTxId);
            if (LockMode) {
                record.SetLockMode(*LockMode);
            }
        }

        if (NodeLockId) {
            record.SetLockNodeId(*NodeLockId);
        }

        if (QuerySpanId) {
            record.SetQuerySpanId(QuerySpanId);
        }

        auto defaultSettings = GetDefaultReadSettings()->Record;
        if (!MaxRowsDefaultQuota || !MaxBytesDefaultQuota) {
            MaxRowsDefaultQuota = defaultSettings.GetMaxRows();
            MaxBytesDefaultQuota = defaultSettings.GetMaxBytes();
        }

        record.SetMaxRows(MaxRowsDefaultQuota);
        record.SetMaxBytes(MaxBytesDefaultQuota);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        TotalBytesQuota += MaxBytesDefaultQuota;
        Counters->StreamLookupIteratorTotalQuotaBytesInFlight->Add(MaxBytesDefaultQuota);

        if (TotalBytesQuota > MaxTotalBytesQuota) {
            Counters->StreamLookupIteratorTotalQuotaBytesExceeded->Inc();
        }

        CA_LOG_D(TStringBuilder() << "Send EvRead (stream lookup) to shardId=" << shardId
            << ", readId = " << record.GetReadId()
            << ", tablePath: " << StreamLookupWorker->GetTablePath()
            << ", snapshot=(txid=" << record.GetSnapshot().GetTxId() << ", step=" << record.GetSnapshot().GetStep() << ")"
            << ", lockTxId=" << record.GetLockTxId()
            << ", lockNodeId=" << record.GetLockNodeId());

        const bool needToCreatePipe = Reads.NeedToCreatePipe(read.ShardId);

        Send(PipeCacheId,
            new TEvPipeCache::TEvForward(
                request.Release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery,
            0,
            LookupActorSpan.GetTraceId());

        Reads.SetPipeCreated(read.ShardId);

        read.State = EReadState::Running;

        auto readId = read.Id;
        auto lastSeqNo = read.LastSeqNo;
        Reads.insertRead(readId, std::move(read));

        if (auto delay = ShardTimeout()) {
            TlsActivationContext->Schedule(
                *delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(readId, lastSeqNo))
            );
        }
    }

    bool CheckTotalRetriesExeeded() {
        const auto limit = MaxTotalRetries();
        return limit && TotalRetryAttempts + 1 > *limit;
    }

    void RetryTableRead(TReadState& failedRead, bool allowInstantRetry = true, bool isThrottled = false) {
        CA_LOG_D("Retry reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << failedRead.Id
            << ", shardId: " << failedRead.ShardId);

        if (!isThrottled) {
            if (CheckTotalRetriesExeeded()) {
                return RuntimeError(TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' retry limit exceeded",
                    NYql::NDqProto::StatusIds::UNAVAILABLE);
            }
            ++TotalRetryAttempts;

            if (Reads.CheckShardRetriesExeeded(failedRead)) {
                StreamLookupWorker->ResetRowsProcessing(failedRead.Id);
                Reads.eraseRead(failedRead);
                return ResolveTableShards();
            }
        }

        auto delay = Reads.CalcDelayForShard(failedRead, allowInstantRetry);
        if (delay == TDuration::Zero()) {
            auto guard = BindAllocator();
            StreamLookupWorker->RebuildRequest(failedRead.ShardId, failedRead.Id, ReadId);
            Reads.eraseRead(failedRead);
            ScheduleNextReads();
        } else {
            CA_LOG_D("Schedule retry atempt for readId: " << failedRead.Id << " after " << delay);
            TlsActivationContext->Schedule(
                delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(failedRead.Id, failedRead.LastSeqNo, /*instantStart = */ true))
            );
        }
    }

    void RetryLock(TLockState& failedLock, bool allowInstantRetry = true) {
        CA_LOG_D("Retry locking for shard: " << failedLock.ShardId << ", lockId: " << failedLock.Id);

        if (CheckTotalRetriesExeeded()) {
            return RuntimeError(TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' lock retry limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }
        ++TotalRetryAttempts;

        if (Reads.CheckShardRetriesExeededLock(failedLock)) {
            Reads.eraseLock(failedLock);
            return ResolveTableShards();
        }

        auto delay = Reads.CalcDelayForShardLock(failedLock, allowInstantRetry);
        if (delay == TDuration::Zero()) {
            auto guard = BindAllocator();
            auto requests = StreamLockWorker->RebuildLockRequest(failedLock.Id, OperationId);
            for (auto& [shardId, request] : requests) {
                SendLockRequest(shardId, std::move(request));
            }
            Reads.eraseLock(failedLock);
        } else {
            CA_LOG_D("Schedule retry for lockId: " << failedLock.Id << " after " << delay);
            TlsActivationContext->Schedule(
                delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryLock(failedLock.Id, /*instantStart = */ true))
            );
        }
    }

    void ResolveTableShards() {
        if (ResolveShardsInProgress) {
            return;
        }

        if (++TotalResolveShardsAttempts > MaxShardResolves()) {
            return RuntimeError(TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' resolve attempts limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }

        CA_LOG_D("Resolve shards for table: " << StreamLookupWorker->GetTablePath());
        ResolveShardsInProgress = true;

        Partitioning.reset();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;

        auto keyColumnTypes = StreamLookupWorker->GetKeyColumnTypes();

        TVector<TCell> minusInf(keyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(StreamLookupWorker->GetTableId(), range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Counters->IteratorsShardResolve->Inc();
        LookupActorStateSpan = NWilson::TSpan(TWilsonKqp::LookupActorShardsResolve, LookupActorSpan.GetTraceId(),
            "WaitForShardsResolve", NWilson::EFlags::AUTO_END);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout()));
    }

    bool AllReadsFinished() const {
        if (StreamLockWorker && Reads.InFlightLocks() > 0) {
            return false;
        }
        return Reads.InFlightReads() == 0;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        if (LookupActorSpan) {
            LookupActorSpan.EndError(issues.ToOneLineString());
        }

        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

private:
    const TString LogPrefix;
    const ui64 InputIndex;
    NYql::NDq::TDqAsyncStats IngressStats;
    NUdf::TUnboxedValue Input;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    IKqpGateway::TKqpSnapshot Snapshot;
    const bool AllowInconsistentReads;
    const bool UseFollowers;
    const bool IsTableImmutable;
    const bool HasVectorTopK;
    const TActorId PipeCacheId;
    const TMaybe<ui64> LockTxId;
    const TMaybe<ui32> NodeLockId;
    const TMaybe<NKikimrDataEvents::ELockMode> LockMode;
    const ui64 QuerySpanId;
    TReads Reads;
    bool SentResultsAvailable = false;
    NUdf::EFetchStatus LastFetchStatus = NUdf::EFetchStatus::Yield;
    TPartitioning::TCPtr Partitioning;
    const TDuration SchemeCacheRequestTimeout;
    NActors::TActorId SchemeCacheRequestTimeoutTimer;
    TVector<NKikimrDataEvents::TLock> Locks;
    TVector<NKikimrDataEvents::TLock> BrokenLocks;
    ui64 DeferredVictimQuerySpanId = 0;
    NKqpProto::EStreamLookupStrategy LookupStrategy;
    std::unique_ptr<TKqpStreamLookupWorker> StreamLookupWorker;
    std::unique_ptr<TKqpStreamLockWorker> StreamLockWorker;
    std::deque<NUdf::TUnboxedValue> UnmodifiedOutputRows;
    ui64 OperationId = 0;
    size_t TotalRetryAttempts = 0;
    size_t TotalResolveShardsAttempts = 0;
    bool ResolveShardsInProgress = false;
    NKqpProto::EIsolationLevel IsolationLevel;
    const TString Database;

    // stats
    ui64 ReadRowsCount = 0;
    ui64 ReadBytesCount = 0;

    size_t TotalBytesQuota = 0;
    ui64 MaxTotalBytesQuota = 0;
    size_t MaxRowsProcessing = 0;
    ui64 MaxInFlightReads = 50;
    ui64 MaxBytesPerFetch = 256_MB;
    size_t MaxBytesDefaultQuota = 0;
    size_t MaxRowsDefaultQuota = 0;

    TIntrusivePtr<TKqpCounters> Counters;
    NWilson::TSpan LookupActorSpan;
    NWilson::TSpan LookupActorStateSpan;
};

} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateStreamLookupActor(NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args,
    NKikimrKqp::TKqpStreamLookupSettings&& settings, TIntrusivePtr<TKqpCounters> counters) {
    auto actor = new TKqpStreamLookupActor(std::move(args), std::move(settings), counters);
    return {actor, actor};
}

void InterceptStreamLookupActorPipeCache(NActors::TActorId id) {
    MainPipeCacheId = id;
}

} // namespace NKqp
} // namespace NKikimr
