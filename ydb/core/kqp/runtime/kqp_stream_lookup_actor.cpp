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
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

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
        , StreamLookupWorker(CreateStreamLookupWorker(std::move(settings), args.TaskId, args.TypeEnv, args.HolderFactory, args.InputDesc))
        , IsolationLevel(settings.GetIsolationLevel())
        , Database(settings.GetDatabase())
        , MaxTotalBytesQuota(MaxTotalBytesQuotaStreamLookup())
        , MaxRowsProcessing(MaxRowsProcessingStreamLookup())
        , Counters(counters)
        , LookupActorSpan(TWilsonKqp::LookupActor, std::move(args.TraceId), "LookupActor")
    {
        IngressStats.Level = args.StatsLevel;
    }

    virtual ~TKqpStreamLookupActor() {
        if (Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();
            StreamLookupWorker.reset();
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
                    case NKqpProto::EStreamLookupStrategy::UNIQUE: {
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

    struct TShardState {
        ui64 RetryAttempts = 0;
        std::unordered_set<ui64> Reads;
        bool HasPipe = false;
    };

    struct TReads {
        std::unordered_map<ui64, TReadState> Reads;
        std::unordered_map<ui64, TShardState> ReadsPerShard;

        std::unordered_map<ui64, TReadState>::iterator begin() { return Reads.begin(); }

        std::unordered_map<ui64, TReadState>::iterator end() { return Reads.end(); }

        std::unordered_map<ui64, TReadState>::iterator find(ui64 readId) {
            return Reads.find(readId);
        }

        void insert(ui64 readId, TReadState&& read) {
            const auto [readIt, succeeded] = Reads.insert({readId, std::move(read)});
            YQL_ENSURE(succeeded);
            ReadsPerShard[readIt->second.ShardId].Reads.emplace(readIt->second.Id);
        }

        size_t InFlightReads() const {
            return Reads.size();
        }

        std::vector<ui64> AffectedShards() const {
            std::vector<ui64> result;
            result.reserve(ReadsPerShard.size());
            for(const auto& [shard, _]: ReadsPerShard) {
                result.push_back(shard);
            }
            return result;
        }

        bool CheckShardRetriesExeeded(TReadState& failedRead) {
            const auto& shardState = ReadsPerShard[failedRead.ShardId];
            return shardState.RetryAttempts + 1 > MaxShardRetries();
        }

        TDuration CalcDelayForShard(TReadState& failedRead, bool allowInstantRetry) {
            auto& shardState = ReadsPerShard[failedRead.ShardId];
            ++shardState.RetryAttempts;
            return CalcDelay(shardState.RetryAttempts, allowInstantRetry);
        }

        void erase(TReadState& read) {
            ReadsPerShard[read.ShardId].Reads.erase(read.Id);
            Reads.erase(read.Id);
        }

        std::vector<TReadState*> GetShardReads(ui64 shardId) {
            auto it = ReadsPerShard.find(shardId);
            std::vector<TReadState*> result;
            if (it == ReadsPerShard.end()) {
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
            return !ReadsPerShard[shardId].HasPipe;
        }

        void SetPipeCreated(ui64 shardId) {
            ReadsPerShard[shardId].HasPipe = true;
        }

        void SetPipeDestroyed(ui64 shardId) {
            ReadsPerShard[shardId].HasPipe = false;
        }
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryRead = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
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

        if (ResolveShardsInProgress) {
            finished = false;
            return 0;
        }

        auto replyResultStats = StreamLookupWorker->ReplyResult(batch, freeSpace);
        ReadRowsCount += replyResultStats.ReadRowsCount;
        ReadBytesCount += replyResultStats.ReadBytesCount;

        auto overloaded = StreamLookupWorker->IsOverloaded(MaxRowsProcessing);
        if (!overloaded.has_value()) {
            FetchInputRows();
        } else {
            CA_LOG_N("Pausing stream lookup because it's overloaded by reason: "
                << overloaded.value_or("empty"));
        }

        if (Partitioning) {
            ProcessInputRows();
        }

        const bool inputRowsFinished = LastFetchStatus == NUdf::EFetchStatus::Finish;
        const bool allReadsFinished = AllReadsFinished();
        const bool allRowsProcessed = StreamLookupWorker->AllRowsProcessed();
        const bool hasPendingResults = StreamLookupWorker->HasPendingResults();

        if (hasPendingResults) {
            // has more results
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
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
        }
        LookupActorStateSpan.EndOk();

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessInputRows();

        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
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
            << ", ReadId=" << record.GetReadId() << " (current ReadId=" << ReadId << ")"
            << ", SeqNo=" << record.GetSeqNo()
            << ", Status=" << Ydb::StatusIds::StatusCode_Name(record.GetStatus().GetCode())
            << ", Finished=" << record.GetFinished()
            << ", RowCount=" << record.GetRowCount()
            << ", TxLocks= " << [&]() {
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

        if (!Snapshot.IsValid()) {
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
                Reads.erase(read);
                return ResolveTableShards();
            }
            case Ydb::StatusIds::OVERLOADED: {
                if (CheckTotalRetriesExeeded() || Reads.CheckShardRetriesExeeded(read)) {
                    return replyError(
                        TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' retry limit exceeded.",
                        NYql::NDqProto::StatusIds::OVERLOADED);
                }
                CA_LOG_D("OVERLOADED was received from tablet: " << read.ShardId << "."
                    << getIssues().ToOneLineString());
                read.SetBlocked();
                return RetryTableRead(read, /*allowInstantRetry = */false);
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
            Reads.erase(read);
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
        StreamLookupWorker->AddResult(TKqpStreamLookupWorker::TShardReadResult{
            shardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())
        });
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        const auto& tabletId = ev->Get()->TabletId;

        Reads.SetPipeDestroyed(tabletId);

        TVector<TReadState*> toRetry;
        for (auto* read : Reads.GetShardReads(tabletId)) {
            if (read->State == EReadState::Running) {
                Counters->IteratorDeliveryProblems->Inc();
                toRetry.push_back(read);
            }
        }
        for (auto* read : toRetry) {
            read->SetBlocked();
            RetryTableRead(*read);
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
                auto requests = StreamLookupWorker->RebuildRequest(read.Id, ReadId);
                for (auto& request : requests) {
                    StartTableRead(read.ShardId, std::move(request));
                }
                Reads.erase(read);
            } else {
                RetryTableRead(read);
            }
        }
    }

    void FetchInputRows() {
        auto guard = BindAllocator();

        NUdf::TUnboxedValue row;

        YQL_ENSURE(!Input.IsInvalid());
        if (Input.IsFinish() || !Input.HasValue()) {
            LastFetchStatus = NUdf::EFetchStatus::Finish;
            return;
        }

        while ((LastFetchStatus = Input.Fetch(row)) == NUdf::EFetchStatus::Ok) {
            StreamLookupWorker->AddInputRow(std::move(row));
        }
    }

    void ProcessInputRows() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        auto guard = BindAllocator();

        auto requests = StreamLookupWorker->BuildRequests(Partitioning, ReadId);
        for (auto& [shardId, request] : requests) {
            StartTableRead(shardId, std::move(request));
        }
    }

    void StartTableRead(ui64 shardId, THolder<TEvDataShard::TEvRead> request) {
        Counters->CreatedIterators->Inc();
        auto& record = request->Record;

        CA_LOG_D("Start reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << record.GetReadId()
            << ", shardId: " << shardId);

        TReadState read(record.GetReadId(), shardId);

        if (Snapshot.IsValid()) {
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
        Reads.insert(readId, std::move(read));

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

    void RetryTableRead(TReadState& failedRead, bool allowInstantRetry = true) {
        CA_LOG_D("Retry reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << failedRead.Id
            << ", shardId: " << failedRead.ShardId);

        if (CheckTotalRetriesExeeded()) {
            return RuntimeError(TStringBuilder() << "Table '" << StreamLookupWorker->GetTablePath() << "' retry limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }
        ++TotalRetryAttempts;

        if (Reads.CheckShardRetriesExeeded(failedRead)) {
            StreamLookupWorker->ResetRowsProcessing(failedRead.Id);
            Reads.erase(failedRead);
            return ResolveTableShards();
        }

        auto delay = Reads.CalcDelayForShard(failedRead, allowInstantRetry);
        if (delay == TDuration::Zero()) {
            auto guard = BindAllocator();
            auto requests = StreamLookupWorker->RebuildRequest(failedRead.Id, ReadId);
            for (auto& request : requests) {
                StartTableRead(failedRead.ShardId, std::move(request));
            }
            Reads.erase(failedRead);
        } else {
            CA_LOG_D("Schedule retry atempt for readId: " << failedRead.Id << " after " << delay);
            TlsActivationContext->Schedule(
                delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(failedRead.Id, failedRead.LastSeqNo, /*instantStart = */ true))
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
    NUdf::EFetchStatus LastFetchStatus = NUdf::EFetchStatus::Yield;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    const TDuration SchemeCacheRequestTimeout;
    NActors::TActorId SchemeCacheRequestTimeoutTimer;
    TVector<NKikimrDataEvents::TLock> Locks;
    TVector<NKikimrDataEvents::TLock> BrokenLocks;
    ui64 DeferredVictimQuerySpanId = 0;
    NKqpProto::EStreamLookupStrategy LookupStrategy;
    std::unique_ptr<TKqpStreamLookupWorker> StreamLookupWorker;
    ui64 ReadId = 0;
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
