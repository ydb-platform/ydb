#include "kqp_stream_lookup_actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_worker.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {
namespace NKqp {

namespace {

static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(10);
static constexpr ui64 MAX_SHARD_RETRIES = 10;

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
        , LockTxId(settings.HasLockTxId() ? settings.GetLockTxId() : TMaybe<ui64>())
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , StreamLookupWorker(CreateStreamLookupWorker(std::move(settings), args.TypeEnv, args.HolderFactory, args.InputDesc))
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

    void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats , bool last, const NYql::NDq::TDqMeteringStats*) override {
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

            // TODO: use evread statistics after KIKIMR-16924
            tableStats->SetReadRows(tableStats->GetReadRows() + ReadRowsCount);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + ReadBytesCount);
            tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + ReadsPerShard.size());

            NKqpProto::TKqpTableExtraStats tableExtraStats;
            auto readActorTableAggrExtraStats = tableExtraStats.MutableReadActorTableAggrExtraStats();
            for (const auto& [shardId, _] : ReadsPerShard) {
                readActorTableAggrExtraStats->AddAffectedShards(shardId);
            }

            tableStats->MutableExtra()->PackFrom(tableExtraStats);
        }
    }

private:
    enum class EReadState {
        Initial,
        Running,
        Finished,
    };

    std::string_view ReadStateToString(EReadState state) {
        switch (state) {
            case EReadState::Initial: return "Initial"sv;
            case EReadState::Running: return "Running"sv;
            case EReadState::Finished: return "Finished"sv;
        }
    }

    struct TReadState {
        TReadState(ui64 id, ui64 shardId)
            : Id(id)
            , ShardId(shardId)
            , State(EReadState::Initial) {}

        void SetFinished() {
            State = EReadState::Finished;
        }

        bool Finished() const {
            return (State == EReadState::Finished);
        }

        const ui64 Id;
        const ui64 ShardId;
        EReadState State;
        TMaybe<TOwnedCellVec> LastProcessedKey;
        ui32 FirstUnprocessedQuery = 0;
    };

    struct TShardState {
        ui64 RetryAttempts = 0;
        std::vector<TReadState*> Reads;
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryReadTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSchemeCacheRequestTimeout,
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
        };
    };

private:
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDqProto::TSourceState&) final {}
    void LoadState(const NYql::NDqProto::TSourceState&) final {}
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
                Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
            }
        }

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpStreamLookupActor>::PassAway();

        LookupActorSpan.End();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");

        auto replyResultStats = StreamLookupWorker->ReplyResult(batch, freeSpace);
        ReadRowsCount += replyResultStats.ReadRowsCount;
        ReadBytesCount += replyResultStats.ReadBytesCount;

        auto status = FetchInputRows();

        if (Partitioning) {
            ProcessInputRows();
        }

        finished = (status == NUdf::EFetchStatus::Finish)
            && AllReadsFinished()
            && StreamLookupWorker->AllRowsProcessed();

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
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    RuntimeError(TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite(),
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        CA_LOG_D("TEvResolveKeySetResult was received for table: " << StreamLookupWorker->GetTablePath());
        if (ev->Get()->Request->ErrorCount > 0) {
            TString errorMsg = TStringBuilder() << "Failed to get partitioning for table: " 
                << StreamLookupWorker->GetTablePath();
            LookupActorStateSpan.EndError(errorMsg);

            return RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        LookupActorStateSpan.EndOk();

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessInputRows();
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        CA_LOG_D("TEvReadResult was received for table: " << StreamLookupWorker->GetTablePath() <<
            ", readId: " << record.GetReadId() << ", finished: " << record.GetFinished());

        auto readIt = Reads.find(record.GetReadId());
        YQL_ENSURE(readIt != Reads.end(), "Unexpected readId: " << record.GetReadId());
        auto& read = readIt->second;

        if (read.State != EReadState::Running) {
            return;
        }

        for (auto& lock : record.GetBrokenTxLocks()) {
            BrokenLocks.push_back(lock);
        }

        for (auto& lock : record.GetTxLocks()) {
            Locks.push_back(lock);
        }

        Counters->DataShardIteratorMessages->Inc();
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Counters->DataShardIteratorFails->Inc();
        }

        switch (record.GetStatus().GetCode()) {
            case Ydb::StatusIds::SUCCESS:
                break;
            case Ydb::StatusIds::NOT_FOUND:
            case Ydb::StatusIds::OVERLOADED:
            case Ydb::StatusIds::INTERNAL_ERROR: {
                return RetryTableRead(read);
            }
            default: {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(record.GetStatus().GetIssues(), issues);
                return RuntimeError("Read request aborted", NYql::NDqProto::StatusIds::ABORTED, issues);
            }
        }

        if (record.GetFinished()) {
            read.SetFinished();
        } else {
            YQL_ENSURE(record.HasContinuationToken(), "Successful TEvReadResult should contain continuation token");
            NKikimrTxDataShard::TReadContinuationToken continuationToken;
            bool parseResult = continuationToken.ParseFromString(record.GetContinuationToken());
            YQL_ENSURE(parseResult, "Failed to parse continuation token");
            read.FirstUnprocessedQuery = continuationToken.GetFirstUnprocessedQuery();

            if (continuationToken.HasLastProcessedKey()) {
                TSerializedCellVec lastKey(continuationToken.GetLastProcessedKey());
                read.LastProcessedKey = TOwnedCellVec(lastKey.GetCells());
            }

            Counters->SentIteratorAcks->Inc();
            THolder<TEvDataShard::TEvReadAck> request(new TEvDataShard::TEvReadAck());
            request->Record.SetReadId(record.GetReadId());
            request->Record.SetSeqNo(record.GetSeqNo());
            request->Record.SetMaxRows(Max<ui16>());
            request->Record.SetMaxBytes(5_MB);
            Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), read.ShardId, true),
                IEventHandle::FlagTrackDelivery);

            CA_LOG_D("TEvReadAck was sent to shard: " << read.ShardId);
        }

        StreamLookupWorker->AddResult(TKqpStreamLookupWorker::TShardReadResult{
            read.ShardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())
        });
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        const auto& tabletId = ev->Get()->TabletId;
        auto shardIt = ReadsPerShard.find(tabletId);
        YQL_ENSURE(shardIt != ReadsPerShard.end());

        for (auto* read : shardIt->second.Reads) {
            if (read->State == EReadState::Running) {
                Counters->IteratorDeliveryProblems->Inc();
                StreamLookupWorker->ResetRowsProcessing(read->Id, read->FirstUnprocessedQuery, read->LastProcessedKey);
                read->SetFinished();
            }
        }

        ResolveTableShards();
    }

    void Handle(TEvPrivate::TEvSchemeCacheRequestTimeout::TPtr&) {
        CA_LOG_D("TEvSchemeCacheRequestTimeout was received, shards for table " << StreamLookupWorker->GetTablePath()
            << " was resolved: " << !!Partitioning);

        if (!Partitioning) {
            TString errorMsg = TStringBuilder() << "Failed to resolve shards for table: " << StreamLookupWorker->GetTablePath()
                << " (request timeout exceeded)";
            LookupActorStateSpan.EndError(errorMsg);

            RuntimeError(errorMsg, NYql::NDqProto::StatusIds::TIMEOUT);
        }
    }

    NUdf::EFetchStatus FetchInputRows() {
        auto guard = BindAllocator();

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue row;
        while ((status = Input.Fetch(row)) == NUdf::EFetchStatus::Ok) {
            StreamLookupWorker->AddInputRow(std::move(row));
        }

        return status;
    }

    void ProcessInputRows() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        auto guard = BindAllocator();

        auto requests = StreamLookupWorker->BuildRequests(Partitioning, ReadId);
        for (auto& [shardId, request] : requests) {
            StartTableRead(shardId, std::move(request));
        }
    }

    TReadState& StartTableRead(ui64 shardId, THolder<TEvDataShard::TEvRead> request) {
        Counters->CreatedIterators->Inc();
        auto& record = request->Record;

        CA_LOG_D("Start reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << record.GetReadId()
            << ", shardId: " << shardId);

        TReadState read(record.GetReadId(), shardId);

        YQL_ENSURE(Snapshot.IsValid(), "Invalid snapshot value");
        record.MutableSnapshot()->SetStep(Snapshot.Step);
        record.MutableSnapshot()->SetTxId(Snapshot.TxId);

        if (LockTxId && BrokenLocks.empty()) {
            record.SetLockTxId(*LockTxId);
        }

        record.SetMaxRows(Max<ui16>());
        record.SetMaxBytes(5_MB);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shardId, true),
            IEventHandle::FlagTrackDelivery, 0, LookupActorSpan.GetTraceId());

        read.State = EReadState::Running;

        auto readId = read.Id;
        const auto [readIt, succeeded] = Reads.insert({readId, std::move(read)});
        YQL_ENSURE(succeeded);
        ReadsPerShard[shardId].Reads.push_back(&readIt->second);

        return readIt->second;
    }

    void RetryTableRead(TReadState& failedRead) {
        CA_LOG_D("Retry reading of table: " << StreamLookupWorker->GetTablePath() << ", readId: " << failedRead.Id
            << ", shardId: " << failedRead.ShardId);

        StreamLookupWorker->ResetRowsProcessing(failedRead.Id, failedRead.FirstUnprocessedQuery, failedRead.LastProcessedKey);
        failedRead.SetFinished();

        auto& shardState = ReadsPerShard[failedRead.ShardId];
        if (shardState.RetryAttempts > MAX_SHARD_RETRIES) {
            RuntimeError(TStringBuilder() << "Retry limit exceeded for shard: " << failedRead.ShardId,
                NYql::NDqProto::StatusIds::ABORTED);
        } else {
            ++shardState.RetryAttempts;
            ResolveTableShards();
        }
    }

    void ResolveTableShards() {
        CA_LOG_D("Resolve shards for table: " << StreamLookupWorker->GetTablePath());

        Partitioning.reset();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();

        auto keyColumnTypes = StreamLookupWorker->GetKeyColumnTypes();

        TVector<TCell> minusInf(keyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(StreamLookupWorker->GetTableId(), range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Counters->IteratorsShardResolve->Inc();
        LookupActorStateSpan = NWilson::TSpan(TWilsonKqp::LookupActorShardsResolve, LookupActorSpan.GetTraceId(), 
            "WaitForShardsResolve", NWilson::EFlags::AUTO_END);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(StreamLookupWorker->GetTableId(), {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout()));
    }

    bool AllReadsFinished() const {
        for (const auto& [_, read] : Reads) {
            if (!read.Finished()) {
                return false;
            }
        }

        return true;
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
    const TMaybe<ui64> LockTxId;
    std::unordered_map<ui64, TReadState> Reads;
    std::unordered_map<ui64, TShardState> ReadsPerShard;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    const TDuration SchemeCacheRequestTimeout;
    NActors::TActorId SchemeCacheRequestTimeoutTimer;
    TVector<NKikimrDataEvents::TLock> Locks;
    TVector<NKikimrDataEvents::TLock> BrokenLocks;
    std::unique_ptr<TKqpStreamLookupWorker> StreamLookupWorker;
    ui64 ReadId = 0;

    // stats
    ui64 ReadRowsCount = 0;
    ui64 ReadBytesCount = 0;

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

} // namespace NKqp
} // namespace NKikimr
