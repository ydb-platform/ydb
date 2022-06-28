#include "kqp_stream_lookup_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NKqp {

namespace {

static constexpr TDuration RESOLVE_SHARDS_TIMEOUT = TDuration::Seconds(5);
static constexpr TDuration RETRY_READ_TIMEOUT = TDuration::Seconds(10);

class TKqpStreamLookupActor : public NActors::TActorBootstrapped<TKqpStreamLookupActor>, public NYql::NDq::IDqComputeActorAsyncInput {
public:
    TKqpStreamLookupActor(ui64 inputIndex, const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId,
        const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
        NKikimrKqp::TKqpStreamLookupSettings&& settings)
        : InputIndex(inputIndex), Input(input), ComputeActorId(computeActorId), TypeEnv(typeEnv)
        , HolderFactory(holderFactory), TableId(MakeTableId(settings.GetTable()))
        , KeyColumnTypes(settings.GetKeyColumnTypes().begin(), settings.GetKeyColumnTypes().end())
        , Snapshot(settings.GetSnapshot().GetStep(), settings.GetSnapshot().GetTxId())
        , ResolveTableShardsTimeout(RESOLVE_SHARDS_TIMEOUT)
        , RetryReadTimeout(RETRY_READ_TIMEOUT) {

        for (const auto& column : settings.GetColumns()) {
            Columns.emplace_back(&column);
        }
    };

    void Bootstrap() {
        ResolveTableShards();

        Become(&TKqpStreamLookupActor::StateFunc);
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
        TReadState(ui64 id, ui64 shardId, std::vector<TOwnedTableRange>&& keys)
            : Id(id)
            , ShardId(shardId)
            , Keys(std::move(keys))
            , State(EReadState::Initial)
            , Retried(false) {}

        void SetFinished(const NActors::TActorContext& ctx) {
            Keys.clear();
            State = EReadState::Finished;

            if (RetryDeadlineTimerId) {
                ctx.Send(RetryDeadlineTimerId, new TEvents::TEvPoisonPill());
                RetryDeadlineTimerId = {};
            }
        }

        bool Finished() const {
            return (State == EReadState::Finished);
        }

        const ui64 Id;
        const ui64 ShardId;
        std::vector<TOwnedTableRange> Keys;
        EReadState State;
        TActorId RetryDeadlineTimerId;
        bool Retried;
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryReadTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveTableShardsTimeout,
        };

        struct TEvResolveTableShardsTimeout : public TEventLocal<TEvResolveTableShardsTimeout, EvResolveTableShardsTimeout> {
        };

        struct TEvRetryReadTimeout : public TEventLocal<TEvRetryReadTimeout, EvRetryReadTimeout> {
            TEvRetryReadTimeout(ui64 readId) : ReadId(readId) {}

            ui64 ReadId;
        };
    };

private:
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDqProto::TSourceState&) final {}
    void LoadState(const NYql::NDqProto::TSourceState&) final {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    void PassAway() final {
        {
            auto alloc = BindAllocator();
            Input.Clear();
        }

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpStreamLookupActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueVector& batch, bool& finished, i64) final {
        i64 totalDataSize = 0;

        for (; !Results.empty(); Results.pop_front()) {
            const auto& result = Results.front();
            YQL_ENSURE(result.size() == Columns.size(), "Result columns mismatch");

            NUdf::TUnboxedValue* rowItems = nullptr;
            auto row = HolderFactory.CreateDirectArrayHolder(Columns.size(), rowItems);

            for (ui32 colId = 0; colId < Columns.size(); ++colId) {
                totalDataSize += result[colId].Size();
                rowItems[colId] = NMiniKQL::GetCellValue(result[colId], Columns[colId].TypeId);
            }

            batch.push_back(std::move(row));
        }

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue key;
        while ((status = Input.Fetch(key)) == NUdf::EFetchStatus::Ok) {
            std::vector<TCell> keyCells(KeyColumnTypes.size());
            for (ui32 colId = 0; colId < KeyColumnTypes.size(); ++colId) {
                keyCells[colId] = MakeCell(KeyColumnTypes[colId], key.GetElement(colId), TypeEnv, /* copy */ true);
            }

            UnprocessedKeys.emplace_back(std::move(keyCells));
        }

        if (Partitioning) {
            ProcessLookupKeys();
        }

        finished = (status == NUdf::EFetchStatus::Finish) && UnprocessedKeys.empty() && AllReadsFinished();
        return totalDataSize;
    }

    STFUNC(StateFunc) {
        Y_UNUSED(ctx);

        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvResolveTableShardsTimeout, Handle);
                hFunc(TEvPrivate::TEvRetryReadTimeout, Handle);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    RuntimeError(TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            RuntimeError(e.what());
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            return RuntimeError(TStringBuilder() << "Failed to get partitioning for table: " << TableId);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range (-inf, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessLookupKeys();
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto readIt = Reads.find(record.GetReadId());
        YQL_ENSURE(readIt != Reads.end(), "Unexpected readId: " << record.GetReadId());
        auto& read = readIt->second;

        if (read.State != EReadState::Running) {
            return;
        }

        // TODO: refactor after KIKIMR-15102
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            NKikimrTxDataShard::TReadContinuationToken continuationToken;
            bool parseResult = continuationToken.ParseFromString(record.GetContinuationToken());
            YQL_ENSURE(parseResult, "Failed to parse continuation token");
            YQL_ENSURE(continuationToken.GetFirstUnprocessedQuery() <= read.Keys.size());

            return RetryTableRead(read, continuationToken);
        }

        YQL_ENSURE(record.GetResultFormat() == NKikimrTxDataShard::EScanDataFormat::CELLVEC);
        auto nrows = ev->Get()->GetRowsCount();
        for (ui64 rowId = 0; rowId < nrows; ++rowId) {
            Results.emplace_back(ev->Get()->GetCells(rowId));
        }

        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));

        if (record.GetFinished()) {
            read.SetFinished(TlsActivationContext->AsActorContext());
        } else {
            THolder<TEvDataShard::TEvReadAck> request(new TEvDataShard::TEvReadAck());
            request->Record.SetReadId(record.GetReadId());
            request->Record.SetSeqNo(record.GetSeqNo());
            Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), read.ShardId, true),
                IEventHandle::FlagTrackDelivery);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const auto& tabletId = ev->Get()->TabletId;
        auto shardIt = ReadsPerShard.find(tabletId);
        YQL_ENSURE(shardIt != ReadsPerShard.end());

        for (auto readId : shardIt->second) {
            auto readIt = Reads.find(readId);
            YQL_ENSURE(readIt != Reads.end());
            auto& read = readIt->second;

            if (read.State == EReadState::Running) {
                for (auto& key : read.Keys) {
                    UnprocessedKeys.emplace_back(std::move(key));
                }

                read.SetFinished(TlsActivationContext->AsActorContext());
            }
        }

        ReadsPerShard.erase(shardIt);
        ResolveTableShards();
    }

    void Handle(TEvPrivate::TEvResolveTableShardsTimeout::TPtr&) {
        if (!Partitioning) {
            RuntimeError(TStringBuilder() << "Failed to resolve shards for table: " << TableId
                << " (request timeout exceeded)");
        }
    }

    void Handle(TEvPrivate::TEvRetryReadTimeout::TPtr& ev) {
        auto readIt = Reads.find(ev->Get()->ReadId);
        YQL_ENSURE(readIt != Reads.end(), "Unexpected readId: " << ev->Get()->ReadId);
        auto& read = readIt->second;

        if (read.Retried) {
            RuntimeError(TStringBuilder() << "Retry timeout exceeded for read: " << ev->Get()->ReadId);
        }
    }

    void ProcessLookupKeys() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        std::map<ui64, std::vector<TOwnedTableRange>> shardKeys;
        for (; !UnprocessedKeys.empty(); UnprocessedKeys.pop_front()) {
            const auto& key = UnprocessedKeys.front();
            YQL_ENSURE(key.Point);

            auto partitionInfo = LowerBound(
                Partitioning->begin(), Partitioning->end(), /* value */ true,
                [&](const auto& partition, bool) {
                    const int result = CompareBorders<true, false>(
                        partition.Range->EndKeyPrefix.GetCells(), key.From,
                        partition.Range->IsInclusive || partition.Range->IsPoint,
                        key.InclusiveFrom || key.Point, KeyColumnTypes
                    );

                    return (result < 0);
                }
            );

            shardKeys[partitionInfo->ShardId].emplace_back(std::move(key));
        }

        for (auto& [shardId, keys] : shardKeys) {
            StartTableRead(shardId, std::move(keys));
        }
    }

    TReadState& StartTableRead(ui64 shardId, std::vector<TOwnedTableRange>&& keys) {
        const auto readId = GetNextReadId();
        TReadState read(readId, shardId, std::move(keys));

        THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.MutableSnapshot()->SetStep(Snapshot.Step);
        record.MutableSnapshot()->SetTxId(Snapshot.TxId);

        record.SetReadId(read.Id);
        record.SetResultFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (const auto& column : Columns) {
            record.AddColumns(column.Id);
        }

        for (auto& key : read.Keys) {
            YQL_ENSURE(key.Point);
            request->Keys.emplace_back(TSerializedCellVec::Serialize(key.From));
        }

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shardId, true),
            IEventHandle::FlagTrackDelivery);

        read.State = EReadState::Running;

        const auto [readIt, succeeded] = Reads.insert({readId, std::move(read)});
        YQL_ENSURE(succeeded);
        ReadsPerShard[shardId].insert(readId);

        return readIt->second;
    }

    void RetryTableRead(TReadState& failedRead, NKikimrTxDataShard::TReadContinuationToken& token) {
        YQL_ENSURE(token.GetFirstUnprocessedQuery() <= failedRead.Keys.size());
        std::vector<TOwnedTableRange> unprocessedKeys(failedRead.Keys.size() - token.GetFirstUnprocessedQuery());
        for (ui64 idx = token.GetFirstUnprocessedQuery(); idx < failedRead.Keys.size(); ++idx) {
            unprocessedKeys.emplace_back(std::move(failedRead.Keys[idx]));
        }

        auto& newRead = StartTableRead(failedRead.ShardId, std::move(unprocessedKeys));
        if (failedRead.Retried) {
            newRead.RetryDeadlineTimerId = failedRead.RetryDeadlineTimerId;
            failedRead.RetryDeadlineTimerId = {};
        } else {
            failedRead.Retried = true;
            newRead.RetryDeadlineTimerId = CreateLongTimer(TlsActivationContext->AsActorContext(), RetryReadTimeout,
                new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryReadTimeout(newRead.Id)));
        }

        failedRead.SetFinished(TlsActivationContext->AsActorContext());
    }

    void ResolveTableShards() {
        Partitioning.reset();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();

        TVector<TCell> minusInf(KeyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Read,
            KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        ResolveTableShardsTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), ResolveTableShardsTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvResolveTableShardsTimeout()));
    }

    bool AllReadsFinished() const {
        for (const auto& [_, read] : Reads) {
            if (!read.Finished()) {
                return false;
            }
        }

        return true;
    }

    ui64 GetNextReadId() {
        static ui64 readId = 0;
        return ++readId;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    void RuntimeError(const TString& message, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), true));
    }

private:
    const ui64 InputIndex;
    NUdf::TUnboxedValue Input;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    const TTableId TableId;
    const TVector<NKikimr::NScheme::TTypeId> KeyColumnTypes;
    std::vector<NYql::TKikimrColumnMetadata> Columns;
    const IKqpGateway::TKqpSnapshot Snapshot;
    std::deque<TOwnedCellVec> Results;
    std::unordered_map<ui64, TReadState> Reads;
    std::unordered_map<ui64, std::set<ui64>> ReadsPerShard;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    const TDuration ResolveTableShardsTimeout;
    NActors::TActorId ResolveTableShardsTimeoutTimer;
    const TDuration RetryReadTimeout;
};

} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateStreamLookupActor(ui64 inputIndex,
    const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId, const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory, NKikimrKqp::TKqpStreamLookupSettings&& settings) {
    auto actor = new TKqpStreamLookupActor(inputIndex, input, computeActorId, typeEnv, holderFactory,
        std::move(settings));
    return {actor, actor};
}

} // namespace NKqp
} // namespace NKikimr
