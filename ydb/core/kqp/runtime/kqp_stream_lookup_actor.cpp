#include "kqp_stream_lookup_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NKqp {

namespace {

static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(5);
static constexpr TDuration RETRY_READ_TIMEOUT = TDuration::Seconds(10);

class TKqpStreamLookupActor : public NActors::TActorBootstrapped<TKqpStreamLookupActor>, public NYql::NDq::IDqComputeActorAsyncInput {
public:
    TKqpStreamLookupActor(ui64 inputIndex, const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId,
        const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
        NKikimrKqp::TKqpStreamLookupSettings&& settings)
        : InputIndex(inputIndex), Input(input), ComputeActorId(computeActorId), TypeEnv(typeEnv)
        , HolderFactory(holderFactory), TableId(MakeTableId(settings.GetTable()))
        , Snapshot(settings.GetSnapshot().GetStep(), settings.GetSnapshot().GetTxId())
        , LockTxId(settings.HasLockTxId() ? settings.GetLockTxId() : TMaybe<ui64>())
        , ImmediateTx(settings.GetImmediateTx())
        , KeyPrefixColumns(settings.GetKeyColumns().begin(), settings.GetKeyColumns().end())
        , Columns(settings.GetColumns().begin(), settings.GetColumns().end())
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , RetryReadTimeout(RETRY_READ_TIMEOUT) {
    };

    void Bootstrap() {
        ResolveTable();

        Become(&TKqpStreamLookupActor::StateFunc);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_STREAM_LOOKUP_ACTOR;
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

    enum EEvSchemeCacheRequestTag : ui64 {
        TableSchemeResolving,
        TableShardsResolving
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryReadTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSchemeCacheRequestTimeout,
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
            TEvSchemeCacheRequestTimeout(EEvSchemeCacheRequestTag tag) : Tag(tag) {}

            const EEvSchemeCacheRequestTag Tag;
        };

        struct TEvRetryReadTimeout : public TEventLocal<TEvRetryReadTimeout, EvRetryReadTimeout> {
            TEvRetryReadTimeout(ui64 readId) : ReadId(readId) {}

            const ui64 ReadId;
        };
    };

    struct TTableScheme {
        TTableScheme(const THashMap<ui32, TSysTables::TTableColumnInfo>& columns) {
            std::map<ui32, NScheme::TTypeInfo> keyColumnTypesByKeyOrder;
            for (const auto& [_, column] : columns) {
                if (column.KeyOrder >= 0) {
                    keyColumnTypesByKeyOrder[column.KeyOrder] = column.PType;
                }

                ColumnsByName.emplace(column.Name, std::move(column));
            }

            KeyColumnTypes.resize(keyColumnTypesByKeyOrder.size());
            for (const auto& [keyOrder, keyColumnType] : keyColumnTypesByKeyOrder) {
                KeyColumnTypes[keyOrder] = keyColumnType;
            }
        }

        std::unordered_map<TString, TSysTables::TTableColumnInfo> ColumnsByName;
        std::vector<NScheme::TTypeInfo> KeyColumnTypes;
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

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueVector& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        i64 totalDataSize = 0;

        if (TableScheme) {
            totalDataSize = PackResults(batch, freeSpace);
            auto status = FetchLookupKeys();

            if (Partitioning) {
                ProcessLookupKeys();
            }

            finished = (status == NUdf::EFetchStatus::Finish)
                && UnprocessedKeys.empty()
                && AllReadsFinished()
                && Results.empty();
        } else {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }

        return totalDataSize;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
        for (auto& lock : Locks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        result.PackFrom(resultInfo);
        return result;
    }

    STFUNC(StateFunc) {
        Y_UNUSED(ctx);

        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvSchemeCacheRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvRetryReadTimeout, Handle);
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
        if (ev->Get()->Request->ErrorCount > 0) {
            return RuntimeError(TStringBuilder() << "Failed to get partitioning for table: " << TableId,
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessLookupKeys();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for table: " << TableId);
        auto& result = resultSet[0];

        if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return RuntimeError(TStringBuilder() << "Failed to resolve table: " << ToString(result.Status),
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        TableScheme = std::make_unique<TTableScheme>(result.Columns);
        ResolveTableShards();
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto readIt = Reads.find(record.GetReadId());
        YQL_ENSURE(readIt != Reads.end(), "Unexpected readId: " << record.GetReadId());
        auto& read = readIt->second;

        if (read.State != EReadState::Running) {
            return;
        }

        if (record.BrokenTxLocksSize()) {
            return RuntimeError("Transaction locks invalidated.", NYql::NDqProto::StatusIds::ABORTED);
        }

        if (!Snapshot.IsValid() && !record.GetFinished()) {
            // HEAD read was converted to repeatable read
            Snapshot = IKqpGateway::TKqpSnapshot(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
        } else if (Snapshot.IsValid()) {
            YQL_ENSURE(record.GetSnapshot().GetStep() == Snapshot.Step && record.GetSnapshot().GetTxId() == Snapshot.TxId,
                "Snapshot version mismatch");
        }

        // TODO: refactor after KIKIMR-15102
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            NKikimrTxDataShard::TReadContinuationToken continuationToken;
            bool parseResult = continuationToken.ParseFromString(record.GetContinuationToken());
            YQL_ENSURE(parseResult, "Failed to parse continuation token");
            YQL_ENSURE(continuationToken.GetFirstUnprocessedQuery() <= read.Keys.size());

            return RetryTableRead(read, continuationToken);
        }

        for (auto& lock : record.GetTxLocks()) {
            Locks.push_back(lock);
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

    void Handle(TEvPrivate::TEvSchemeCacheRequestTimeout::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case EEvSchemeCacheRequestTag::TableSchemeResolving:
                if (!TableScheme) {
                    RuntimeError(TStringBuilder() << "Failed to resolve scheme for table: " << TableId
                        << " (request timeout exceeded)", NYql::NDqProto::StatusIds::TIMEOUT);
                }
                break;
            case EEvSchemeCacheRequestTag::TableShardsResolving:
                if (!Partitioning) {
                    RuntimeError(TStringBuilder() << "Failed to resolve shards for table: " << TableId
                        << " (request timeout exceeded)", NYql::NDqProto::StatusIds::TIMEOUT);
                }
                break;
            default:
                RuntimeError(TStringBuilder() << "Unexpected tag for TEvSchemeCacheRequestTimeout: " << (ui64)ev->Get()->Tag,
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Handle(TEvPrivate::TEvRetryReadTimeout::TPtr& ev) {
        auto readIt = Reads.find(ev->Get()->ReadId);
        YQL_ENSURE(readIt != Reads.end(), "Unexpected readId: " << ev->Get()->ReadId);
        auto& read = readIt->second;

        if (read.Retried) {
            RuntimeError(TStringBuilder() << "Retry timeout exceeded for read: " << ev->Get()->ReadId,
                NYql::NDqProto::StatusIds::TIMEOUT);
        }
    }

    ui64 PackResults(NKikimr::NMiniKQL::TUnboxedValueVector& batch, i64 freeSpace) {
        YQL_ENSURE(TableScheme);

        i64 totalSize = 0;
        batch.clear();
        batch.reserve(Results.size());

        std::vector<NKikimr::NScheme::TTypeInfo> columnTypes;
        columnTypes.reserve(Columns.size());
        for (const auto& column : Columns) {
            auto colIt = TableScheme->ColumnsByName.find(column);
            YQL_ENSURE(colIt != TableScheme->ColumnsByName.end());
            columnTypes.push_back(colIt->second.PType);
        }

        for (; !Results.empty(); Results.pop_front()) {
            const auto& result = Results.front();
            YQL_ENSURE(result.size() == Columns.size(), "Result columns mismatch");

            NUdf::TUnboxedValue* rowItems = nullptr;
            auto row = HolderFactory.CreateDirectArrayHolder(Columns.size(), rowItems);

            i64 rowSize = 0;
            for (ui32 colId = 0; colId < Columns.size(); ++colId) {
                rowItems[colId] = NMiniKQL::GetCellValue(result[colId], columnTypes[colId]);
                rowSize += result[colId].Size();
            }

            if (totalSize + rowSize > freeSpace) {
                row.DeleteUnreferenced();
                break;
            }

            batch.push_back(std::move(row));
            totalSize += rowSize;
        }

        return totalSize;
    }

    NUdf::EFetchStatus FetchLookupKeys() {
        YQL_ENSURE(TableScheme);
        YQL_ENSURE(KeyPrefixColumns.size() <= TableScheme->KeyColumnTypes.size());

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue key;
        while ((status = Input.Fetch(key)) == NUdf::EFetchStatus::Ok) {
            std::vector<TCell> keyCells(KeyPrefixColumns.size());
            for (ui32 colId = 0; colId < KeyPrefixColumns.size(); ++colId) {
                keyCells[colId] = MakeCell(TableScheme->KeyColumnTypes[colId], key.GetElement(colId), TypeEnv, /* copy */ true);
            }

            UnprocessedKeys.emplace_back(std::move(keyCells));
        }

        return status;
    }

    void ProcessLookupKeys() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        std::map<ui64, std::vector<TOwnedTableRange>> shardKeys;
        for (; !UnprocessedKeys.empty(); UnprocessedKeys.pop_front()) {
            const auto& key = UnprocessedKeys.front();
            YQL_ENSURE(key.Point);

            std::vector<ui64> shardIds;
            if (KeyPrefixColumns.size() < TableScheme->KeyColumnTypes.size()) {
                /* build range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf]) */
                std::vector<TCell> fromCells(TableScheme->KeyColumnTypes.size());
                fromCells.insert(fromCells.begin(), key.From.begin(), key.From.end());
                std::vector<TCell> toCells(key.From.begin(), key.From.end());

                shardIds = GetRangePartitioning(TOwnedTableRange{std::move(fromCells), /* inclusiveFrom */ true,
                     std::move(toCells), /* inclusiveTo */ false});
            } else {
                shardIds = GetRangePartitioning(key);
            }

            for (auto shardId : shardIds) {
                shardKeys[shardId].emplace_back(key);
            }
        }

        for (auto& [shardId, keys] : shardKeys) {
            StartTableRead(shardId, std::move(keys));
        }
    }

    std::vector<ui64> GetRangePartitioning(const TOwnedTableRange& range) {
        YQL_ENSURE(TableScheme);
        YQL_ENSURE(Partitioning);

        auto it = LowerBound(Partitioning->begin(), Partitioning->end(), /* value */ true,
            [&](const auto& partition, bool) {
                const int result = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, TableScheme->KeyColumnTypes
                );

                return (result < 0);
            }
        );

        YQL_ENSURE(it != Partitioning->end());

        std::vector<ui64> rangePartitions;
        for (; it != Partitioning->end(); ++it) {
            rangePartitions.push_back(it->ShardId);

            if (range.Point) {
                break;
            }

            auto cmp = CompareBorders<true, true>(
                it->Range->EndKeyPrefix.GetCells(), range.To,
                it->Range->IsInclusive || it->Range->IsPoint,
                range.InclusiveTo || range.Point, TableScheme->KeyColumnTypes
            );

            if (cmp >= 0) {
                break;
            }
        }

        return rangePartitions;
    }

    TReadState& StartTableRead(ui64 shardId, std::vector<TOwnedTableRange>&& keys) {
        const auto readId = GetNextReadId();
        TReadState read(readId, shardId, std::move(keys));

        THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        if (Snapshot.IsValid()) {
            record.MutableSnapshot()->SetStep(Snapshot.Step);
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
        } else {
            YQL_ENSURE(ImmediateTx, "HEAD reading is only available for immediate txs");
        }

        if (LockTxId) {
            record.SetLockTxId(*LockTxId);
        }

        record.SetReadId(read.Id);
        record.SetResultFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (const auto& column : Columns) {
            auto colIt = TableScheme->ColumnsByName.find(column);
            YQL_ENSURE(colIt != TableScheme->ColumnsByName.end());
            record.AddColumns(colIt->second.Id);
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

    void ResolveTable() {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

        SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout(EEvSchemeCacheRequestTag::TableSchemeResolving)));
    }

    void ResolveTableShards() {
        YQL_ENSURE(TableScheme);
        Partitioning.reset();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();

        TVector<TCell> minusInf(TableScheme->KeyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Read,
            TableScheme->KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout(EEvSchemeCacheRequestTag::TableShardsResolving)));
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

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

private:
    const ui64 InputIndex;
    NUdf::TUnboxedValue Input;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    const TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;
    const TMaybe<ui64> LockTxId;
    const bool ImmediateTx;
    const std::vector<TString> KeyPrefixColumns;
    const std::vector<TString> Columns;
    std::unique_ptr<const TTableScheme> TableScheme;
    std::deque<TOwnedCellVec> Results;
    std::unordered_map<ui64, TReadState> Reads;
    std::unordered_map<ui64, std::set<ui64>> ReadsPerShard;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    const TDuration SchemeCacheRequestTimeout;
    NActors::TActorId SchemeCacheRequestTimeoutTimer;
    const TDuration RetryReadTimeout;

    TVector<NKikimrTxDataShard::TLock> Locks;
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
