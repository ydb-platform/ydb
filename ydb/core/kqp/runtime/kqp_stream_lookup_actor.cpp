#include "kqp_stream_lookup_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

namespace NKikimr {
namespace NKqp {

namespace {

static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(10);
static constexpr ui64 MAX_SHARD_RETRIES = 10;

class TKqpStreamLookupActor : public NActors::TActorBootstrapped<TKqpStreamLookupActor>, public NYql::NDq::IDqComputeActorAsyncInput {
public:
    TKqpStreamLookupActor(ui64 inputIndex, const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId,
        const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
        std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc, NKikimrKqp::TKqpStreamLookupSettings&& settings,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "StreamLookupActor, inputIndex: " << inputIndex << ", CA Id " << computeActorId)
        , InputIndex(inputIndex), Input(input), ComputeActorId(computeActorId), TypeEnv(typeEnv)
        , HolderFactory(holderFactory), Alloc(alloc), TablePath(settings.GetTable().GetPath())
        , TableId(MakeTableId(settings.GetTable()))
        , Snapshot(settings.GetSnapshot().GetStep(), settings.GetSnapshot().GetTxId())
        , LockTxId(settings.HasLockTxId() ? settings.GetLockTxId() : TMaybe<ui64>())
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , Counters(counters)
    {
        KeyColumns.reserve(settings.GetKeyColumns().size());
        i32 keyOrder = 0;
        for (const auto& keyColumn : settings.GetKeyColumns()) {
            KeyColumns.emplace(
                keyColumn.GetName(),
                TSysTables::TTableColumnInfo{
                    keyColumn.GetName(),
                    keyColumn.GetId(),
                    NScheme::TTypeInfo{static_cast<NScheme::TTypeId>(keyColumn.GetTypeId())},
                    "",
                    keyOrder++
                }
            );
        }

        LookupKeyColumns.reserve(KeyColumns.size());
        for (const auto& lookupKeyColumn : settings.GetLookupKeyColumns()) {
            auto columnIt = KeyColumns.find(lookupKeyColumn);
            YQL_ENSURE(columnIt != KeyColumns.end());
            LookupKeyColumns.push_back(&columnIt->second);
        }

        Columns.reserve(settings.GetColumns().size());
        for (const auto& column : settings.GetColumns()) {
            Columns.emplace_back(TSysTables::TTableColumnInfo{
                column.GetName(),
                column.GetId(),
                NScheme::TTypeInfo{static_cast<NScheme::TTypeId>(column.GetTypeId())}
            });
        }
    };

    virtual ~TKqpStreamLookupActor() {
        if (Input.HasValue() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();
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
                if (table.GetTablePath() == TablePath) {
                    tableStats = &table;
                }
            }

            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(TablePath);
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
        TReadState(ui64 id, ui64 shardId, std::vector<TOwnedTableRange>&& keys)
            : Id(id)
            , ShardId(shardId)
            , Keys(std::move(keys))
            , State(EReadState::Initial) {}

        void SetFinished() {
            Keys.clear();
            State = EReadState::Finished;
        }

        bool Finished() const {
            return (State == EReadState::Finished);
        }

        const ui64 Id;
        const ui64 ShardId;
        std::vector<TOwnedTableRange> Keys;
        EReadState State;
    };

    struct TShardState {
        ui64 RetryAttempts = 0;
        std::vector<TReadState*> Reads;
    };

    struct TResult {
        const ui64 ShardId;
        THolder<TEventHandle<TEvDataShard::TEvReadResult>> ReadResult;
        size_t UnprocessedResultRow = 0;
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

    void PassAway() final {
        Counters->StreamLookupActorsCount->Dec();
        {
            auto alloc = BindAllocator();
            Input.Clear();
            for (auto& [id, state] : Reads) {
                Counters->SentIteratorCancels->Inc();
                auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
                cancel->Record.SetReadId(id);
                Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
            }
        }

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpStreamLookupActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueVector& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        i64 totalDataSize = 0;

        totalDataSize = PackResults(batch, freeSpace);
        auto status = FetchLookupKeys();

        if (Partitioning) {
            ProcessLookupKeys();
        }

        finished = (status == NUdf::EFetchStatus::Finish)
            && UnprocessedKeys.empty()
            && AllReadsFinished()
            && Results.empty();

        CA_LOG_D("Returned " << totalDataSize << " bytes, finished: " << finished);
        return totalDataSize;
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
        Y_UNUSED(ctx);

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
        CA_LOG_D("TEvResolveKeySetResult was received for table: " << TablePath);
        if (ev->Get()->Request->ErrorCount > 0) {
            return RuntimeError(TStringBuilder() << "Failed to get partitioning for table: " << TableId,
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        ProcessLookupKeys();
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        CA_LOG_D("TEvReadResult was received for table: " << TablePath <<
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
                TMaybe<NKikimrTxDataShard::TReadContinuationToken> continuationToken;
                if (record.HasContinuationToken()) {
                    bool parseResult = continuationToken->ParseFromString(record.GetContinuationToken());
                    YQL_ENSURE(parseResult, "Failed to parse continuation token");
                }

                return RetryTableRead(read, continuationToken);
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

        Results.emplace_back(TResult{read.ShardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())});
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
                for (auto& key : read->Keys) {
                    UnprocessedKeys.emplace_back(std::move(key));
                }

                read->SetFinished();
            }
        }

        ResolveTableShards();
    }

    void Handle(TEvPrivate::TEvSchemeCacheRequestTimeout::TPtr&) {
        CA_LOG_D("TEvSchemeCacheRequestTimeout was received, shards for table " << TablePath
            << " was resolved: " << !!Partitioning);

        if (!Partitioning) {
            RuntimeError(TStringBuilder() << "Failed to resolve shards for table: " << TableId
                << " (request timeout exceeded)", NYql::NDqProto::StatusIds::TIMEOUT);
        }
    }

    ui64 PackResults(NKikimr::NMiniKQL::TUnboxedValueVector& batch, i64 freeSpace) {
        i64 totalSize = 0;
        bool sizeLimitExceeded = false;
        batch.clear();

        size_t rowsCount = 0;
        for (const auto& result : Results) {
            rowsCount += result.ReadResult->Get()->GetRowsCount();
        }
        batch.reserve(rowsCount);

        while (!Results.empty() && !sizeLimitExceeded) {
            auto& result = Results.front();
            for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
                const auto& resultRow = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
                YQL_ENSURE(resultRow.size() <= Columns.size(), "Result columns mismatch");

                NUdf::TUnboxedValue* rowItems = nullptr;
                auto row = HolderFactory.CreateDirectArrayHolder(Columns.size(), rowItems);

                i64 rowSize = 0;
                for (size_t colIndex = 0, resultColIndex = 0; colIndex < Columns.size(); ++colIndex) {
                    const auto& column = Columns[colIndex];
                    if (IsSystemColumn(column.Name)) {
                        NMiniKQL::FillSystemColumn(rowItems[colIndex], result.ShardId, column.Id, column.PType);
                        rowSize += sizeof(NUdf::TUnboxedValue);
                    } else {
                        YQL_ENSURE(resultColIndex < resultRow.size());
                        rowItems[colIndex] = NMiniKQL::GetCellValue(resultRow[resultColIndex], column.PType);
                        rowSize += NMiniKQL::GetUnboxedValueSize(rowItems[colIndex], column.PType).AllocatedBytes;
                        ++resultColIndex;
                    }
                }

                if (totalSize + rowSize > freeSpace) {
                    row.DeleteUnreferenced();
                    sizeLimitExceeded = true;
                    break;
                }

                batch.push_back(std::move(row));
                ++ReadRowsCount;
                ReadBytesCount += rowSize;
                totalSize += rowSize;
            }

            if (result.UnprocessedResultRow == result.ReadResult->Get()->GetRowsCount()) {
                Results.pop_front();
            }
        }

        CA_LOG_D("Total batch size: " << totalSize << ", size limit exceeded: " << sizeLimitExceeded);
        return totalSize;
    }

    NUdf::EFetchStatus FetchLookupKeys() {
        YQL_ENSURE(LookupKeyColumns.size() <= KeyColumns.size());

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue key;
        while ((status = Input.Fetch(key)) == NUdf::EFetchStatus::Ok) {
            std::vector<TCell> keyCells(LookupKeyColumns.size());
            for (size_t colId = 0; colId < LookupKeyColumns.size(); ++colId) {
                const auto* lookupKeyColumn = LookupKeyColumns[colId];
                YQL_ENSURE(lookupKeyColumn->KeyOrder < static_cast<i64>(keyCells.size()));
                keyCells[lookupKeyColumn->KeyOrder] = MakeCell(lookupKeyColumn->PType,
                    key.GetElement(colId), TypeEnv, /* copy */ true);
            }

            UnprocessedKeys.emplace_back(std::move(keyCells));
        }

        return status;
    }

    void ProcessLookupKeys() {
        YQL_ENSURE(Partitioning, "Table partitioning should be initialized before lookup keys processing");

        std::unordered_map<ui64, std::vector<TOwnedTableRange>> shardKeys;
        for (; !UnprocessedKeys.empty(); UnprocessedKeys.pop_front()) {
            const auto& key = UnprocessedKeys.front();
            YQL_ENSURE(key.Point);

            std::vector<ui64> shardIds;
            if (LookupKeyColumns.size() < KeyColumns.size()) {
                /* build range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf]) */
                std::vector<TCell> fromCells(KeyColumns.size());
                fromCells.insert(fromCells.begin(), key.From.begin(), key.From.end());
                std::vector<TCell> toCells(key.From.begin(), key.From.end());

                shardIds = GetRangePartitioning(TOwnedTableRange{std::move(fromCells), /* inclusiveFrom */ true,
                     std::move(toCells), /* inclusiveTo */ false});
            } else {
                shardIds = GetRangePartitioning(key);
            }

            for (auto shardId : shardIds) {
                shardKeys[shardId].emplace_back(std::move(key));
            }
        }

        for (auto& [shardId, keys] : shardKeys) {
            StartTableRead(shardId, std::move(keys));
        }
    }

    std::vector<ui64> GetRangePartitioning(const TOwnedTableRange& range) {
        YQL_ENSURE(Partitioning);

        std::vector<NScheme::TTypeInfo> keyColumnTypes(KeyColumns.size());
        for (const auto& [_, columnInfo] : KeyColumns) {
            YQL_ENSURE(columnInfo.KeyOrder < static_cast<i64>(keyColumnTypes.size()));
            keyColumnTypes[columnInfo.KeyOrder] = columnInfo.PType;
        }

        auto it = LowerBound(Partitioning->begin(), Partitioning->end(), /* value */ true,
            [&](const auto& partition, bool) {
                const int result = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, keyColumnTypes
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
                range.InclusiveTo || range.Point, keyColumnTypes
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

        CA_LOG_D("Start reading of table: " << TablePath << ", readId: " << readId << ", shardId: " << shardId);

        Counters->CreatedIterators->Inc();
        THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        YQL_ENSURE(Snapshot.IsValid(), "Invalid snapshot value");
        record.MutableSnapshot()->SetStep(Snapshot.Step);
        record.MutableSnapshot()->SetTxId(Snapshot.TxId);

        if (LockTxId && BrokenLocks.empty()) {
            record.SetLockTxId(*LockTxId);
        }

        record.SetReadId(read.Id);
        record.SetMaxRows(Max<ui16>());
        record.SetMaxBytes(5_MB);
        record.SetResultFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (const auto& column : Columns) {
            if (!IsSystemColumn(column.Name)) {
                record.AddColumns(column.Id);
            }
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
        ReadsPerShard[shardId].Reads.push_back(&readIt->second);

        return readIt->second;
    }

    void RetryTableRead(TReadState& failedRead, TMaybe<NKikimrTxDataShard::TReadContinuationToken>& token) {
        CA_LOG_D("Retry reading of table: " << TablePath << ", readId: " << failedRead.Id
            << ", shardId: " << failedRead.ShardId);

        size_t firstUnprocessedQuery = token ? token->GetFirstUnprocessedQuery() : 0;
        YQL_ENSURE(firstUnprocessedQuery <= failedRead.Keys.size());
        for (ui64 idx = firstUnprocessedQuery; idx < failedRead.Keys.size(); ++idx) {
            UnprocessedKeys.emplace_back(std::move(failedRead.Keys[idx]));
        }

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
        CA_LOG_D("Resolve shards for table: " << TablePath);

        Partitioning.reset();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();

        TVector<TCell> minusInf(KeyColumns.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        std::vector<NScheme::TTypeInfo> keyColumnTypes(KeyColumns.size());
        for (const auto& [_, columnInfo] : KeyColumns) {
            keyColumnTypes[columnInfo.KeyOrder] = columnInfo.PType;
        }

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Counters->IteratorsShardResolve->Inc();
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
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
    const TString LogPrefix;
    const ui64 InputIndex;
    NUdf::TUnboxedValue Input;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const TString TablePath;
    const TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;
    const TMaybe<ui64> LockTxId;
    std::vector<TSysTables::TTableColumnInfo*> LookupKeyColumns;
    std::unordered_map<TString, TSysTables::TTableColumnInfo> KeyColumns;
    std::vector<TSysTables::TTableColumnInfo> Columns;
    std::deque<TResult> Results;
    std::unordered_map<ui64, TReadState> Reads;
    std::unordered_map<ui64, TShardState> ReadsPerShard;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    const TDuration SchemeCacheRequestTimeout;
    NActors::TActorId SchemeCacheRequestTimeoutTimer;
    TVector<NKikimrTxDataShard::TLock> Locks;
    TVector<NKikimrTxDataShard::TLock> BrokenLocks;

    // stats
    ui64 ReadRowsCount = 0;
    ui64 ReadBytesCount = 0;

    TIntrusivePtr<TKqpCounters> Counters;
};

} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateStreamLookupActor(ui64 inputIndex,
    const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId, const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory, std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
    NKikimrKqp::TKqpStreamLookupSettings&& settings,
    TIntrusivePtr<TKqpCounters> counters) {
    auto actor = new TKqpStreamLookupActor(inputIndex, input, computeActorId, typeEnv, holderFactory, alloc,
        std::move(settings), counters);
    return {actor, actor};
}

} // namespace NKqp
} // namespace NKikimr
