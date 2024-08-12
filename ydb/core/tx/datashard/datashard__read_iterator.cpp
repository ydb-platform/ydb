#include "datashard_failpoints.h"
#include "datashard_impl.h"
#include "datashard_read_operation.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "probes.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>

#include <ydb/library/actors/core/monotonic_provider.h>

#include <util/system/hp_timer.h>

#include <utility>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr::NDataShard {

using namespace NTabletFlatExecutor;

namespace {

constexpr ui64 MinRowsPerCheck = 1000;

class TRowCountBlockBuilder : public IBlockBuilder {
public:
    bool Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>&, ui64, ui64, TString&) override
    {
        return true;
    }

    void AddRow(const TDbTupleRef&, const TDbTupleRef&) override {
        ++RowCount;
    }

    TString Finish() override {
        return TString();
    }

    size_t Bytes() const override { return 0; }

private:
    ui64 RowCount = 0;

    std::unique_ptr<IBlockBuilder> Clone() const override {
        return nullptr;
    }
};

class TCellBlockBuilder : public IBlockBuilder {
public:
    bool Start(
        const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns,
        ui64 maxRowsInBlock,
        ui64 maxBytesInBlock,
        TString& err) override
    {
        Columns = columns;
        Y_UNUSED(maxRowsInBlock);
        Y_UNUSED(maxBytesInBlock);
        Y_UNUSED(err);
        return true;
    }

    void AddRow(const TDbTupleRef& key, const TDbTupleRef& value) override {
        Y_UNUSED(key);

        size_t DataSize = Batch.Append(value.Cells());
        BytesCount += DataSize;
    }

    TString Finish() override {
        return TString();
    }

    size_t Bytes() const override { return BytesCount; }

public:
    void FlushBatch(TOwnedCellVecBatch & result) {
        result = std::move(Batch);
        Batch = {};
    }

private:
    std::vector<std::pair<TString, NScheme::TTypeInfo>> Columns;

    TOwnedCellVecBatch Batch;
    ui64 BytesCount = 0;

    std::unique_ptr<IBlockBuilder> Clone() const override {
        return nullptr;
    }
};

struct TShortColumnInfo {
    NTable::TTag Tag;
    NScheme::TTypeInfo Type;
    TString Name;

    TShortColumnInfo(NTable::TTag tag, NScheme::TTypeInfo type, const TString& name)
        : Tag(tag)
        , Type(type)
        , Name(name)
    {}

    TString Dump() const {
        TStringStream ss;
        // TODO: support pg types
        ss << "{Tag: " << Tag << ", Type: " << Type.GetTypeId() << ", Name: " << Name << "}";
        return ss.Str();
    }
};

struct TShortTableInfo {
    TShortTableInfo() = default;
    TShortTableInfo(TShortTableInfo&& other) = default;

    TShortTableInfo(TUserTable::TCPtr& tableInfo) {
        LocalTid = tableInfo->LocalTid;
        SchemaVersion = tableInfo->GetTableSchemaVersion();
        KeyColumnTypes = tableInfo->KeyColumnTypes;
        KeyColumnCount = tableInfo->KeyColumnIds.size();

        for (const auto& it: tableInfo->Columns) {
            const auto& column = it.second;
            Columns.emplace(it.first, TShortColumnInfo(it.first, column.Type, column.Name));
        }
    }

    TShortTableInfo(ui32 localTid, const NTable::TRowScheme& schema) {
        LocalTid = localTid;
        KeyColumnCount = schema.Keys->Types.size();
        KeyColumnTypes.reserve(KeyColumnCount);
        for (auto type: schema.Keys->Types) {
            KeyColumnTypes.push_back(type.ToTypeInfo());
        }

        // note that we don't have column names here, but
        // for cellvec we will not need them at all
        for (const auto& col: schema.Cols) {
            Columns.emplace(col.Tag, TShortColumnInfo(col.Tag, col.TypeInfo, ""));
        }
    }

    TShortTableInfo& operator =(TShortTableInfo&& other) = default;

    TString Dump() const {
        TStringStream ss;
        ss << "{LocalTid: " << LocalTid << ", SchemaVerstion: " << SchemaVersion  << ", Columns: {";
        for (const auto& it: Columns) {
            ss << it.second.Dump();
        }
        ss << "}";
        return ss.Str();
    }

    ui32 LocalTid = 0;
    ui64 SchemaVersion = 0;
    size_t KeyColumnCount = 0;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TMap<NTable::TTag, TShortColumnInfo> Columns;
};

TVector<std::pair<TString, NScheme::TTypeInfo>> GetNameTypeColumns(
    const std::vector<NTable::TTag>& tags,
    const TShortTableInfo& tableInfo)
{
    TVector<std::pair<TString, NScheme::TTypeInfo>> result;
    for (auto tag: tags) {
        auto it = tableInfo.Columns.find(tag);
        if (it == tableInfo.Columns.end()) {
            result.clear();
            return result;
        }
        const auto& userColumn = it->second;
        result.emplace_back(userColumn.Name, userColumn.Type);
    }
    return result;
}

std::pair<std::unique_ptr<IBlockBuilder>, TString> CreateBlockBuilder(
    const TReadIteratorState& state,
    const TShortTableInfo& tableInfo)
{
    std::unique_ptr<IBlockBuilder> blockBuilder;
    TString error;

    if (state.Columns.empty()) {
        blockBuilder.reset(new TRowCountBlockBuilder());
        return std::make_pair(std::move(blockBuilder), error);
    }

    auto nameTypeCols = GetNameTypeColumns(state.Columns, tableInfo);
    if (nameTypeCols.empty()) {
        error = "Wrong columns requested";
        return std::make_pair(nullptr, error);
    }

    switch (state.Format) {
    case NKikimrDataEvents::FORMAT_ARROW:
        blockBuilder.reset(new NArrow::TArrowBatchBuilder());
        break;
    case NKikimrDataEvents::FORMAT_CELLVEC:
        blockBuilder.reset(new TCellBlockBuilder());
        break;
    default:
        error = TStringBuilder() << "Unknown format: " << (int)state.Format;
        return std::make_pair(nullptr, error);
    }

    TString err;
    if (!blockBuilder->Start(nameTypeCols, state.Quota.Rows, state.Quota.Bytes, err)) {
        error = TStringBuilder() << "Failed to start block builder: " << err;
        return std::make_pair(nullptr, error);
    }

    return std::make_pair(std::move(blockBuilder), error);
}

std::vector<TRawTypeValue> ToRawTypeValue(
    const TSerializedCellVec& keyCells,
    const TShortTableInfo& tableInfo,
    bool addNulls)
{
    std::vector<TRawTypeValue> result;
    result.reserve(keyCells.GetCells().size());

    for (ui32 i = 0; i < keyCells.GetCells().size(); ++i) {
        result.push_back(TRawTypeValue(keyCells.GetCells()[i].AsRef(), tableInfo.KeyColumnTypes[i]));
    }

    // note that currently without nulls it is [prefix, +inf, +inf],
    // and with nulls it is  [prefix, null, null]
    if (addNulls)
        result.resize(tableInfo.KeyColumnTypes.size());

    return result;
}

TSerializedCellVec ExtendWithNulls(
    const TSerializedCellVec& cells,
    size_t columnCount)
{
    TVector<TCell> extendedCells;
    extendedCells.reserve(columnCount);
    for (const auto& cell: cells.GetCells()) {
        extendedCells.emplace_back(cell);
    }

    extendedCells.resize(columnCount, TCell());
    return TSerializedCellVec(extendedCells);
}

ui64 ResetRowSkips(NTable::TIteratorStats& stats)
{
    return std::exchange(stats.DeletedRowSkips, 0UL) +
        std::exchange(stats.InvisibleRowSkips, 0UL);
}

// nota that reader captures state reference and must be used only
// after checking that state is still alive, i.e. read can be aborted
// between Execute() and Complete()
class TReader {
    const TReadIteratorState& State;
    IBlockBuilder& BlockBuilder;
    const TShortTableInfo& TableInfo;
    const TMonotonic StartTs;
    TDataShard* Self;

    const TTableId TableId;

    std::vector<NScheme::TTypeInfo> ColumnTypes;

    ui32 FirstUnprocessedQuery; // must be unsigned
    TString LastProcessedKey;
    bool LastProcessedKeyErased = false;

    ui64 RowsRead = 0;
    ui64 RowsProcessed = 0;
    ui64 RowsSinceLastCheck = 0;

    ui64 BytesInResult = 0;

    ui64 DeletedRowSkips = 0;
    ui64 InvisibleRowSkips = 0;
    bool HadInconsistentResult_ = false;

    NHPTimer::STime StartTime;
    NHPTimer::STime EndTime;

    static const NHPTimer::STime MaxCyclesPerIteration;

    NTable::ITransactionMapPtr TxMap;
    NTable::ITransactionObserverPtr TxObserver;
    absl::flat_hash_set<ui64> VolatileReadDependencies;
    bool VolatileWaitForCommit = false;

    enum class EReadStatus {
        Done = 0,
        NeedData,
        StoppedByLimit,
    };

public:
    TReader(TReadIteratorState& state,
            IBlockBuilder& blockBuilder,
            const TShortTableInfo& tableInfo,
            TMonotonic ts,
            TDataShard* self)
        : State(state)
        , BlockBuilder(blockBuilder)
        , TableInfo(tableInfo)
        , StartTs(ts)
        , Self(self)
        , TableId(state.PathId.OwnerId, state.PathId.LocalPathId, state.SchemaVersion)
        , FirstUnprocessedQuery(State.FirstUnprocessedQuery)
        , LastProcessedKey(State.LastProcessedKey)
        , LastProcessedKeyErased(State.LastProcessedKeyErased)
    {
        GetTimeFast(&StartTime);
        EndTime = StartTime;
    }

    EReadStatus ReadRange(
        TTransactionContext& txc,
        const TActorContext& ctx,
        const TSerializedTableRange& range)
    {
        bool fromInclusive;
        bool toInclusive;
        TSerializedCellVec keyFromCells;
        TSerializedCellVec keyToCells;
        if (LastProcessedKey) {
            if (!State.Reverse) {
                keyFromCells = TSerializedCellVec(LastProcessedKey);
                fromInclusive = LastProcessedKeyErased;

                keyToCells = range.To;
                toInclusive = range.ToInclusive;
            } else {
                // reverse
                keyFromCells = range.From;
                fromInclusive = range.FromInclusive;

                keyToCells = TSerializedCellVec(LastProcessedKey);
                toInclusive = LastProcessedKeyErased;
            }
        } else {
            keyFromCells = range.From;
            fromInclusive = range.FromInclusive;

            keyToCells = range.To;
            toInclusive = range.ToInclusive;
        }

        const auto keyFrom = ToRawTypeValue(keyFromCells, TableInfo, fromInclusive);
        const auto keyTo = ToRawTypeValue(keyToCells, TableInfo, !toInclusive);

        // TODO: split range into parts like in read_columns

        NTable::TKeyRange iterRange;
        iterRange.MinKey = keyFrom;
        iterRange.MaxKey = keyTo;
        iterRange.MinInclusive = fromInclusive;
        iterRange.MaxInclusive = toInclusive;
        const bool reverse = State.Reverse;

        if (TArrayRef<const TCell> cells = keyFromCells.GetCells()) {
            if (!fromInclusive || cells.size() >= TableInfo.KeyColumnTypes.size()) {
                Self->GetKeyAccessSampler()->AddSample(TableId, cells);
            } else {
                TVector<TCell> extended(cells.begin(), cells.end());
                extended.resize(TableInfo.KeyColumnTypes.size());
                Self->GetKeyAccessSampler()->AddSample(TableId, extended);
            }
        }

        EReadStatus result;
        if (!reverse) {
            auto iter = txc.DB.IterateRange(TableInfo.LocalTid, iterRange, State.Columns, State.ReadVersion, GetReadTxMap(), GetReadTxObserver());
            result = IterateRange(iter.Get(), ctx);
        } else {
            auto iter = txc.DB.IterateRangeReverse(TableInfo.LocalTid, iterRange, State.Columns, State.ReadVersion, GetReadTxMap(), GetReadTxObserver());
            result = IterateRange(iter.Get(), ctx);
        }

        if (result == EReadStatus::NeedData && !(RowsProcessed && CanResume())) {
            if (LastProcessedKey) {
                keyFromCells = TSerializedCellVec(LastProcessedKey);
                const auto keyFrom = ToRawTypeValue(keyFromCells, TableInfo, false);
                Precharge(txc.DB, keyFrom, iterRange.MaxKey, reverse);
            } else {
                Precharge(txc.DB, iterRange.MinKey, iterRange.MaxKey, reverse);
            }
            return EReadStatus::NeedData;
        }

        return result;
    }

    EReadStatus ReadKey(
        TTransactionContext& txc,
        const TActorContext& ctx,
        const TSerializedCellVec& keyCells,
        size_t keyIndex)
    {
        if (keyCells.GetCells().size() != TableInfo.KeyColumnCount) {
            // key prefix, treat it as range [prefix, null, null] - [prefix, +inf, +inf]
            TSerializedTableRange range;
            range.From = State.Keys[keyIndex];
            range.To = keyCells;
            range.ToInclusive = true;
            range.FromInclusive = true;
            return ReadRange(txc, ctx, range);
        }

        if (ColumnTypes.empty()) {
            for (auto tag: State.Columns) {
                auto it = TableInfo.Columns.find(tag);
                Y_ASSERT(it != TableInfo.Columns.end());
                ColumnTypes.emplace_back(it->second.Type);
            }
        }

        const auto key = ToRawTypeValue(keyCells, TableInfo, true);

        NTable::TRowState rowState;
        rowState.Init(State.Columns.size());
        NTable::TSelectStats stats;
        auto ready = txc.DB.Select(TableInfo.LocalTid, key, State.Columns, rowState, stats, 0, State.ReadVersion, GetReadTxMap(), GetReadTxObserver());
        if (ready == NTable::EReady::Page) {
            return EReadStatus::NeedData;
        }

        InvisibleRowSkips += stats.InvisibleRowSkips;
        RowsSinceLastCheck += 1 + stats.InvisibleRowSkips;
        RowsProcessed += 1 + stats.InvisibleRowSkips;

        Self->GetKeyAccessSampler()->AddSample(TableId, keyCells.GetCells());

        if (ready == NTable::EReady::Gone) {
            ++DeletedRowSkips;
            return EReadStatus::Done;
        }

        // TODO: looks kind of ugly: we assume that cells in rowState are stored in array
        TDbTupleRef value(ColumnTypes.data(), (*rowState).data(), ColumnTypes.size());

        // note that if user requests key columns then they will be in
        // rowValues and we don't have to add rowKey columns
        BlockBuilder.AddRow(TDbTupleRef(), value);
        ++RowsRead;

        return EReadStatus::Done;
    }

    bool PrechargeKey(
        TTransactionContext& txc,
        const TSerializedCellVec& keyCells)
    {
        if (keyCells.GetCells().size() != TableInfo.KeyColumnCount) {
            // key prefix, treat it as range [prefix, null, null] - [prefix, +inf, +inf]
            auto minKey = ToRawTypeValue(keyCells, TableInfo, true);
            auto maxKey = ToRawTypeValue(keyCells, TableInfo, false);
            return Precharge(txc.DB, minKey, maxKey, State.Reverse);
        } else {
            auto key = ToRawTypeValue(keyCells, TableInfo, true);
            return Precharge(txc.DB, key, key, State.Reverse);
        }
    }

    bool PrechargeKeysAfter(
        TTransactionContext& txc,
        ui32 queryIndex)
    {
        ui64 rowsLeft = GetRowsLeft();

        bool ready = true;
        while (rowsLeft > 0) {
            if (!State.Reverse) {
                ++queryIndex;
            } else {
                --queryIndex;
            }
            if (!(queryIndex < State.Request->Keys.size())) {
                break;
            }
            if (!PrechargeKey(txc, State.Request->Keys[queryIndex])) {
                ready = false;
            }
            --rowsLeft;
        }
        return ready;
    }

    // TODO: merge ReadRanges and ReadKeys to single template Read?

    bool ReadRanges(TTransactionContext& txc, const TActorContext& ctx) {
        // note that FirstUnprocessedQuery is unsigned and if we do reverse iteration,
        // then it will also become less than size() when finished
        while (FirstUnprocessedQuery < State.Request->Ranges.size()) {
            if (ReachedTotalRowsLimit()) {
                FirstUnprocessedQuery = -1;
                LastProcessedKey.clear();
                return true;
            }

            if (ShouldStop())
                return true;

            const auto& range = State.Request->Ranges[FirstUnprocessedQuery];
            auto status = ReadRange(txc, ctx, range);
            switch (status) {
            case EReadStatus::Done:
                break;
            case EReadStatus::StoppedByLimit:
                return true;
            case EReadStatus::NeedData:
                if (RowsProcessed && CanResume())
                    return true;

                // Note: ReadRange has already precharged current range and
                //       we don't precharge multiple ranges as opposed to keys
                return false;
            }

            if (!State.Reverse)
               FirstUnprocessedQuery++;
            else
               FirstUnprocessedQuery--;
            LastProcessedKey.clear();
        }

        return true;
    }

    bool ReadKeys(TTransactionContext& txc, const TActorContext& ctx) {
        // note that FirstUnprocessedQuery is unsigned and if we do reverse iteration,
        // then it will also become less than size() when finished
        while (FirstUnprocessedQuery < State.Request->Keys.size()) {
            if (ReachedTotalRowsLimit()) {
                FirstUnprocessedQuery = -1;
                LastProcessedKey.clear();
                return true;
            }

            if (ShouldStop())
                return true;

            const auto& key = State.Request->Keys[FirstUnprocessedQuery];
            auto status = ReadKey(txc, ctx, key, FirstUnprocessedQuery);
            switch (status) {
            case EReadStatus::Done:
                break;
            case EReadStatus::StoppedByLimit:
                return true;
            case EReadStatus::NeedData:
                if (RowsProcessed && CanResume())
                    return true;

                PrechargeKeysAfter(txc, FirstUnprocessedQuery);
                return false;
            }

            if (!State.Reverse)
               FirstUnprocessedQuery++;
            else
               FirstUnprocessedQuery--;
            LastProcessedKey.clear();
        }

        return true;
    }

    // return semantics the same as in the Execute()
    bool Read(TTransactionContext& txc, const TActorContext& ctx) {
        // TODO: consider trying to precharge multiple records at once in case
        // when first precharge fails?

        if (!State.Request->Keys.empty()) {
            return ReadKeys(txc, ctx);
        }

        // since no keys, then we must have ranges (has been checked initially)
        return ReadRanges(txc, ctx);
    }

    bool HasUnreadQueries() const {
        return FirstUnprocessedQuery < State.Request->Keys.size()
            || FirstUnprocessedQuery < State.Request->Ranges.size();
    }

    size_t GetQueriesCount() const {
        return State.Request->Keys.size() + State.Request->Ranges.size();
    }

    void UpdateCycles() {
        GetTimeFast(&EndTime);
    }

    NHPTimer::STime ElapsedCycles() const {
        return EndTime - StartTime;
    }

    bool ShouldStopByElapsedTime() {
        // TODO: should we also check bytes for the case
        // when rows are very heavy?
        if (RowsSinceLastCheck >= MinRowsPerCheck) {
            RowsSinceLastCheck = 0;
            UpdateCycles();

            return ElapsedCycles() >= MaxCyclesPerIteration;
        }

        return false;
    }

    /**
     * Fills the result and returns true when it is useful, false when it may be omitted
     */
    bool FillResult(TEvDataShard::TEvReadResult& result, TReadIteratorState& state) {
        bool useful = false;

        auto& record = result.Record;
        record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        auto now = AppData()->MonotonicTimeProvider->Now();
        auto delta = now - StartTs;
        Self->IncCounter(COUNTER_READ_ITERATOR_ITERATION_LATENCY_MS, delta.MilliSeconds());

        // note that in all metrics below we treat key prefix read as key read
        // and not as range read
        const bool isKeysRequest = !State.Request->Keys.empty();

        if (HasUnreadQueries()) {
            if (OutOfQuota()) {
                useful = true;
                Self->IncCounter(COUNTER_READ_ITERATOR_NO_QUOTA);
                record.SetLimitReached(true);
            } else if (HasMaxRowsInResult()) {
                useful = true;
                Self->IncCounter(COUNTER_READ_ITERATOR_MAX_ROWS_REACHED);
            } else {
                // FIXME: we could flush due to page faults
                Self->IncCounter(COUNTER_READ_ITERATOR_MAX_TIME_REACHED);
            }

            NKikimrTxDataShard::TReadContinuationToken continuationToken;
            continuationToken.SetFirstUnprocessedQuery(FirstUnprocessedQuery);

            // note that when LastProcessedKey set then
            // FirstUnprocessedQuery is definitely partially read range
            if (LastProcessedKey)
                continuationToken.SetLastProcessedKey(LastProcessedKey);

            bool res = continuationToken.SerializeToString(record.MutableContinuationToken());
            Y_ASSERT(res);
        } else {
            useful = true;
            state.IsFinished = true;
            record.SetFinished(true);
            auto fullDelta = now - State.StartTs;
            Self->IncCounter(COUNTER_READ_ITERATOR_LIFETIME_MS, fullDelta.MilliSeconds());

            if (isKeysRequest) {
                Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW, State.Request->Keys.size());
                Self->IncCounter(COUNTER_SELECT_ROWS_PER_REQUEST, State.Request->Keys.size());
            } else {
                Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE, State.Request->Ranges.size());
            }
        }

        if (record.TxLocksSize() > 0 || record.BrokenTxLocksSize() > 0) {
            useful = true;
        }

        Self->IncCounter(COUNTER_READ_ITERATOR_ROWS_READ, RowsRead);
        if (!isKeysRequest) {
            Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROW_SKIPS, DeletedRowSkips);
            Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS, RowsRead);
            Self->IncCounter(COUNTER_RANGE_READ_ROWS_PER_REQUEST, RowsRead);
        }

        if (RowsRead) {
            useful = true;
            record.SetRowCount(RowsRead);
        }

        // not that in case of empty columns set, here we have 0 bytes
        // and if is false
        BytesInResult = BlockBuilder.Bytes();
        if (BytesInResult) {
            Self->IncCounter(COUNTER_READ_ITERATOR_BYTES_READ, BytesInResult);
            if (isKeysRequest) {
                // backward compatibility
                Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW_BYTES, BytesInResult);
            } else {
                // backward compatibility
                Self->IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES, BytesInResult);
            }

            switch (State.Format) {
            case NKikimrDataEvents::FORMAT_ARROW: {
                auto& arrowBuilder = static_cast<NArrow::TArrowBatchBuilder&>(BlockBuilder);
                result.SetArrowBatch(arrowBuilder.FlushBatch(false));
                break;
            }
            case NKikimrDataEvents::FORMAT_CELLVEC: {
                auto& cellBuilder = static_cast<TCellBlockBuilder&>(BlockBuilder);
                TOwnedCellVecBatch batch;
                cellBuilder.FlushBatch(batch);
                result.SetBatch(std::move(batch));
                break;
            }
            default: {
                // never happens
            }
            }
        }

        record.SetResultFormat(State.Format);

        record.SetReadId(State.ReadId);
        record.SetSeqNo(State.SeqNo + 1);

        if (!State.IsHeadRead) {
            record.MutableSnapshot()->SetStep(State.ReadVersion.Step);
            record.MutableSnapshot()->SetTxId(State.ReadVersion.TxId);
        }

        return useful;
    }

    void UpdateState(TReadIteratorState& state, bool sentResult) {
        if (state.FirstUnprocessedQuery == FirstUnprocessedQuery &&
            state.LastProcessedKey && !LastProcessedKey)
        {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "DataShard " << Self->TabletID() << " detected unexpected reset of LastProcessedKey:"
                << " ReadId# " << State.ReadId
                << " LastSeqNo# " << State.SeqNo
                << " LastQuery# " << State.FirstUnprocessedQuery
                << " RowsRead# " << RowsRead
                << " RowsProcessed# " << RowsProcessed
                << " RowsSinceLastCheck# " << RowsSinceLastCheck
                << " BytesInResult# " << BytesInResult
                << " DeletedRowSkips# " << DeletedRowSkips
                << " InvisibleRowSkips# " << InvisibleRowSkips
                << " Quota.Rows# " << State.Quota.Rows
                << " Quota.Bytes# " << State.Quota.Bytes
                << " State.TotalRows# " << State.TotalRows
                << " State.TotalRowsLimit# " << State.TotalRowsLimit
                << " State.MaxRowsInResult# " << State.MaxRowsInResult);
            Self->IncCounterReadIteratorLastKeyReset();
        }

        state.TotalRows += RowsRead;
        state.FirstUnprocessedQuery = FirstUnprocessedQuery;
        state.LastProcessedKey = LastProcessedKey;
        state.LastProcessedKeyErased = LastProcessedKeyErased;
        if (sentResult) {
            state.ConsumeSeqNo(RowsRead, BytesInResult);
        }
    }

    ui64 GetRowsRead() const { return RowsRead; }
    ui64 GetBytesRead() const { return BytesInResult > 0 ? BytesInResult : BlockBuilder.Bytes(); }
    bool HadInvisibleRowSkips() const { return InvisibleRowSkips > 0; }
    bool HadInconsistentResult() const { return HadInconsistentResult_; }

    const absl::flat_hash_set<ui64>& GetVolatileReadDependencies() const { return VolatileReadDependencies; }
    bool NeedVolatileWaitForCommit() const { return VolatileWaitForCommit; }

private:
    bool CanResume() const {
        if (Self->IsFollower() && State.ReadVersion.IsMax()) {
            // HEAD reads from follower cannot be resumed
            return false;
        }

        // All other reads are assumed to be resumable
        return true;
    }

    bool OutOfQuota() const {
        return RowsRead >= State.Quota.Rows ||
            BlockBuilder.Bytes() >= State.Quota.Bytes||
            BytesInResult >= State.Quota.Bytes;
    }

    bool HasMaxRowsInResult() const {
        return RowsRead >= State.MaxRowsInResult;
    }

    bool ReachedTotalRowsLimit() const {
        if (State.TotalRowsLimit == Max<ui64>()) {
            return false;
        }

        return State.TotalRows + RowsRead >= State.TotalRowsLimit;
    }

    ui64 GetTotalRowsLeft() const {
        if (State.TotalRowsLimit == Max<ui64>()) {
            return Max<ui64>();
        }

        if (State.TotalRows + RowsRead >= State.TotalRowsLimit) {
            return 0;
        }


        return State.TotalRowsLimit - State.TotalRows - RowsRead;
    }

    bool ShouldStop() {
        if (!CanResume()) {
            return false;
        }

        return OutOfQuota() || HasMaxRowsInResult() || ShouldStopByElapsedTime();
    }

    ui64 GetRowsLeft() {
        ui64 rowsLeft = Max<ui64>();

        if (CanResume()) {
            Y_ASSERT(RowsRead <= State.Quota.Rows);
            rowsLeft = State.Quota.Rows - RowsRead;
        }

        return Min(rowsLeft, GetTotalRowsLeft());
    }

    ui64 GetBytesLeft() {
        ui64 bytesLeft = Max<ui64>();

        if (CanResume()) {
            Y_ASSERT(BlockBuilder.Bytes() <= State.Quota.Bytes);
            bytesLeft = State.Quota.Bytes - BlockBuilder.Bytes();
        }

        return bytesLeft;
    }

    bool Precharge(
        NTable::TDatabase& db,
        NTable::TRawVals keyFrom,
        NTable::TRawVals keyTo,
        bool reverse)
    {
        ui64 rowsLeft = GetRowsLeft();
        ui64 bytesLeft = GetBytesLeft();

        auto direction = reverse ? NTable::EDirection::Reverse : NTable::EDirection::Forward;
        return db.Precharge(TableInfo.LocalTid,
                            keyFrom,
                            keyTo,
                            State.Columns,
                            0,
                            rowsLeft,
                            bytesLeft,
                            direction,
                            State.ReadVersion);
    }

    template <typename TIterator>
    EReadStatus IterateRange(TIterator* iter, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto keyAccessSampler = Self->GetKeyAccessSampler();

        bool advanced = false;
        while (iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
            advanced = true;
            DeletedRowSkips += iter->Stats.DeletedRowSkips;
            InvisibleRowSkips += iter->Stats.InvisibleRowSkips;

            TDbTupleRef rowKey = iter->GetKey();

            keyAccessSampler->AddSample(TableId, rowKey.Cells());
            const ui64 processedRecords = 1 + ResetRowSkips(iter->Stats);
            RowsSinceLastCheck += processedRecords;
            RowsProcessed += processedRecords;

            TDbTupleRef rowValues = iter->GetValues();

            // note that if user requests key columns then they will be in
            // rowValues and we don't have to add rowKey columns
            BlockBuilder.AddRow(TDbTupleRef(), rowValues);
            ++RowsRead;

            if (ReachedTotalRowsLimit()) {
                LastProcessedKey.clear();
                return EReadStatus::Done;
            }

            if (ShouldStop()) {
                LastProcessedKey = TSerializedCellVec::Serialize(rowKey.Cells());
                LastProcessedKeyErased = false;
                return EReadStatus::StoppedByLimit;
            }
        }

        // Note: when stopping due to page faults after an erased row we will
        // reposition on that same row so erase cache can extend that cached
        // erased range. When we don't observe any user-visible rows before a
        // page fault we want to make sure we observe multiple deleted rows,
        // which must be at least 2 (because we may resume from a known deleted
        // row). When there are not enough rows we would prefer restarting in
        // the same transaction, instead of starting a new one, in which case
        // we will not update stats and will not update RowsProcessed.
        auto lastKey = iter->GetKey().Cells();
        if (lastKey && (advanced || iter->Stats.DeletedRowSkips >= 4) && iter->Last() == NTable::EReady::Page) {
            LastProcessedKey = TSerializedCellVec::Serialize(lastKey);
            LastProcessedKeyErased = iter->GetKeyState() == NTable::ERowOp::Erase;
            advanced = true;
        } else {
            LastProcessedKey.clear();
        }

        // last iteration to Page or Gone might also have deleted or invisible rows
        if (advanced || iter->Last() != NTable::EReady::Page) {
            DeletedRowSkips += iter->Stats.DeletedRowSkips;
            InvisibleRowSkips += iter->Stats.InvisibleRowSkips;
            const ui64 processedRecords = ResetRowSkips(iter->Stats);
            RowsSinceLastCheck += processedRecords;
            RowsProcessed += processedRecords;
        }

        // TODO: consider restart when Page and too few data read
        // (how much is too few, less than user's limit?)
        if (iter->Last() == NTable::EReady::Page) {
            return EReadStatus::NeedData;
        }

        return EReadStatus::Done;
    }

    const NTable::ITransactionMapPtr& GetReadTxMap() {
        if (!TxMap && Self->IsUserTable(State.PathId)) {
            auto baseTxMap = Self->GetVolatileTxManager().GetTxMap();

            bool needTxMap = (
                // We need tx map when there are waiting volatile transactions
                baseTxMap ||
                // We need tx map when current lock has uncommitted changes
                State.LockId && Self->SysLocksTable().HasCurrentWriteLock(State.PathId));

            if (needTxMap) {
                auto ptr = MakeIntrusive<NTable::TDynamicTransactionMap>(baseTxMap);
                if (State.LockId) {
                    ptr->Add(State.LockId, TRowVersion::Min());
                }
                TxMap = ptr;
            }
        }

        return TxMap;
    }

    const NTable::ITransactionObserverPtr& GetReadTxObserver() {
        if (!TxObserver && Self->IsUserTable(State.PathId)) {
            auto baseTxMap = Self->GetVolatileTxManager().GetTxMap();

            bool needTxObserver = (
                // We need tx observer when there are waiting volatile transactions
                baseTxMap ||
                // We need tx observer when there are active write locks
                State.LockId && Self->SysLocksTable().HasWriteLocks(State.PathId));

            if (needTxObserver) {
                if (State.LockId) {
                    TxObserver = new TLockedReadTxObserver(this);
                } else {
                    TxObserver = new TReadTxObserver(this);
                }
            }
        }

        return TxObserver;
    }

    class TLockedReadTxObserver : public NTable::ITransactionObserver {
    public:
        TLockedReadTxObserver(TReader* reader)
            : Reader(reader)
        {
        }

        void OnSkipUncommitted(ui64 txId) override {
            Reader->AddReadConflict(txId);
        }

        void OnSkipCommitted(const TRowVersion&) override {
            // We already use InvisibleRowSkips for these
        }

        void OnSkipCommitted(const TRowVersion&, ui64) override {
            // We already use InvisibleRowSkips for these
        }

        void OnApplyCommitted(const TRowVersion& rowVersion) override {
            Reader->CheckReadConflict(rowVersion);
        }

        void OnApplyCommitted(const TRowVersion& rowVersion, ui64 txId) override {
            Reader->CheckReadConflict(rowVersion);
            Reader->CheckReadDependency(txId);
        }

    private:
        TReader* const Reader;
    };

    class TReadTxObserver : public NTable::ITransactionObserver {
    public:
        TReadTxObserver(TReader* reader)
            : Reader(reader)
        {
        }

        void OnSkipUncommitted(ui64) override {
            // We don't care about uncommitted changes
        }

        void OnSkipCommitted(const TRowVersion&) override {
            // We already use InvisibleRowSkips for these
        }

        void OnSkipCommitted(const TRowVersion&, ui64) override {
            // We already use InvisibleRowSkips for these
        }

        void OnApplyCommitted(const TRowVersion&) override {
            // Not needed
        }

        void OnApplyCommitted(const TRowVersion&, ui64 txId) override {
            Reader->CheckReadDependency(txId);
        }

    private:
        TReader* const Reader;
    };

    void AddReadConflict(ui64 txId) {
        Y_ABORT_UNLESS(State.LockId);
        // We have skipped uncommitted changes in txId, which would affect
        // the read result when it commits. Add a conflict edge that breaks
        // our lock when txId is committed.
        Self->SysLocksTable().AddReadConflict(txId);
    }

    void CheckReadConflict(const TRowVersion& rowVersion) {
        if (rowVersion > State.ReadVersion) {
            // We have applied changes from a version above our snapshot
            // Normally these changes are skipped (since we are reading from
            // snapshot), but if we previously written changes for a key,
            // modified by transactions after our snapshot, we would hit this
            // code path. We have to break our own lock and make sure we won't
            // reply with inconsistent results.
            HadInconsistentResult_ = true;
        }
    }

    void CheckReadDependency(ui64 txId) {
        if (auto* info = Self->GetVolatileTxManager().FindByCommitTxId(txId)) {
            switch (info->State) {
                case EVolatileTxState::Waiting:
                    // We are reading undecided changes and need to wait until they are resolved
                    VolatileReadDependencies.insert(info->TxId);
                    break;
                case EVolatileTxState::Committed:
                    // Committed changes are immediately visible and don't need a dependency
                    if (!info->AddCommitted) {
                        // However we may need to wait until they are persistent
                        VolatileWaitForCommit = true;
                    }
                    break;
                case EVolatileTxState::Aborting:
                    // We just read something that we know is aborting, we would have to retry later
                    VolatileReadDependencies.insert(info->TxId);
                    break;
            }
        }
    }
};

std::unique_ptr<TEvDataShard::TEvReadResult> MakeEvReadResult(ui32 nodeId) {
    auto result = std::make_unique<TEvDataShard::TEvReadResult>();
    result->Record.SetNodeId(nodeId);
    return result;
}


const NHPTimer::STime TReader::MaxCyclesPerIteration =
    /* 10ms */ (NHPTimer::GetCyclesPerSecond() + 99) / 100;

} // namespace

class TDataShard::TReadOperation : public TOperation, public IReadOperation {
    TDataShard* Self;
    TReadIteratorId ReadId;

    NMiniKQL::IEngineFlat::TValidationInfo ValidationInfo;

    size_t ExecuteCount = 0;
    bool ResultSent = false;

    std::unique_ptr<TEvDataShard::TEvReadResult> Result;

    std::unique_ptr<IBlockBuilder> BlockBuilder;
    TShortTableInfo TableInfo;
    std::unique_ptr<TReader> Reader;

    static constexpr ui32 Flags = NTxDataShard::TTxFlags::ReadOnly | NTxDataShard::TTxFlags::Immediate;

public:
    TReadOperation(TDataShard* ds, TInstant receivedAt, ui64 tieBreakerIndex, const TReadIteratorId& readId)
        : TOperation(TBasicOpInfo(EOperationKind::ReadTx, Flags, 0, receivedAt, tieBreakerIndex))
        , Self(ds)
        , ReadId(readId)
    {}

    void BuildExecutionPlan(bool loaded) override
    {
        Y_ABORT_UNLESS(GetExecutionPlan().empty());
        Y_ABORT_UNLESS(!loaded);

        TVector<EExecutionUnitKind> plan;
        plan.push_back(EExecutionUnitKind::CheckRead);
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::ExecuteRead);
        plan.push_back(EExecutionUnitKind::CompletedOperations);

        RewriteExecutionPlan(plan);
    }

    const NMiniKQL::IEngineFlat::TValidationInfo& GetKeysInfo() const override {
        return ValidationInfo;
    }

    EExecutionStatus Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto readIt = Self->ReadIterators.find(ReadId);
        if (readIt == Self->ReadIterators.end()) {
            // iterator has been aborted
            return EExecutionStatus::DelayComplete;
        }
        Y_ABORT_UNLESS(readIt->second);
        auto& state = *readIt->second;

        if (Result->Record.HasStatus()) {
            // error happened on check phase
            return EExecutionStatus::DelayComplete;
        }

        Y_ABORT_UNLESS(state.State == TReadIteratorState::EState::Executing);

        auto* request = state.Request;

        ++ExecuteCount;
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Execute read# " << ExecuteCount
            << ", request: " << request->Record);

        switch (Self->State) {
        case TShardState::Ready:
        case TShardState::Readonly:
        case TShardState::Frozen:
        case TShardState::SplitSrcWaitForNoTxInFlight:
            break;
        case TShardState::Offline:
        case TShardState::PreOffline: {
            if (Self->SrcSplitDescription) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::OVERLOADED,
                    TStringBuilder() << "Shard in state " << DatashardStateName(Self->State)
                        << ", tablet id: " << Self->TabletID()
                        << ", node# " << ctx.SelfID.NodeId());
                return EExecutionStatus::DelayComplete;
            } else {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Shard in state " << DatashardStateName(Self->State)
                        << ", will be deleted soon, tablet id: " << Self->TabletID()
                        << ", node# " << ctx.SelfID.NodeId());
                return EExecutionStatus::DelayComplete;
            }
        }
        case TShardState::SplitSrcMakeSnapshot:
        case TShardState::SplitSrcSendingSnapshot:
        case TShardState::SplitSrcWaitForPartitioningChanged:
        case TShardState::SplitDstReceivingSnapshot: {
            SetStatusError(
                Result->Record,
                Ydb::StatusIds::OVERLOADED,
                TStringBuilder() << "Shard in state " << DatashardStateName(Self->State)
                    << ", tablet id: " << Self->TabletID()
                    << ", node# " << ctx.SelfID.NodeId());
            return EExecutionStatus::DelayComplete;
        }
        case TShardState::Uninitialized:
        case TShardState::WaitScheme:
        case TShardState::Unknown:
        default:
            SetStatusError(
                Result->Record,
                Ydb::StatusIds::INTERNAL_ERROR,
                TStringBuilder() << "Wrong shard state: " << DatashardStateName(Self->State)
                    << ", tablet id: " << Self->TabletID()
                    << ", node# " << ctx.SelfID.NodeId());
            return EExecutionStatus::DelayComplete;
        }

        // we need to check that scheme version is still correct, table presents and
        // version is still available

        if (state.PathId.OwnerId != Self->TabletID()) {
            // owner is schemeshard, read user table
            auto tableId = state.PathId.LocalPathId;
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << tableId
                    << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return EExecutionStatus::DelayComplete;
            }
            auto& userTableInfo = it->second;

            if (!state.ReadVersion.IsMax()) {
                bool snapshotFound = false;
                if (!state.IsHeadRead) {
                    const ui64 ownerId = state.PathId.OwnerId;
                    TSnapshotKey snapshotKey(
                        ownerId,
                        tableId,
                        state.ReadVersion.Step,
                        state.ReadVersion.TxId);

                    if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                        // TODO: do we need to acquire?
                        snapshotFound = true;
                    }
                }

                if (!snapshotFound) {
                    bool isMvccReadable = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
                    if (!isMvccReadable) {
                        SetStatusError(
                            Result->Record,
                            Ydb::StatusIds::PRECONDITION_FAILED,
                            TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                                << state.ReadVersion << " shard " << Self->TabletID()
                                << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                                << (Self->IsFollower() ? " RO replica" : "")
                                << " (node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                        return EExecutionStatus::DelayComplete;
                    }
                }
            }

            if (state.SchemaVersion != userTableInfo->GetTableSchemaVersion()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Schema changed, current " << userTableInfo->GetTableSchemaVersion()
                        << ", requested table schemaversion " << state.SchemaVersion
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return EExecutionStatus::DelayComplete;
            }
        }

        state.LockId = request->Record.GetLockTxId();
        state.LockNodeId = request->Record.GetLockNodeId();

        TDataShardLocksDb locksDb(*Self, txc);
        TSetupSysLocks guardLocks(state.LockId, state.LockNodeId, *Self, &locksDb);

        if (guardLocks.LockTxId) {
            switch (Self->SysLocksTable().EnsureCurrentLock()) {
                case EEnsureCurrentLock::Success:
                    // Lock is valid, we may continue with reads and side-effects
                    break;

                case EEnsureCurrentLock::Broken:
                    // Lock is valid, but broken, we could abort early in some
                    // cases, but it doesn't affect correctness.
                    break;

                case EEnsureCurrentLock::TooMany:
                    // Lock cannot be created, it's not necessarily a problem
                    // for read-only transactions.
                    break;

                case EEnsureCurrentLock::Abort:
                    // Lock cannot be created and we must abort
                    SetStatusError(
                        Result->Record,
                        Ydb::StatusIds::ABORTED,
                        TStringBuilder() << "Transaction was already committed or aborted"
                            << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                    return EExecutionStatus::DelayComplete;
            }
        }

        LWTRACK(ReadExecute, state.Orbit);
        if (!Read(txc, ctx, state))
            return EExecutionStatus::Restart;

        // Check if successful result depends on unresolved volatile transactions
        if (Result && !Result->Record.HasStatus() && !Reader->GetVolatileReadDependencies().empty()) {
            for (ui64 txId : Reader->GetVolatileReadDependencies()) {
                AddVolatileDependency(txId);
                bool ok = Self->GetVolatileTxManager().AttachBlockedOperation(txId, GetTxId());
                Y_ABORT_UNLESS(ok, "Unexpected failure to attach a blocked operation");
            }
            Reader.reset();
            Result = MakeEvReadResult(ctx.SelfID.NodeId());
            return EExecutionStatus::Continue;
        }

        TDataShard::EPromotePostExecuteEdges readType = TDataShard::EPromotePostExecuteEdges::RepeatableRead;

        if (state.IsHeadRead) {
            bool hasError = !Result || Result->Record.HasStatus();
            if (!hasError && Reader->HasUnreadQueries()) {
                // We failed to read everything in a single transaction
                // We would prefer to return current result and continue reading,
                // but we may have to retry at a different version or wait for
                // additional dependencies before retrying.
                state.IsHeadRead = false;

                if (!Self->IsFollower()) {
                    TRowVersion unreadableEdge = Self->Pipeline.GetUnreadableEdge();
                    if (state.ReadVersion >= unreadableEdge) {
                        // This version is unreadable in repeatable read mode at the moment, we have to wait
                        // We actually have to completely destroy current state and start from scratch
                        LWTRACK(ReadWaitSnapshot, request->Orbit, state.ReadVersion.Step, state.ReadVersion.TxId);
                        Self->Pipeline.AddWaitingReadIterator(state.ReadVersion, std::move(state.Ev), ctx);
                        Self->DeleteReadIterator(readIt);

                        // Make sure we rollback everything (on a slim chance there are any changes)
                        if (txc.DB.HasChanges()) {
                            txc.DB.RollbackChanges();
                        }

                        // This unit will remove current operation from the pipeline when we return
                        Abort(EExecutionUnitKind::CompletedOperations);

                        return EExecutionStatus::Executed;
                    }

                    // Switch to repeatable read at the same version
                    SetMvccSnapshot(state.ReadVersion, /* isRepeatable */ true);

                    // We may have had repeatable read conflicts, promote them
                    PromoteRepeatableReadConflicts();

                    // Having runtime conflicts now means we have to wait and restart
                    if (HasRuntimeConflicts()) {
                        // Make sure current incomplete result will not be sent
                        Result = MakeEvReadResult(ctx.SelfID.NodeId());

                        return EExecutionStatus::Continue;
                    }
                } else {
                    auto [followerEdge, followerRepeatable] = Self->GetSnapshotManager().GetFollowerReadEdge();
                    auto maxRepeatable = !followerEdge || followerRepeatable ? followerEdge : followerEdge.Prev();
                    if (maxRepeatable >= Self->GetSnapshotManager().GetLowWatermark() && maxRepeatable < state.ReadVersion) {
                        // We need to retry at a different version
                        state.ReadVersion = maxRepeatable;
                        SetMvccSnapshot(state.ReadVersion, /* isRepeatable */ true);

                        // Make sure current incomplete result will not be sent
                        Result = MakeEvReadResult(ctx.SelfID.NodeId());

                        return EExecutionStatus::Reschedule;
                    }
                }

                // We will send current incomplete result and continue reading from snapshot
            } else {
                // Either error or a complete result
                readType = TDataShard::EPromotePostExecuteEdges::ReadOnly;
            }
        }

        bool hadWrites = false;

        if (state.LockId) {
            // note that we set locks only when first read finish transaction,
            // i.e. we have read something without page faults
            AcquireLock(state, ctx);

            // Make sure we wait for commit (e.g. persisted lock added a write range)
            hadWrites |= locksDb.HasChanges();

            // We remember acquired lock for faster checking
            state.Lock = guardLocks.Lock;
        }

        if (!Self->IsFollower()) {
            auto res = Self->PromoteImmediatePostExecuteEdges(state.ReadVersion, readType, txc);
            hadWrites |= res.HadWrites;
        }

        if (hadWrites)
            return EExecutionStatus::DelayCompleteNoMoreRestarts;

        if (Self->Pipeline.HasCommittingOpsBelow(state.ReadVersion) || Reader && Reader->NeedVolatileWaitForCommit())
            return EExecutionStatus::DelayComplete;

        Complete(ctx);
        return EExecutionStatus::Executed;
    }

    void CheckRequestAndInit(TTransactionContext& txc, const TActorContext& ctx) override {
        auto it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // iterator has been aborted
            return;
        }
        Y_ABORT_UNLESS(it->second);
        auto& state = *it->second;
        Y_ABORT_UNLESS(state.State == TReadIteratorState::EState::Init);

        Result = MakeEvReadResult(ctx.SelfID.NodeId());

        auto* request = state.Request;
        const auto& record = request->Record;

        if (record.HasMaxRows())
            state.Quota.Rows = record.GetMaxRows();

        if (record.HasMaxBytes())
            state.Quota.Bytes = record.GetMaxBytes();

        if (record.HasResultFormat())
            state.Format = record.GetResultFormat();

        if (record.HasMaxRowsInResult())
            state.MaxRowsInResult = record.GetMaxRowsInResult();

        if (record.HasTotalRowsLimit())
            state.TotalRowsLimit = record.GetTotalRowsLimit();

        state.Reverse = record.GetReverse();
        if (state.Reverse) {
            state.FirstUnprocessedQuery = request->Keys.size() + request->Ranges.size() - 1;
        }

        // Note: some checks already performed in TTxReadViaPipeline::Execute
        if (state.PathId.OwnerId != Self->TabletID()) {
            // owner is schemeshard, read user table
            Y_ABORT_UNLESS(state.PathId.OwnerId == Self->GetPathOwnerId());

            const auto tableId = state.PathId.LocalPathId;
            auto it = Self->TableInfos.find(tableId);
            Y_ABORT_UNLESS(it != Self->TableInfos.end());

            auto& userTableInfo = it->second;
            TableInfo = TShortTableInfo(userTableInfo);

            Y_ABORT_UNLESS(!userTableInfo->IsBackup);
            Y_ABORT_UNLESS(Self->IsMvccEnabled());

            state.SchemaVersion = userTableInfo->GetTableSchemaVersion();
            if (record.GetTableId().HasSchemaVersion()) {
                if (state.SchemaVersion != 0 &&
                    state.SchemaVersion != record.GetTableId().GetSchemaVersion())
                {
                    SetStatusError(
                        Result->Record,
                        Ydb::StatusIds::SCHEME_ERROR,
                        TStringBuilder() << "Wrong schemaversion " << record.GetTableId().GetSchemaVersion()
                            << " requested, table schemaversion " << state.SchemaVersion
                            << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                    return;
                 }
            }

            userTableInfo->Stats.AccessTime = TAppData::TimeProvider->Now();
        } else {
            // DS is owner, read system table
            auto schema = txc.DB.GetRowScheme(state.PathId.LocalPathId);
            if (!schema) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Failed to get scheme for table local id: "
                        << state.PathId.LocalPathId
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        // Make ranges in the new 'any' form compatible with the old '+inf' form
        for (size_t i = 0; i < request->Ranges.size(); ++i) {
            auto& range = request->Ranges[i];
            auto& keyFrom = range.From;
            auto& keyTo = request->Ranges[i].To;

            if (range.FromInclusive && keyFrom.GetCells().size() != TableInfo.KeyColumnCount) {
                keyFrom = ExtendWithNulls(keyFrom, TableInfo.KeyColumnCount);
            }

            if (!range.ToInclusive && keyTo.GetCells().size() != TableInfo.KeyColumnCount) {
                keyTo = ExtendWithNulls(keyTo, TableInfo.KeyColumnCount);
            }
        }

        // Make prefixes in the new 'any' form compatible with the old '+inf' form
        for (size_t i = 0; i < request->Keys.size(); ++i) {
            const auto& key = request->Keys[i];
            if (key.GetCells().size() == TableInfo.KeyColumnCount)
                continue;

            if (state.Keys.size() != request->Keys.size()) {
                state.Keys.resize(request->Keys.size());
            }

            // we can safely use cells referencing original request->Keys[x],
            // because request will live until the end
            state.Keys[i] = ExtendWithNulls(key, TableInfo.KeyColumnCount);
        }

        state.Columns.reserve(record.ColumnsSize());
        for (auto col: record.GetColumns()) {
            auto it = TableInfo.Columns.find(col);
            if (it == TableInfo.Columns.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Unknown column: " << col
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return;
            }

            state.Columns.push_back(col);
        }

        state.State = TReadIteratorState::EState::Executing;

        Y_ASSERT(Result);

        if (state.PathId.OwnerId != Self->TabletID()) {
            PrepareValidationInfo(ctx, state);
        } else {
            // There should be no keys when reading sysm tables
            ValidationInfo.SetLoaded();
        }
    }

    void SendResult(const TActorContext& ctx) {
        if (ResultSent)
            return;
        ResultSent = true;

        auto it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // the one who removed the iterator should have replied to user
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " has been invalidated before TReadOperation::SendResult()");
            return;
        }

        Y_ABORT_UNLESS(it->second);
        auto& state = *it->second;
        auto* request = state.Request;

        if (!Result) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " TReadOperation::Execute() finished without Result, aborting");
            Result = MakeEvReadResult(ctx.SelfID.NodeId());
            SetStatusError(Result->Record, Ydb::StatusIds::ABORTED, TStringBuilder()
                << "Iterator aborted"
                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
            Result->Record.SetReadId(ReadId.ReadId);
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);
            
            request->ReadSpan.EndError("Iterator aborted");
            Self->DeleteReadIterator(it);
            return;
        }

        if (!Result->Record.HasStatus() && Reader && Reader->HadInconsistentResult()) {
            SetStatusError(Result->Record, Ydb::StatusIds::ABORTED, TStringBuilder()
                << "Read conflict with concurrent transaction"
                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
        }

        // error happened and status set
        auto& record = Result->Record;
        if (record.HasStatus()) {
            record.SetReadId(ReadId.ReadId);
            record.SetSeqNo(state.SeqNo + 1);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " TReadOperation::Execute() finished with error, aborting: " << record.DebugString());
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);

            request->ReadSpan.EndError("Finished with error");
            Self->DeleteReadIterator(it);
            return;
        }

        Y_ASSERT(Reader);
        Y_ASSERT(BlockBuilder);

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
            << " sends rowCount# " << Reader->GetRowsRead() << ", bytes# " << Reader->GetBytesRead()
            << ", quota rows left# " << (state.Quota.Rows - Reader->GetRowsRead())
            << ", quota bytes left# " << (state.Quota.Bytes - Reader->GetBytesRead())
            << ", hasUnreadQueries# " << Reader->HasUnreadQueries()
            << ", total queries# " << Reader->GetQueriesCount()
            << ", firstUnprocessed# " << state.FirstUnprocessedQuery);

        // Note: we only send useful non-empty results
        if (!Reader->FillResult(*Result, state)) {
            ResultSent = false;
            return;
        }

        if (!gSkipReadIteratorResultFailPoint.Check(Self->TabletID())) {
            LWTRACK(ReadSendResult, state.Orbit);
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);
        }
    }

    void Complete(const TActorContext& ctx) override {
        auto it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // the one who removed the iterator should have reply to user
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " has been invalidated before TReadOperation::Complete()");
            return;
        }
        auto& state = *it->second;
        auto* request = state.Request;

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Complete read# " << ReadId
            << " after executionsCount# " << ExecuteCount);

        SendResult(ctx);

        it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // We sent an error and deleted iterator
            return;
        }

        Y_ABORT_UNLESS(it->second);

        // Note that we save the state only when there are unread queries
        if (Reader->HasUnreadQueries()) {
            Reader->UpdateState(state, ResultSent);
            if (!state.IsExhausted()) {
                state.ReadContinuePending = true;
                ctx.Send(
                    Self->SelfId(),
                    new TEvDataShard::TEvReadContinue(ReadId.Sender, ReadId.ReadId));
            } else {
                Self->IncCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT);
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                    << " read iterator# " << ReadId << " exhausted");
            }
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " finished in read");

            request->ReadSpan.EndOk();
            Self->DeleteReadIterator(it);
        }
    }

private:
    // return semantics is like in Execute()
    bool Read(TTransactionContext& txc, const TActorContext& ctx, TReadIteratorState& state) {
        const auto& tableId = state.PathId.LocalPathId;
        if (state.PathId.OwnerId == Self->GetPathOwnerId()) {
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << state.PathId.LocalPathId
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return true;
            }
            auto userTableInfo = it->second;
            TableInfo = TShortTableInfo(userTableInfo);
            auto currentSchemaVersion = TableInfo.SchemaVersion;
            if (state.SchemaVersion != currentSchemaVersion) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Schema changed, current " << currentSchemaVersion
                        << ", requested table schemaversion " << state.SchemaVersion
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return true;
            }

            userTableInfo->Stats.AccessTime = TAppData::TimeProvider->Now();
        } else {
            auto schema = txc.DB.GetRowScheme(state.PathId.LocalPathId);
            if (!schema) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Failed to get scheme for table local id: "
                        << state.PathId.LocalPathId
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                return true;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        if (!state.ReadVersion.IsMax()) {
            bool snapshotFound = false;
            if (!state.IsHeadRead) {
                ui64 ownerId = state.PathId.OwnerId;
                TSnapshotKey snapshotKey(
                    ownerId,
                    tableId,
                    state.ReadVersion.Step,
                    state.ReadVersion.TxId);

                if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                    snapshotFound = true;
                }
            }

            if (!snapshotFound) {
                bool isMvccReadable = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
                if (!isMvccReadable) {
                    SetStatusError(
                        Result->Record,
                        Ydb::StatusIds::PRECONDITION_FAILED,
                        TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                            << state.ReadVersion << " shard " << Self->TabletID()
                            << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                            << (Self->IsFollower() ? " RO replica" : "")
                            << " (node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                    return true;
                }
            }
        }

        {
            auto p = CreateBlockBuilder(state, TableInfo);
            if (!p.first) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::BAD_REQUEST,
                    p.second);
                return true;
            }
            std::swap(BlockBuilder, p.first);
        }

        Y_ASSERT(Result);

        Reader.reset(new TReader(
            state,
            *BlockBuilder,
            TableInfo,
            AppData()->MonotonicTimeProvider->Now(),
            Self));

        return Reader->Read(txc, ctx);
    }

    void PrepareValidationInfo(const TActorContext&, const TReadIteratorState& state) {
        TTableId tableId(state.PathId.OwnerId, state.PathId.LocalPathId, state.SchemaVersion);

        TVector<NScheme::TTypeInfo> keyTypes;

        TVector<TKeyDesc::TColumnOp> columnOps;
        columnOps.reserve(TableInfo.Columns.size());
        for (const auto& it: TableInfo.Columns) {
            const auto& column = it.second;
            TKeyDesc::TColumnOp op;
            op.Column = it.first;
            op.Operation = TKeyDesc::EColumnOperation::Read;
            op.ExpectedType = column.Type;
            columnOps.emplace_back(std::move(op));
        }

        if (!state.Request->Keys.empty()) {
            for (size_t i = 0; i < state.Request->Keys.size(); ++i) {
                THolder<TKeyDesc> desc;
                const auto& key = state.Request->Keys[i];
                if (key.GetCells().size() != TableInfo.KeyColumnCount) {
                    // key prefix, treat it as range [prefix, 0, 0] - [prefix, +inf, +inf]
                    TTableRange range(
                        state.Keys[i].GetCells(),
                        true,
                        key.GetCells(),
                        true);

                    desc = MakeHolder<TKeyDesc>(
                        tableId,
                        range,
                        TKeyDesc::ERowOperation::Read,
                        TableInfo.KeyColumnTypes,
                        columnOps,
                        state.Quota.Rows,
                        state.Quota.Bytes,
                        state.Reverse);
                } else {
                    desc = MakeHolder<TKeyDesc>(
                        tableId,
                        TTableRange(key.GetCells()),
                        TKeyDesc::ERowOperation::Read,
                        TableInfo.KeyColumnTypes,
                        columnOps,
                        state.Quota.Rows,
                        state.Quota.Bytes,
                        state.Reverse);
                }

                ValidationInfo.Keys.emplace_back(
                    NMiniKQL::IEngineFlat::TValidatedKey(
                        std::move(desc),
                        /* isWrite */ false));
                ++ValidationInfo.ReadsCount;
            }
        } else {
            // since no keys, then we must have ranges (has been checked initially)
            for (size_t i = 0; i < state.Request->Ranges.size(); ++i) {
                TTableRange range = state.Request->Ranges[i].ToTableRange();

                auto desc = MakeHolder<TKeyDesc>(
                    tableId,
                    range,
                    TKeyDesc::ERowOperation::Read,
                    TableInfo.KeyColumnTypes,
                    columnOps,
                    state.Quota.Rows,
                    state.Quota.Bytes,
                    state.Reverse);

                ValidationInfo.Keys.emplace_back(
                    NMiniKQL::IEngineFlat::TValidatedKey(
                        std::move(desc),
                        /* isWrite */ false));
                ++ValidationInfo.ReadsCount;
            }
        }

        ValidationInfo.SetLoaded();
    }

    void AcquireLock(TReadIteratorState& state, const TActorContext& ctx) {
        auto& sysLocks = Self->SysLocksTable();

        TTableId tableId(state.PathId.OwnerId, state.PathId.LocalPathId, state.SchemaVersion);

        if (!state.Request->Keys.empty()) {
            for (size_t i = 0; i < state.Request->Keys.size(); ++i) {
                const auto& key = state.Request->Keys[i];
                if (key.GetCells().size() != TableInfo.KeyColumnCount) {
                    // key prefix, treat it as range [prefix, 0, 0] - [prefix, +inf, +inf]
                    TTableRange lockRange(
                        state.Keys[i].GetCells(),
                        true,
                        key.GetCells(),
                        true);
                    sysLocks.SetLock(tableId, lockRange);
                } else {
                    sysLocks.SetLock(tableId, key.GetCells());
                }
            }
        } else {
            // no keys, so we must have ranges (has been checked initially)
            for (size_t i = 0; i < state.Request->Ranges.size(); ++i) {
                auto range = state.Request->Ranges[i].ToTableRange();
                sysLocks.SetLock(tableId, range);
            }
        }

        if (Reader->HadInvisibleRowSkips() || Reader->HadInconsistentResult()) {
            sysLocks.BreakSetLocks();
        }

        auto locks = sysLocks.ApplyLocks();

        for (auto& lock : locks) {
            NKikimrDataEvents::TLock* addLock;
            if (lock.IsError()) {
                addLock = Result->Record.AddBrokenTxLocks();
            } else {
                addLock = Result->Record.AddTxLocks();
            }

            addLock->SetLockId(lock.LockId);
            addLock->SetDataShard(lock.DataShard);
            addLock->SetGeneration(lock.Generation);
            addLock->SetCounter(lock.Counter);
            addLock->SetSchemeShard(lock.SchemeShard);
            addLock->SetPathId(lock.PathId);
            if (lock.HasWrites) {
                addLock->SetHasWrites(true);
            }

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Acquired lock# " << lock.LockId << ", counter# " << lock.Counter
                << " for " << state.PathId);
        }
    }
};

class TDataShard::TTxReadViaPipeline : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    TReadIteratorId ReadId;

    // When we need to reply with an error
    std::unique_ptr<TEvDataShard::TEvReadResult> Reply;

    TOperation::TPtr Op;
    TVector<EExecutionUnitKind> CompleteList;
    bool WaitComplete = false;

public:
    TTxReadViaPipeline(TDataShard* ds, const TReadIteratorId& readId, NWilson::TTraceId &&traceId)
        : TBase(ds, std::move(traceId))
        , ReadId(readId)
    {}

    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxReadViaPipeline execute"
            << ": at tablet# " << Self->TabletID());

        auto readIt = Self->ReadIterators.find(ReadId);
        if (readIt == Self->ReadIterators.end() && !Op) {
            // iterator aborted before we could start operation
            return true;
        }

        try {
            // If tablet is in follower mode then we should sync scheme
            // before we build and check operation.
            if (Self->IsFollower()) {
                NKikimrTxDataShard::TError::EKind status = NKikimrTxDataShard::TError::OK;
                TString errMessage;

                if (!Self->SyncSchemeOnFollower(txc, ctx, status, errMessage)) {
                    return false;
                }

                if (status != NKikimrTxDataShard::TError::OK) {
                    Y_DEBUG_ABORT_UNLESS(!Op);
                    if (Y_UNLIKELY(readIt == Self->ReadIterators.end())) {
                        // iterator already aborted
                        return true;
                    }
                    auto& state = *readIt->second;
                    ReplyError(
                        Ydb::StatusIds::INTERNAL_ERROR,
                        TStringBuilder() << "Failed to sync follower: " << errMessage
                            << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                        ctx.SelfID.NodeId(),
                        state.Request->ReadSpan);
                    return true;
                }
            }

            if (!Op) {
                // We must perform some initialization in transaction (e.g. after a follower sync), but before the operation is built
                Y_ABORT_UNLESS(readIt != Self->ReadIterators.end());
                Y_ABORT_UNLESS(readIt->second);
                auto& state = *readIt->second;
                auto* request = state.Request;
                const auto& record = request->Record;
                NWilson::TSpan &readSpan = request->ReadSpan;

                Y_ABORT_UNLESS(state.State == TReadIteratorState::EState::Init);

                bool setUsingSnapshotFlag = false;

                // We assume that owner is schemeshard and it's a user table
                if (state.PathId.OwnerId != Self->TabletID()) {
                    if (state.PathId.OwnerId != Self->GetPathOwnerId()) {
                        ReplyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "Requesting ownerId: " << state.PathId.OwnerId
                                << ", tableId: " << state.PathId.LocalPathId
                                << ", from shard with owner: " << Self->GetPathOwnerId()
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }

                    const auto tableId = state.PathId.LocalPathId;
                    auto it = Self->TableInfos.find(tableId);
                    if (it == Self->TableInfos.end()) {
                        ReplyError(
                            Ydb::StatusIds::NOT_FOUND,
                            TStringBuilder() << "Unknown table id: " << tableId
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }

                    auto& userTableInfo = it->second;
                    if (userTableInfo->IsBackup) {
                        ReplyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "Can't read from a backup table"
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }

                    if (!Self->IsMvccEnabled()) {
                        ReplyError(
                            Ydb::StatusIds::UNSUPPORTED,
                            TStringBuilder() << "Cannot use read iterators without mvcc"
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }

                    if (state.IsHeadRead) {
                        // We want to try and choose a more specific non-repeatable snapshot
                        if (Self->IsFollower()) {
                            auto [followerEdge, followerRepeatable] = Self->GetSnapshotManager().GetFollowerReadEdge();
                            // Note: during transition follower edge may be unitialized or lag behind
                            // We assume we can use it when it's not before low watermark
                            auto maxRepeatable = !followerEdge || followerRepeatable ? followerEdge : followerEdge.Prev();
                            if (maxRepeatable >= Self->GetSnapshotManager().GetLowWatermark()) {
                                state.ReadVersion = followerEdge;
                                state.IsHeadRead = !followerRepeatable;
                            }
                        } else {
                            state.ReadVersion = Self->GetMvccTxVersion(EMvccTxMode::ReadOnly);
                        }
                        if (!state.ReadVersion.IsMax()) {
                            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                                Self->TabletID() << " changed HEAD read to "
                                << (state.IsHeadRead ? "non-repeatable" : "repeatable")
                                << " " << state.ReadVersion);
                        }
                    } else {
                        bool snapshotFound = false;

                        const ui64 ownerId = state.PathId.OwnerId;
                        TSnapshotKey snapshotKey(
                            ownerId,
                            tableId,
                            state.ReadVersion.Step,
                            state.ReadVersion.TxId);

                        if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                            // TODO: do we need to acquire?
                            setUsingSnapshotFlag = true;
                            snapshotFound = true;
                        }

                        if (!snapshotFound) {
                            bool snapshotUnavailable = false;

                            if (state.ReadVersion < Self->GetSnapshotManager().GetLowWatermark() || state.ReadVersion.Step == Max<ui64>()) {
                                snapshotUnavailable = true;
                            }

                            if (Self->IsFollower()) {
                                auto [followerEdge, followerRepeatable] = Self->GetSnapshotManager().GetFollowerReadEdge();
                                auto maxRepeatable = !followerEdge || followerRepeatable ? followerEdge : followerEdge.Prev();
                                if (state.ReadVersion > maxRepeatable) {
                                    snapshotUnavailable = true;
                                }
                            } else {
                                TRowVersion unreadableEdge = Self->Pipeline.GetUnreadableEdge();
                                if (state.ReadVersion >= unreadableEdge) {
                                    LWTRACK(ReadWaitSnapshot, request->Orbit, state.ReadVersion.Step, state.ReadVersion.TxId);
                                    Self->Pipeline.AddWaitingReadIterator(state.ReadVersion, std::move(state.Ev), ctx);
                                    Self->DeleteReadIterator(readIt);
                                    return true;
                                }
                            }

                            if (snapshotUnavailable) {
                                ReplyError(
                                    Ydb::StatusIds::PRECONDITION_FAILED,
                                    TStringBuilder() << "Table id " << tableId << " has no snapshot at "
                                        << state.ReadVersion << " shard " << Self->TabletID()
                                        << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                                        << (Self->IsFollower() ? " RO replica" : "")
                                        << " (node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                                    ctx.SelfID.NodeId(),
                                    readSpan);
                                return true;
                            }
                        }
                    }
                } else {
                    // Handle system table reads
                    if (Self->IsFollower()) {
                        ReplyError(
                            Ydb::StatusIds::UNSUPPORTED,
                            TStringBuilder() << "Followers don't support system table reads"
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }
                    if (!state.IsHeadRead) {
                        ReplyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "Cannot read system table using snapshot " << state.ReadVersion
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }
                    if (record.GetTableId().GetTableId() >= TDataShard::Schema::MinLocalTid) {
                        ReplyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "Cannot read from user tables using system tables"
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }
                    if (record.GetResultFormat() != NKikimrDataEvents::FORMAT_CELLVEC) {
                        ReplyError(
                            Ydb::StatusIds::UNSUPPORTED,
                            TStringBuilder() << "Unsupported result format "
                                << (int)record.GetResultFormat() << " when reading from system tables"
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }
                    if (record.GetTableId().HasSchemaVersion()) {
                        ReplyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "Cannot request system table at shard " << record.GetTableId().GetOwnerId()
                                << ", localTid: " << record.GetTableId().GetTableId()
                                << ", with schema: " << record.GetTableId().GetSchemaVersion()
                                << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")",
                            ctx.SelfID.NodeId(),
                            readSpan);
                        return true;
                    }

                    // We don't want this read to interact with other operations
                    setUsingSnapshotFlag = true;
                }

                const ui64 tieBreaker = Self->NextTieBreakerIndex++;
                Op = new TReadOperation(Self, ctx.Now(), tieBreaker, ReadId);

                Op->BuildExecutionPlan(false);
                Self->Pipeline.GetExecutionUnit(Op->GetCurrentUnit()).AddOperation(Op);

                if (!state.ReadVersion.IsMax()) {
                    Op->SetMvccSnapshot(
                        TRowVersion(state.ReadVersion.Step, state.ReadVersion.TxId),
                        /* repeatable = */ state.IsHeadRead ? false : true);
                }
                if (setUsingSnapshotFlag) {
                    Op->SetUsingSnapshotFlag();
                }

                Op->IncrementInProgress();
            }

            Y_ABORT_UNLESS(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

            auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);

            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxReadViaPipeline(" << GetTxType()
                << ") Execute with status# " << status << " at tablet# " << Self->TabletID());

            switch (status) {
                case EExecutionStatus::Restart:
                    return false;

                case EExecutionStatus::Reschedule:
                    // Reschedule transaction as soon as possible
                    if (!Op->IsExecutionPlanFinished()) {
                        Op->IncrementInProgress();
                        Self->ExecuteProgressTx(Op, ctx);
                    }
                    Op->DecrementInProgress();
                    break;

                case EExecutionStatus::Executed:
                case EExecutionStatus::Continue:
                    Op->DecrementInProgress();
                    break;

                case EExecutionStatus::WaitComplete:
                    WaitComplete = true;
                    break;

                default:
                    Y_FAIL_S("unexpected execution status " << status << " for operation "
                            << *Op << " " << Op->GetKind() << " at " << Self->TabletID());
            }

            if (WaitComplete || !CompleteList.empty()) {
                // Keep operation active until we run the complete list
            } else {
                // Release operation as it's no longer needed
                Op = nullptr;
            }

            return true;
        } catch (const TSchemeErrorTabletException&) {
            Y_ABORT();
        } catch (const TMemoryLimitExceededException&) {
            Y_ABORT("there must be no leaked exceptions: TMemoryLimitExceededException");
        } catch (const std::exception &e) {
            Y_ABORT("there must be no leaked exceptions: %s", e.what());
        } catch (...) {
            Y_ABORT("there must be no leaked exceptions");
        }
    }

    void ReplyError(Ydb::StatusIds::StatusCode code, const TString& message, ui32 nodeId, NWilson::TSpan &readSpan) {
        Reply = MakeEvReadResult(nodeId);
        SetStatusError(Reply->Record, code, message);
        Reply->Record.SetReadId(ReadId.ReadId);
        
        readSpan.EndError(message);
    }

    void Complete(const TActorContext& ctx) override {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxReadViaPipeline(" << GetTxType() << ") Complete"
            << ": at tablet# " << Self->TabletID());

        if (Reply) {
            Y_ABORT_UNLESS(!Op);
            auto it = Self->ReadIterators.find(ReadId);
            if (it != Self->ReadIterators.end()) {
                Y_ABORT_UNLESS(it->second);
                auto& state = *it->second;
                SendViaSession(state.SessionId, ReadId.Sender, Self->SelfId(), Reply.release());
                // ReadSpan is already ended in ReplyError.
                Self->DeleteReadIterator(it);
            }
            return;
        }

        if (!Op)
            return;

        Y_ABORT_UNLESS(!Op->GetExecutionPlan().empty());
        if (!CompleteList.empty()) {
            Self->Pipeline.RunCompleteList(Op, CompleteList, ctx);
        }

        if (WaitComplete) {
            Op->DecrementInProgress();

            if (!Op->IsInProgress() && !Op->IsExecutionPlanFinished()) {
                Self->Pipeline.AddCandidateOp(Op);

                if (Self->Pipeline.CanRunAnotherOp()) {
                    Self->PlanQueue.Progress(ctx);
                }
            }
        }
    }
};

class TDataShard::TTxReadContinue : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    TReadIteratorId ReadId;

    std::unique_ptr<TEvDataShard::TEvReadResult> Result;
    std::unique_ptr<IBlockBuilder> BlockBuilder;
    TShortTableInfo TableInfo;
    std::unique_ptr<TReader> Reader;
    bool DelayedResult = false;

public:
    TTxReadContinue(TDataShard* ds, const TReadIteratorId& readId, NWilson::TTraceId &&traceId)
        : TBase(ds, std::move(traceId))
        , ReadId(readId)
    {}

    // note that intentionally the same as TEvRead
    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        // note that we don't need to check shard state here:
        // 1. Since TTxReadContinue scheduled, shard was ready.
        // 2. If shards changes the state, it must cancel iterators and we will
        // not find our readId ReadIterators.
        auto it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // read has been aborted
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ReadContinue for iterator# " << ReadId
                << " didn't find state");
            return true;
        }

        Y_ASSERT(it->second);
        auto& state = *it->second;

        if (state.IsExhausted()) {
            // iterator quota reduced and exhausted while ReadContinue was inflight
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ReadContinue for iterator# " << ReadId
                << ", quota exhausted while rescheduling");
            state.ReadContinuePending = false;
            Result.reset();
            return true;
        }

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ReadContinue for iterator# " << ReadId
            << ", firstUnprocessedQuery# " << state.FirstUnprocessedQuery);

        Result = MakeEvReadResult(ctx.SelfID.NodeId());

        const auto& tableId = state.PathId.LocalPathId;
        if (state.PathId.OwnerId == Self->GetPathOwnerId()) {
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << state.PathId.LocalPathId
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                SendResult(ctx);
                return true;
            }
            auto userTableInfo = it->second;
            TableInfo = TShortTableInfo(userTableInfo);
            auto currentSchemaVersion = TableInfo.SchemaVersion;
            if (state.SchemaVersion != currentSchemaVersion) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Schema changed, current " << currentSchemaVersion
                        << ", requested table schemaversion " << state.SchemaVersion
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                SendResult(ctx);
                return true;
            }

            userTableInfo->Stats.AccessTime = TAppData::TimeProvider->Now();
        } else {
            auto schema = txc.DB.GetRowScheme(state.PathId.LocalPathId);
            if (!schema) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Failed to get scheme for table local id: "
                        << state.PathId.LocalPathId
                        << " (shard# " << Self->TabletID() << " node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                SendResult(ctx);
                return true;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        if (!state.ReadVersion.IsMax()) {
            bool snapshotFound = false;
            if (!state.IsHeadRead) {
                ui64 ownerId = state.PathId.OwnerId;
                TSnapshotKey snapshotKey(
                    ownerId,
                    tableId,
                    state.ReadVersion.Step,
                    state.ReadVersion.TxId);

                if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                    snapshotFound = true;
                }
            }

            if (!snapshotFound) {
                bool isMvccReadable = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
                if (!isMvccReadable) {
                    SetStatusError(
                        Result->Record,
                        Ydb::StatusIds::PRECONDITION_FAILED,
                        TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                            << state.ReadVersion << " shard " << Self->TabletID()
                            << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                            << (Self->IsFollower() ? " RO replica" : "")
                            << " (node# " << ctx.SelfID.NodeId() << " state# " << DatashardStateName(Self->State) << ")");
                    SendResult(ctx);
                    return true;
                }
            }
        }

        {
            auto p = CreateBlockBuilder(state, TableInfo);
            if (!p.first) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::BAD_REQUEST,
                    p.second);
                SendResult(ctx);
                return true;
            }
            std::swap(BlockBuilder, p.first);
        }

        Y_ASSERT(Result);

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
            << " ReadContinue: iterator# " << ReadId
            << ", FirstUnprocessedQuery# " << state.FirstUnprocessedQuery);

        TDataShardLocksDb locksDb(*Self, txc);
        TSetupSysLocks guardLocks(state.LockId, state.LockNodeId, *Self, &locksDb);

        Reader.reset(new TReader(
            state,
            *BlockBuilder,
            TableInfo,
            AppData()->MonotonicTimeProvider->Now(),
            Self));

        LWTRACK(ReadExecute, state.Orbit);
        if (Reader->Read(txc, ctx)) {
            // Retry later when dependencies are resolved
            if (!Reader->GetVolatileReadDependencies().empty()) {
                state.ReadContinuePending = true;
                Self->WaitVolatileDependenciesThenSend(
                    Reader->GetVolatileReadDependencies(),
                    Self->SelfId(),
                    std::make_unique<TEvDataShard::TEvReadContinue>(ReadId.Sender, ReadId.ReadId));
                return true;
            }

            ApplyLocks(ctx);

            if (!Reader->NeedVolatileWaitForCommit()) {
                SendResult(ctx);
            } else {
                DelayedResult = true;
            }
            return true;
        }
        return false;
    }

    void Complete(const TActorContext& ctx) override {
        if (DelayedResult) {
            SendResult(ctx);
        }
    }

    void ApplyLocks(const TActorContext& ctx) {
        auto it = Self->ReadIterators.find(ReadId);
        Y_ABORT_UNLESS(it != Self->ReadIterators.end());
        Y_ABORT_UNLESS(it->second);
        auto& state = *it->second;

        if (!Result) {
            return;
        }

        auto& record = Result->Record;
        if (record.HasStatus()) {
            return;
        }

        Y_ASSERT(Reader);

        if (state.Lock) {
            auto& sysLocks = Self->SysLocksTable();

            bool isBroken = state.Lock->IsBroken();
            if (!isBroken && (Reader->HadInvisibleRowSkips() || Reader->HadInconsistentResult())) {
                sysLocks.BreakLock(state.Lock->GetLockId());
                sysLocks.ApplyLocks();
                Y_ABORT_UNLESS(state.Lock->IsBroken());
                isBroken = true;
            }

            if (isBroken) {
                NKikimrDataEvents::TLock *addLock = record.AddBrokenTxLocks();
                addLock->SetLockId(state.Lock->GetLockId());
                addLock->SetDataShard(Self->TabletID());
                addLock->SetGeneration(state.Lock->GetGeneration());
                addLock->SetCounter(state.Lock->GetCounter(state.ReadVersion));
                addLock->SetSchemeShard(state.PathId.OwnerId);
                addLock->SetPathId(state.PathId.LocalPathId);

                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                    << " TTxReadContinue::Execute() found broken lock# " << state.Lock->GetLockId());

                // A broken write lock means we are reading inconsistent results and must abort
                if (state.Lock->IsWriteLock()) {
                    SetStatusError(record, Ydb::StatusIds::ABORTED, "Read conflict with concurrent transaction");
                    return;
                }

                state.Lock = nullptr;
            } else {
                // Lock valid, apply conflict changes
                auto locks = sysLocks.ApplyLocks();
                Y_ABORT_UNLESS(locks.empty(), "ApplyLocks acquired unexpected locks");
            }
        }
    }

    void SendResult(const TActorContext& ctx) {
        auto it = Self->ReadIterators.find(ReadId);
        Y_ABORT_UNLESS(it != Self->ReadIterators.end());
        Y_ABORT_UNLESS(it->second);
        auto& state = *it->second;

        state.ReadContinuePending = false;

        if (!Result) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " TTxReadContinue::Execute() finished without Result, aborting");

            Result = MakeEvReadResult(ctx.SelfID.NodeId());
            SetStatusError(Result->Record, Ydb::StatusIds::ABORTED, "Iterator aborted");
            Result->Record.SetReadId(ReadId.ReadId);
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);

            state.Request->ReadSpan.EndError("Iterator aborted");
            Self->DeleteReadIterator(it);
            return;
        }

        // error happened and status set
        auto& record = Result->Record;
        if (record.HasStatus()) {
            record.SetSeqNo(state.SeqNo + 1);
            record.SetReadId(ReadId.ReadId);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " TTxReadContinue::Execute() finished with error, aborting: " << record.DebugString());
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);

            state.Request->ReadSpan.EndError("Finished with error");
            Self->DeleteReadIterator(it);
            return;
        }

        Y_ASSERT(Reader);
        Y_ASSERT(BlockBuilder);

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " readContinue iterator# " << ReadId
            << " sends rowCount# " << Reader->GetRowsRead() << ", bytes# " << Reader->GetBytesRead()
            << ", quota rows left# " << (state.Quota.Rows - Reader->GetRowsRead())
            << ", quota bytes left# " << (state.Quota.Bytes - Reader->GetBytesRead())
            << ", hasUnreadQueries# " << Reader->HasUnreadQueries()
            << ", total queries# " << Reader->GetQueriesCount()
            << ", firstUnprocessed# " << state.FirstUnprocessedQuery);

        // Note: we only send useful non-empty results
        bool useful = Reader->FillResult(*Result, state);
        if (useful) {
            LWTRACK(ReadSendResult, state.Orbit);
            Self->SendImmediateReadResult(ReadId.Sender, Result.release(), 0, state.SessionId);
        }

        if (Reader->HasUnreadQueries()) {
            bool wasExhausted = state.IsExhausted();
            Reader->UpdateState(state, useful);
            if (!state.IsExhausted()) {
                state.ReadContinuePending = true;
                ctx.Send(
                    Self->SelfId(),
                    new TEvDataShard::TEvReadContinue(ReadId.Sender, ReadId.ReadId));
            } else if (!wasExhausted) {
                Self->IncCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT);
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                    << " read iterator# " << ReadId << " exhausted");
            }
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << ReadId
                << " finished in ReadContinue");
            
            state.Request->ReadSpan.EndOk();
            Self->DeleteReadIterator(it);
        }
    }
};

void TDataShard::Handle(TEvDataShard::TEvRead::TPtr& ev, const TActorContext& ctx) {
    // Note that we mutate this request below
    auto* request = ev->Get();
    
    if (!request->ReadSpan) {
        request->ReadSpan = NWilson::TSpan(TWilsonTablet::TabletTopLevel, std::move(ev->TraceId), "Datashard.Read", NWilson::EFlags::AUTO_END);
        request->ReadSpan.Attribute("Shard", std::to_string(TabletID()));
    }

    const auto& record = request->Record;
    if (Y_UNLIKELY(!record.HasReadId())) {
        TString msg = TStringBuilder() << "Missing ReadId at shard " << TabletID();
        
        auto result = MakeEvReadResult(ctx.SelfID.NodeId());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_REQUEST, msg);
        ctx.Send(ev->Sender, result.release());

        request->ReadSpan.EndError(msg);
        return;
    }

    LWTRACK(ReadRequest, request->Orbit, record.GetReadId());

    TReadIteratorId readId(ev->Sender, record.GetReadId());
    if (!Pipeline.HandleWaitingReadIterator(readId, request)) {
        // This request has been cancelled
        request->ReadSpan.EndError("Cancelled");
        return;
    }

    auto replyWithError = [&] (auto code, const auto& msg) {
        auto result = MakeEvReadResult(ctx.SelfID.NodeId());
        
        SetStatusError(
            result->Record,
            code,
            msg);
        result->Record.SetReadId(readId.ReadId);
        ctx.Send(ev->Sender, result.release());

        request->ReadSpan.EndError(msg);
    };

    if (Y_UNLIKELY(Pipeline.HasWaitingReadIterator(readId) || ReadIterators.contains(readId))) {
        replyWithError(
            Ydb::StatusIds::ALREADY_EXISTS,
            TStringBuilder() << "Request " << readId.ReadId << " already executing at shard " << TabletID());
        return;
    }

    if (!IsStateNewReadAllowed()) {
        replyWithError(
            Ydb::StatusIds::OVERLOADED,
            TStringBuilder() << "Shard " << TabletID() << " is splitting/merging"
                << " (node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    if (MediatorStateWaiting) {
        // TODO: save span
        LWTRACK(ReadWaitMediatorState, request->Orbit);
        Pipeline.RegisterWaitingReadIterator(readId, request);
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (Pipeline.HasProposeDelayers()) {
        // TODO: save span
        LWTRACK(ReadWaitProposeDelayers, request->Orbit);
        Pipeline.RegisterWaitingReadIterator(readId, request);
        DelayedProposeQueue.emplace_back().Reset(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (Pipeline.HasDrop()) {
        replyWithError(
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "Request " << readId.ReadId << " rejected, because pipeline is in process of drop"
                << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    size_t totalInFly = ReadIteratorsInFly() + TxInFly() + ImmediateInFly()
        + MediatorStateWaitingMsgs.size() + ProposeQueue.Size() + TxWaiting();

    if (totalInFly > GetMaxTxInFly()) {
        replyWithError(
            Ydb::StatusIds::OVERLOADED,
            TStringBuilder() << "Request " << readId.ReadId << " rejected, MaxTxInFly was exceeded"
                << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    if (!request->Keys.empty() && !request->Ranges.empty()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
            << "Both keys and ranges are forbidden"
            << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    if (request->Keys.empty() && request->Ranges.empty()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
            << "Neither keys nor ranges specified"
            << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    if (record.HasProgram()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
            << "PushDown is not supported"
            << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        return;
    }

    TRowVersion readVersion = TRowVersion::Max();
    bool isHeadRead = true;
    if (record.HasSnapshot()) {
        readVersion.Step = record.GetSnapshot().GetStep();
        readVersion.TxId = record.GetSnapshot().GetTxId();
        if (readVersion.Step == Max<ui64>()) {
            replyWithError(
                Ydb::StatusIds::UNSUPPORTED, TStringBuilder()
                << "invalid snapshot value specified"
                << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
            return;
        }
        isHeadRead = false;
    }

    if (MvccSwitchState == TSwitchState::SWITCHING) {
        Pipeline.AddWaitingReadIterator(readVersion, std::move(ev), ctx);
        return;
    }

    TActorId sessionId;
    if (readId.Sender.NodeId() != SelfId().NodeId()) {
        Y_DEBUG_ABORT_UNLESS(ev->InterconnectSession);
        THashMap<TActorId, TReadIteratorSession>::insert_ctx sessionsInsertCtx;
        auto itSession = ReadIteratorSessions.find(ev->InterconnectSession, sessionsInsertCtx);
        if (itSession == ReadIteratorSessions.end()) {
            Send(ev->InterconnectSession, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
            itSession = ReadIteratorSessions.emplace_direct(
                sessionsInsertCtx,
                ev->InterconnectSession,
                TReadIteratorSession());
        }

        auto& session = itSession->second;
        session.Iterators.insert(readId);
        sessionId = ev->InterconnectSession;
    }

    auto pr = ReadIterators.emplace(
        readId,
        new TReadIteratorState(
            readId, TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()),
            sessionId, readVersion, isHeadRead,
            AppData()->MonotonicTimeProvider->Now(), std::move(request->Orbit)));
    Y_ABORT_UNLESS(pr.second);

    auto& state = *pr.first->second;
    state.Ev = std::move(ev);
    state.Request = request;

    SetCounter(COUNTER_READ_ITERATORS_COUNT, ReadIterators.size());

    Executor()->Execute(new TTxReadViaPipeline(this, readId, request->ReadSpan.GetTraceId()), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvReadContinue::TPtr& ev, const TActorContext& ctx) {
    TReadIteratorId readId(ev->Get()->Reader, ev->Get()->ReadId);
    auto it = ReadIterators.find(readId);
    if (Y_UNLIKELY(it == ReadIterators.end())) {
        return;
    }

    Executor()->Execute(new TTxReadContinue(this, readId, it->second->Request->ReadSpan.GetTraceId()), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvReadAck::TPtr& ev, const TActorContext& ctx) {
    // Possible cases:
    // 1. read exhausted and we need to start its execution (if bytes available again),
    // can start transaction right from here.
    // 2. read is in progress, we need just to update quota.
    // 3. we have become non-active and ignore.

    if (!IsStateActive()) {
        return;
    }

    const auto& record = ev->Get()->Record;
    if (Y_UNLIKELY(!record.HasReadId() || !record.HasSeqNo() ||
        !record.HasMaxRows() || !record.HasMaxBytes()))
    {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " ReadAck: " << record);

        auto result = MakeEvReadResult(ctx.SelfID.NodeId());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
            << "Missing mandatory fields in TEvReadAck"
            << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")");
        if (record.HasReadId())
            result->Record.SetReadId(record.GetReadId());
        ctx.Send(ev->Sender, result.release());
        return;
    }

    TReadIteratorId readId(ev->Sender, record.GetReadId());

    auto it = ReadIterators.find(readId);
    if (it == ReadIterators.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
            << " ReadAck from " << ev->Sender << " on missing iterator: " << record);
        return;
    }

    Y_ASSERT(it->second);
    auto& state = *it->second;
    if (state.State == NDataShard::TReadIteratorState::EState::Init) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
            << " ReadAck on not inialized iterator: " << record);

        return;
    }

    LWTRACK(ReadAck, state.Orbit);

    // We received ACK on message we hadn't sent yet
    if (state.SeqNo < record.GetSeqNo()) {
        auto issueStr = TStringBuilder() << TabletID() << " ReadAck from future: " << record.GetSeqNo()
            << ", current seqNo# " << state.SeqNo
            << " (shard# " << TabletID() << " node# " << SelfId().NodeId() << " state# " << DatashardStateName(State) << ")";
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, issueStr);

        auto result = MakeEvReadResult(ctx.SelfID.NodeId());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_SESSION, issueStr);
        result->Record.SetReadId(readId.ReadId);
        SendViaSession(state.SessionId, readId.Sender, SelfId(), result.release());

        // We definitely have Request in the read iterator state, because it's state is not Init.
        state.Request->ReadSpan.EndError(issueStr);
        DeleteReadIterator(it);
        return;
    }

    if (state.LastAckSeqNo && state.LastAckSeqNo >= record.GetSeqNo()) {
        // out of order, ignore
        return;
    }

    bool wasExhausted = state.IsExhausted();
    state.UpQuota(
        record.GetSeqNo(),
        record.HasMaxRows() ? record.GetMaxRows() : Max<ui64>(),
        record.HasMaxBytes() ? record.GetMaxBytes() : Max<ui64>());

    if (wasExhausted && !state.IsExhausted()) {
        DecCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT);
        if (!state.ReadContinuePending) {
            state.ReadContinuePending = true;
            ctx.Send(
                SelfId(),
                new TEvDataShard::TEvReadContinue(ev->Sender, record.GetReadId()));
        }
    } else if (!wasExhausted && state.IsExhausted()) {
        IncCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT);
    }

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " ReadAck for read iterator# " << readId
        << ": " << record << ", " << (wasExhausted ? "read continued" : "quota updated")
        << ", bytesLeft# " << state.Quota.Bytes << ", rowsLeft# " << state.Quota.Rows);
}

void TDataShard::Handle(TEvDataShard::TEvReadCancel::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    if (!record.HasReadId())
        return;

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " ReadCancel: " << record);

    TReadIteratorId readId(ev->Sender, record.GetReadId());
    if (Pipeline.CancelWaitingReadIterator(readId)) {
        Y_ABORT_UNLESS(!ReadIterators.contains(readId));
        return;
    }

    auto it = ReadIterators.find(readId);
    if (it == ReadIterators.end())
        return;

    const auto& state = it->second;
    if (!state->IsFinished) {
        auto now = AppData()->MonotonicTimeProvider->Now();
        auto delta = now - state->StartTs;
        IncCounter(COUNTER_READ_ITERATOR_LIFETIME_MS, delta.MilliSeconds());
        IncCounter(COUNTER_READ_ITERATOR_CANCEL);
    }

    LWTRACK(ReadCancel, state->Orbit);

    state->Request->ReadSpan.EndError("Cancelled");
    DeleteReadIterator(it);
}

void TDataShard::CancelReadIterators(Ydb::StatusIds::StatusCode code, const TString& issue, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " CancelReadIterators#" << ReadIterators.size());

    auto now = AppData()->MonotonicTimeProvider->Now();
    for (const auto& iterator: ReadIterators) {
        const auto& readIteratorId = iterator.first;

        const auto& state = iterator.second;
        if (!state->IsFinished) {
            auto delta = now - state->StartTs;
            IncCounter(COUNTER_READ_ITERATOR_LIFETIME_MS, delta.MilliSeconds());
        }

        auto result = MakeEvReadResult(ctx.SelfID.NodeId());
        SetStatusError(result->Record, code, issue);
        result->Record.SetReadId(iterator.first.ReadId);
        result->Record.SetSeqNo(state->SeqNo + 1);

        SendViaSession(state->SessionId, readIteratorId.Sender, SelfId(), result.release());
        state->Request->ReadSpan.EndError("Cancelled");
    }

    ReadIterators.clear();
    UnsubscribeReadIteratorSessions(ctx);

    SetCounter(COUNTER_READ_ITERATORS_COUNT, 0);
    SetCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT, 0);
}

void TDataShard::DeleteReadIterator(TReadIteratorsMap::iterator it) {
    const auto& state = it->second;
    if (state->SessionId) {
        auto itSession = ReadIteratorSessions.find(state->SessionId);
        if (itSession != ReadIteratorSessions.end()) {
            auto& session = itSession->second;
            session.Iterators.erase(it->first);
        }
    }
    if (state->IsExhausted()) {
        DecCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT);
    }
    ReadIterators.erase(it);
    SetCounter(COUNTER_READ_ITERATORS_COUNT, ReadIterators.size());
}

void TDataShard::ReadIteratorsOnNodeDisconnected(const TActorId& sessionId, const TActorContext &ctx) {
    auto itSession = ReadIteratorSessions.find(sessionId);
    if (itSession == ReadIteratorSessions.end())
        return;

    const auto& session = itSession->second;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
        << " closed session# " << sessionId << ", iterators# " << session.Iterators.size());

    auto now = AppData()->MonotonicTimeProvider->Now();
    ui64 exhaustedCount = 0;
    for (const auto& readId: session.Iterators) {
        // we don't send anything to client, because it's up
        // to client to detect disconnect
        auto it = ReadIterators.find(readId);
        if (it == ReadIterators.end())
            continue;

        const auto& state = it->second;
        if (!state->IsFinished) {
            auto delta = now - state->StartTs;
            IncCounter(COUNTER_READ_ITERATOR_LIFETIME_MS, delta.MilliSeconds());
        }

        if (state->IsExhausted()) {
            ++exhaustedCount;
        }

        state->Request->ReadSpan.EndError("Disconnected");
        ReadIterators.erase(it);
    }

    ReadIteratorSessions.erase(itSession);
    SetCounter(COUNTER_READ_ITERATORS_COUNT, ReadIterators.size());
    DecCounter(COUNTER_READ_ITERATORS_EXHAUSTED_COUNT, exhaustedCount);
}

void TDataShard::UnsubscribeReadIteratorSessions(const TActorContext& ctx) {
    Y_UNUSED(ctx);
    for (const auto& pr : ReadIteratorSessions) {
        Send(pr.first, new TEvents::TEvUnsubscribe);
    }
    ReadIteratorSessions.clear();
}

void TDataShard::IncCounterReadIteratorLastKeyReset() {
    if (!CounterReadIteratorLastKeyReset) {
        CounterReadIteratorLastKeyReset = GetServiceCounters(AppData()->Counters, "tablets")
            ->GetSubgroup("type", "DataShard")
            ->GetSubgroup("category", "app")
            ->GetCounter("DataShard/ReadIteratorLastKeyReset", true);
    }
    ++*CounterReadIteratorLastKeyReset;
}

} // NKikimr::NDataShard

template<>
inline void Out<NKikimr::NDataShard::TReadIteratorId>(
    IOutputStream& o,
    const NKikimr::NDataShard::TReadIteratorId& info)
{
    o << info.ToString();
}
