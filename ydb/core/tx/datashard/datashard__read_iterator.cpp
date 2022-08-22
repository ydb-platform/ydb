#include "datashard_impl.h"
#include "datashard_read_operation.h"

#include <ydb/core/formats/arrow_batch_builder.h>

#include <util/system/hp_timer.h>

#include <utility>

namespace NKikimr::NDataShard {

using namespace NTabletFlatExecutor;

namespace {

constexpr ui64 MinRowsPerCheck = 1000;

class TCellBlockBuilder : public IBlockBuilder {
public:
    bool Start(
        const TVector<std::pair<TString,  NScheme::TTypeId>>& columns,
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

        Rows.emplace_back(value.Cells());
        BytesCount += Rows.back().DataSize();
    }

    TString Finish() override {
        return TString();
    }

    size_t Bytes() const override { return BytesCount; }

public:
    TVector<TOwnedCellVec> FlushBatch() { return std::move(Rows); }

private:
    TVector<std::pair<TString,  NScheme::TTypeId>> Columns;

    TVector<TOwnedCellVec> Rows;
    ui64 BytesCount = 0;

    std::unique_ptr<IBlockBuilder> Clone() const override {
        return nullptr;
    }
};

struct TShortColumnInfo {
    NTable::TTag Tag;
    NScheme::TTypeId Type;
    TString Name;

    TShortColumnInfo(NTable::TTag tag, NScheme::TTypeId type, const TString& name)
        : Tag(tag)
        , Type(type)
        , Name(name)
    {}

    TString Dump() const {
        TStringStream ss;
        ss << "{Tag: " << Tag << ", Type: " << Type << ", Name: " << Name << "}";
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
            KeyColumnTypes.push_back(type.GetTypeId());
        }

        // note that we don't have column names here, but
        // for cellvec we will not need them at all
        for (const auto& col: schema.Cols) {
            Columns.emplace(col.Tag, TShortColumnInfo(col.Tag, col.TypeId, ""));
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
    TVector<NScheme::TTypeId> KeyColumnTypes;
    TMap<NTable::TTag, TShortColumnInfo> Columns;
};

TVector<std::pair<TString, NScheme::TTypeId>> GetNameTypeColumns(
    const std::vector<NTable::TTag>& tags,
    const TShortTableInfo& tableInfo)
{
    TVector<std::pair<TString, NScheme::TTypeId>> result;
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

    auto nameTypeCols = GetNameTypeColumns(state.Columns, tableInfo);
    if (nameTypeCols.empty()) {
        error = "Wrong columns requested";
        return std::make_pair(nullptr, error);
    }

    switch (state.Format) {
    case NKikimrTxDataShard::EScanDataFormat::ARROW:
        blockBuilder.reset(new NArrow::TArrowBatchBuilder());
        break;
    case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
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
    return TSerializedCellVec(TSerializedCellVec::Serialize(extendedCells));
}

ui64 ResetRowStats(NTable::TIteratorStats& stats)
{
    return std::exchange(stats.DeletedRowSkips, 0UL) +
        std::exchange(stats.InvisibleRowSkips, 0UL);
}

// nota that reader captures state reference and must be used only
// after checking that state is still alife, i.e. read can be aborted
// between Execute() and Complete()
class TReader {
    const TReadIteratorState& State;
    IBlockBuilder& BlockBuilder;
    const TShortTableInfo& TableInfo;

    std::vector<NKikimr::NScheme::TTypeId> ColumnTypes;

    ui32 FirstUnprocessedQuery;
    TString LastProcessedKey;

    ui64 RowsRead = 0;
    ui64 RowsSinceLastCheck = 0;

    ui64 BytesInResult = 0;

    bool InvisibleRowSkipsMet = false;

    NHPTimer::STime StartTime;
    NHPTimer::STime EndTime;

    static const NHPTimer::STime MaxCyclesPerIteration;

    enum class EReadStatus {
        Done = 0,
        NeedData,
        StoppedByLimit,
    };

public:
    TReader(TReadIteratorState& state,
            IBlockBuilder& blockBuilder,
            const TShortTableInfo& tableInfo)
        : State(state)
        , BlockBuilder(blockBuilder)
        , TableInfo(tableInfo)
        , FirstUnprocessedQuery(State.FirstUnprocessedQuery)
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
        TSerializedCellVec keyFromCells;
        if (Y_UNLIKELY(FirstUnprocessedQuery == State.FirstUnprocessedQuery && State.LastProcessedKey)) {
            fromInclusive = false;
            keyFromCells = TSerializedCellVec(State.LastProcessedKey);
        } else {
            fromInclusive = range.FromInclusive;
            keyFromCells = range.From;
        }
        const auto keyFrom = ToRawTypeValue(keyFromCells, TableInfo, fromInclusive);

        const TSerializedCellVec keyToCells(range.To);
        const auto keyTo = ToRawTypeValue(keyToCells, TableInfo, !range.ToInclusive);

        // TODO: split range into parts like in read_columns

        NTable::TKeyRange iterRange;
        iterRange.MinKey = keyFrom;
        iterRange.MaxKey = keyTo;
        iterRange.MinInclusive = fromInclusive;
        iterRange.MaxInclusive = range.ToInclusive;
        bool reverse = State.Reverse;

        EReadStatus result;
        if (!reverse) {
            auto iter = txc.DB.IterateRange(TableInfo.LocalTid, iterRange, State.Columns, State.ReadVersion);
            result = Iterate(iter.Get(), true, ctx);
        } else {
            auto iter = txc.DB.IterateRangeReverse(TableInfo.LocalTid, iterRange, State.Columns, State.ReadVersion);
            result = Iterate(iter.Get(), true, ctx);
        }

        if (result == EReadStatus::NeedData) {
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
            // key prefix, treat it as range [prefix, 0, 0] - [prefix, +inf, +inf]
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
        auto ready = txc.DB.Select(TableInfo.LocalTid, key, State.Columns, rowState, stats, 0, State.ReadVersion);
        RowsSinceLastCheck += 1 + stats.InvisibleRowSkips;
        InvisibleRowSkipsMet |= stats.InvisibleRowSkips > 0;
        if (ready == NTable::EReady::Page) {
            return EReadStatus::NeedData;
        }
        if (ready == NTable::EReady::Gone) {
            return EReadStatus::Done;
        }

        // TODO: looks kind of ugly: we assume that cells in rowState are stored in array
        TDbTupleRef value(&ColumnTypes[0], &rowState.Get(0), ColumnTypes.size());

        // note that if user requests key columns then they will be in
        // rowValues and we don't have to add rowKey columns
        BlockBuilder.AddRow(TDbTupleRef(), value);
        ++RowsRead;

        return EReadStatus::Done;
    }

    // TODO: merge ReadRanges and ReadKeys to single template Read?

    bool ReadRanges(TTransactionContext& txc, const TActorContext& ctx) {
        for (; FirstUnprocessedQuery < State.Request->Ranges.size(); ++FirstUnprocessedQuery) {
            if (ShouldStop())
                return true;

            const auto& range = State.Request->Ranges[FirstUnprocessedQuery];
            auto status = ReadRange(txc, ctx, range);
            switch (status) {
            case EReadStatus::Done:
                continue;
            case EReadStatus::StoppedByLimit:
                return true;
            case EReadStatus::NeedData:
                return false;
            }
        }

        return true;
    }

    bool ReadKeys(TTransactionContext& txc, const TActorContext& ctx) {
        for (; FirstUnprocessedQuery < State.Request->Keys.size(); ++FirstUnprocessedQuery) {
            if (ShouldStop())
                return true;

            const auto& key = State.Request->Keys[FirstUnprocessedQuery];
            auto status = ReadKey(txc, ctx, key, FirstUnprocessedQuery);
            switch (status) {
            case EReadStatus::Done:
                continue;
            case EReadStatus::StoppedByLimit:
                return true;
            case EReadStatus::NeedData:
                return false;
            }
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

    void FillResult(TEvDataShard::TEvReadResult& result) {
        auto& record = result.Record;
        record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        if (HasUnreadQueries()) {
            if (OutOfQuota()) {
                record.SetLimitReached(true);
            }
        } else {
            record.SetFinished(true);
        }

        BytesInResult = BlockBuilder.Bytes();

        if (BytesInResult = BlockBuilder.Bytes()) {
            switch (State.Format) {
            case NKikimrTxDataShard::ARROW: {
                auto& arrowBuilder = static_cast<NArrow::TArrowBatchBuilder&>(BlockBuilder);
                result.ArrowBatch = arrowBuilder.FlushBatch(false);
                break;
            }
            case NKikimrTxDataShard::CELLVEC: {
                auto& cellBuilder = static_cast<TCellBlockBuilder&>(BlockBuilder);
                result.SetRows(cellBuilder.FlushBatch());
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

        record.MutableSnapshot()->SetStep(State.ReadVersion.Step);
        record.MutableSnapshot()->SetTxId(State.ReadVersion.TxId);

        NKikimrTxDataShard::TReadContinuationToken continuationToken;
        continuationToken.SetFirstUnprocessedQuery(FirstUnprocessedQuery);

        // note that when LastProcessedKey set then
        // FirstUnprocessedQuery is definitely partially read range
        if (LastProcessedKey)
            continuationToken.SetLastProcessedKey(LastProcessedKey);

        bool res = continuationToken.SerializeToString(record.MutableContinuationToken());
        Y_ASSERT(res);
    }

    void UpdateState(TReadIteratorState& state) {
        state.FirstUnprocessedQuery = FirstUnprocessedQuery;
        state.LastProcessedKey = LastProcessedKey;
        state.ConsumeSeqNo(RowsRead, BytesInResult);
    }

    ui64 GetRowsRead() const { return RowsRead; }
    bool HasInvisibleRowSkips() const { return InvisibleRowSkipsMet; }

private:
    bool OutOfQuota() const {
        return RowsRead >= State.Quota.Rows ||
            BlockBuilder.Bytes() >= State.Quota.Bytes||
            BytesInResult >= State.Quota.Bytes;
    }

    bool HasMaxRowsInResult() const {
        return RowsRead >= State.MaxRowsInResult;
    }

    bool ShouldStop() {
        return OutOfQuota() || HasMaxRowsInResult() || ShouldStopByElapsedTime();
    }

    bool Precharge(
        NTable::TDatabase& db,
        NTable::TRawVals keyFrom,
        NTable::TRawVals keyTo,
        bool reverse)
    {
        Y_ASSERT(RowsRead < State.Quota.Rows);
        Y_ASSERT(BlockBuilder.Bytes() < State.Quota.Bytes);

        ui64 rowsLeft = State.Quota.Rows - RowsRead;
        ui64 bytesLeft = State.Quota.Bytes - BlockBuilder.Bytes();

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
    EReadStatus Iterate(TIterator* iter, bool isRange, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        while (iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
            TDbTupleRef rowKey = iter->GetKey();

            if (isRange) {
                LastProcessedKey = TSerializedCellVec::Serialize(rowKey.Cells());
            }

            TDbTupleRef rowValues = iter->GetValues();

            // note that if user requests key columns then they will be in
            // rowValues and we don't have to add rowKey columns
            BlockBuilder.AddRow(TDbTupleRef(), rowValues);

            ++RowsRead;
            InvisibleRowSkipsMet |= iter->Stats.InvisibleRowSkips > 0;
            RowsSinceLastCheck += 1 + ResetRowStats(iter->Stats);
            if (ShouldStop()) {
                return EReadStatus::StoppedByLimit;
            }
        }

        // last iteration to Page or Gone also might have deleted or invisible rows
        RowsSinceLastCheck += ResetRowStats(iter->Stats);
        InvisibleRowSkipsMet |= iter->Stats.InvisibleRowSkips > 0;

        // TODO: consider restart when Page and too few data read
        // (how much is too few, less than user's limit?)
        if (iter->Last() == NTable::EReady::Page && RowsRead == 0) {
            return EReadStatus::NeedData;
        }

        // range fully read, no reason to keep LastProcessedKey
        if (isRange && iter->Last() == NTable::EReady::Gone)
            LastProcessedKey.clear();

        return EReadStatus::Done;
    }
};

const NHPTimer::STime TReader::MaxCyclesPerIteration =
    /* 10ms */ (NHPTimer::GetCyclesPerSecond() + 99) / 100;

} // namespace

class TDataShard::TReadOperation : public TOperation, public IReadOperation {
    TDataShard* Self;
    TActorId Sender;
    std::shared_ptr<TEvDataShard::TEvRead> Request;

    NMiniKQL::IEngineFlat::TValidationInfo ValidationInfo;

    size_t ExecuteCount = 0;
    bool ResultSent = false;

    std::unique_ptr<TEvDataShard::TEvReadResult> Result;

    std::unique_ptr<IBlockBuilder> BlockBuilder;
    TShortTableInfo TableInfo;
    std::unique_ptr<TReader> Reader;

    static constexpr ui32 Flags = NTxDataShard::TTxFlags::ReadOnly | NTxDataShard::TTxFlags::Immediate;

public:
    TReadOperation(TDataShard* ds, ui64 txId, TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvRead::TPtr ev)
        : TOperation(TBasicOpInfo(txId, EOperationKind::ReadTx, Flags, 0, receivedAt, tieBreakerIndex))
        , Self(ds)
        , Sender(ev->Sender)
        , Request(ev->Release().Release())
    {}

    void BuildExecutionPlan(bool loaded) override
    {
        Y_VERIFY(GetExecutionPlan().empty());
        Y_VERIFY(!loaded);

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

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        TReadIteratorId readId(Sender, Request->Record.GetReadId());
        auto it = Self->ReadIterators.find(readId);
        if (it == Self->ReadIterators.end()) {
            // iterator has been aborted
            return true;
        }
        Y_VERIFY(it->second);
        auto& state = *it->second;
        Y_VERIFY(state.State == TReadIteratorState::EState::Executing);

        ++ExecuteCount;
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Execute read# " << ExecuteCount
            << ", request: " << Request->Record);

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
                        << ", tablet id: " << Self->TabletID());
                return true;
            } else {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Shard in state " << DatashardStateName(Self->State)
                        << ", will be deleted soon, tablet id: " << Self->TabletID());
                return true;
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
                    << ", tablet id: " << Self->TabletID());
            return true;
        }
        case TShardState::Uninitialized:
        case TShardState::WaitScheme:
        case TShardState::Unknown:
        default:
            SetStatusError(
                Result->Record,
                Ydb::StatusIds::INTERNAL_ERROR,
                TStringBuilder() << "Wrong shard state: " << DatashardStateName(Self->State)
                    << ", tablet id: " << Self->TabletID());
            return true;
        }

        // we need to check that scheme version is still correct, table presents and
        // version is still available

        if (state.PathId.OwnerId != Self->TabletID()) {
            // owner is schemeshard, read user table
            auto tableId = state.PathId.LocalPathId;
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << tableId);
                return true;
            }
            auto& userTableInfo = it->second;

            const ui64 ownerId = state.PathId.OwnerId;
            TSnapshotKey snapshotKey(
                ownerId,
                tableId,
                state.ReadVersion.Step,
                state.ReadVersion.TxId);

            bool isMvccVersion = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
            bool allowMvcc = isMvccVersion && !Self->IsFollower();
            bool snapshotFound = false;
            if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                // TODO: do we need to acquire?
                snapshotFound = true;
            } else if (allowMvcc) {
                snapshotFound = true;
            }

            if (!snapshotFound) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                         << state.ReadVersion << " shard " << Self->TabletID()
                         << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                         << (Self->IsFollower() ? " RO replica" : ""));
                return true;
            }

            if (state.SchemaVersion != userTableInfo->GetTableSchemaVersion()) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Schema changed, current " << userTableInfo->GetTableSchemaVersion()
                        << ", requested table schemaversion " << state.SchemaVersion);
                return true;
            }
        }

        if (!Read(txc, ctx, state))
            return false;

        if (Request->Record.HasLockTxId()) {
            // note that we set locks only when first read finish transaction,
            // i.e. we have read something without page faults
            AcquireLock(ctx, state);
        }

        Self->PromoteImmediatePostExecuteEdges(state.ReadVersion, TDataShard::EPromotePostExecuteEdges::ReadOnly, txc);
        return true;
    }

    void CheckRequestAndInit(TTransactionContext& txc, const TActorContext& ctx) override {
        TReadIteratorId readId(Sender, Request->Record.GetReadId());
        auto it = Self->ReadIterators.find(readId);
        if (it == Self->ReadIterators.end()) {
            // iterator has been aborted
            Abort(EExecutionUnitKind::CompletedOperations);
            return;
        }
        Y_VERIFY(it->second);
        auto& state = *it->second;
        Y_VERIFY(state.State == TReadIteratorState::EState::Init);

        Result.reset(new TEvDataShard::TEvReadResult());

        const auto& record = Request->Record;

        state.ReadId = record.GetReadId();
        state.PathId = TPathId(
            record.GetTableId().GetOwnerId(),
            record.GetTableId().GetTableId());

        if (record.HasMaxRows())
            state.Quota.Rows = record.GetMaxRows();

        if (record.HasMaxBytes())
            state.Quota.Bytes = record.GetMaxBytes();

        if (record.HasResultFormat())
            state.Format = record.GetResultFormat();

        if (record.HasMaxRowsInResult())
            state.MaxRowsInResult = record.GetMaxRowsInResult();

        if (record.HasSnapshot()) {
            state.ReadVersion.Step = record.GetSnapshot().GetStep();
            state.ReadVersion.TxId = record.GetSnapshot().GetTxId();
        }

        state.Reverse = record.GetReverse();

        if (state.PathId.OwnerId != Self->TabletID()) {
            // owner is schemeshard, read user table
            if (state.PathId.OwnerId != Self->GetPathOwnerId()) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Requesting ownerId: " << state.PathId.OwnerId
                        << ", tableId: " << state.PathId.LocalPathId
                        << ", from wrong owner: " << Self->GetPathOwnerId());
                return;
            }

            const auto tableId = state.PathId.LocalPathId;
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << tableId);
                return;
            }

            auto& userTableInfo = it->second;
            TableInfo = TShortTableInfo(userTableInfo);

            if (userTableInfo->IsBackup) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::BAD_REQUEST,
                    "Can't read from a backup table");
                return;
            }

            // we must have chosen the version
            Y_VERIFY(!state.ReadVersion.IsMax());

            const ui64 ownerId = state.PathId.OwnerId;
            TSnapshotKey snapshotKey(
                ownerId,
                tableId,
                state.ReadVersion.Step,
                state.ReadVersion.TxId);

            bool isMvccVersion = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
            bool allowMvcc = isMvccVersion && !Self->IsFollower();
            bool snapshotFound = false;
            if (Self->GetSnapshotManager().FindAvailable(snapshotKey)) {
                // TODO: do we need to acquire?
                snapshotFound = true;
                SetUsingSnapshotFlag();
            } else if (allowMvcc) {
                snapshotFound = true;
                SetMvccSnapshot(TRowVersion(state.ReadVersion.Step, state.ReadVersion.TxId));
            }

            if (!snapshotFound) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Table id " << tableId << " has no snapshot at "
                         << state.ReadVersion << " shard " << Self->TabletID()
                         << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                         << (Self->IsFollower() ? " RO replica" : ""));
                return;
            }

            state.SchemaVersion = userTableInfo->GetTableSchemaVersion();
            if (record.GetTableId().HasSchemaVersion()) {
                if (state.SchemaVersion != record.GetTableId().GetSchemaVersion()) {
                    SendErrorAndAbort(
                        ctx,
                        state,
                        Ydb::StatusIds::SCHEME_ERROR,
                        TStringBuilder() << "Wrong schemaversion " << record.GetTableId().GetSchemaVersion()
                            << " requested, table schemaversion " << state.SchemaVersion);
                    return;
                 }
            }

            // TODO: remove later, when we sure that key prefix is properly
            // interpreted same way everywhere: i.e. treated as 0 at the left and
            // inf on the right.
            // We really do weird transformations here, not sure if we can do better though
            for (size_t i = 0; i < Request->Ranges.size(); ++i) {
                auto& range = Request->Ranges[i];
                auto& keyFrom = range.From;
                auto& keyTo = Request->Ranges[i].To;

                if (range.FromInclusive && keyFrom.GetCells().size() != TableInfo.KeyColumnCount) {
                    keyFrom = ExtendWithNulls(keyFrom, TableInfo.KeyColumnCount);
                }

                if (!range.ToInclusive && keyTo.GetCells().size() != TableInfo.KeyColumnCount) {
                    keyTo = ExtendWithNulls(keyTo, TableInfo.KeyColumnCount);
                }
            }

            // TODO: remove later, when we sure that key prefix is properly
            // interpreted same way everywhere: i.e. treated as 0 at the left and
            // inf on the right.
            // We really do weird transformations here, not sure if we can do better though
            for (size_t i = 0; i < Request->Keys.size(); ++i) {
                const auto& key = Request->Keys[i];
                if (key.GetCells().size() == TableInfo.KeyColumnCount)
                    continue;

                if (state.Keys.size() != Request->Keys.size()) {
                    state.Keys.resize(Request->Keys.size());
                }

                // we can safely use cells referencing original Request->Keys[x],
                // because request will live until the end
                state.Keys[i] = ExtendWithNulls(key, TableInfo.KeyColumnCount);
            }

            userTableInfo->Stats.AccessTime = TAppData::TimeProvider->Now();
        } else {
            // DS is owner, read system table

            auto schema = txc.DB.GetRowScheme(state.PathId.LocalPathId);
            if (!schema) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Failed to get scheme for table local id: "
                        << state.PathId.LocalPathId);
                return;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        state.Columns.reserve(record.ColumnsSize());
        for (auto col: record.GetColumns()) {
            auto it = TableInfo.Columns.find(col);
            if (it == TableInfo.Columns.end()) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Unknown column: " << col);
                return;
            }

            state.Columns.push_back(col);
        }

        {
            auto p = CreateBlockBuilder(state, TableInfo);
            if (!p.first) {
                SendErrorAndAbort(
                    ctx,
                    state,
                    Ydb::StatusIds::BAD_REQUEST,
                    p.second);
                return;
            }
            std::swap(BlockBuilder, p.first);
        }

        state.Request = Request;

        Y_ASSERT(Result);

        state.State = TReadIteratorState::EState::Executing;
        Reader.reset(new TReader(state, *BlockBuilder, TableInfo));

        PrepareValidationInfo(ctx, state);
    }

    void SendResult(const TActorContext& ctx) override {
        if (ResultSent)
            return;
        ResultSent = true;

        TReadIteratorId readId(Sender, Request->Record.GetReadId());
        auto it = Self->ReadIterators.find(readId);
        if (it == Self->ReadIterators.end()) {
            // the one who removed the iterator should have reply to user
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " has been invalidated before TReadOperation::SendResult()");
            return;
        }

        Y_VERIFY(it->second);
        auto& state = *it->second;

        if (!Result) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " TReadOperation::Execute() finished without Result, aborting");
            Self->DeleteReadIterator(it);

            Result.reset(new TEvDataShard::TEvReadResult());
            SetStatusError(Result->Record, Ydb::StatusIds::ABORTED, "Iterator aborted");
            Result->Record.SetReadId(readId.ReadId);
            Self->SendImmediateReadResult(Sender, Result.release(), 0, state.SessionId);
            return;
        }

        // error happened and status set
        auto& record = Result->Record;
        if (record.HasStatus()) {
            record.SetReadId(readId.ReadId);
            record.SetSeqNo(state.SeqNo + 1);
            Self->SendImmediateReadResult(Sender, Result.release(), 0, state.SessionId);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " TReadOperation::Execute() finished with error, aborting: " << record.DebugString());
            Self->DeleteReadIterator(it);
            return;
        }

        Y_ASSERT(Reader);
        Y_ASSERT(BlockBuilder);

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
            << " sends rowCount# " << Reader->GetRowsRead() << ", hasUnreadQueries# " << Reader->HasUnreadQueries()
            << ", firstUnprocessed# " << state.FirstUnprocessedQuery);

        Reader->FillResult(*Result);
        Self->SendImmediateReadResult(Sender, Result.release(), 0, state.SessionId);
    }

    void Complete(const TActorContext& ctx) override {
        SendResult(ctx);

        TReadIteratorId readId(Sender, Request->Record.GetReadId());
        auto it = Self->ReadIterators.find(readId);
        if (it == Self->ReadIterators.end()) {
            // the one who removed the iterator should have reply to user
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " has been invalidated before TReadOperation::Complete()");
            return;
        }

        Y_VERIFY(it->second);

        // note that we save the state only when there're unread queries
        if (Reader->HasUnreadQueries()) {
            Y_ASSERT(it->second);
            auto& state = *it->second;
            Reader->UpdateState(state);
            if (!state.IsExhausted()) {
                ctx.Send(
                    Self->SelfId(),
                    new TEvDataShard::TEvReadContinue(Sender, Request->Record.GetReadId()));
            }
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " finished in read");
            Self->DeleteReadIterator(it);
        }
    }

private:
    void SendErrorAndAbort(
        const TActorContext& ctx,
        TReadIteratorState& state,
        Ydb::StatusIds::StatusCode code,
        const TString& msg)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " aborted read iterator# "
            << state.ReadId << " with msg " << msg);

        if (!Result)
            Result.reset(new TEvDataShard::TEvReadResult());

        SetStatusError(Result->Record, code, msg);
        Result->Record.SetReadId(state.ReadId);
        Self->SendImmediateReadResult(Sender, Result.release(), 0, state.SessionId);

        // note that we don't need to execute any other unit
        Abort(EExecutionUnitKind::CompletedOperations);

        TReadIteratorId readId(Sender, state.ReadId);
        Self->DeleteReadIterator(readId);
    }

    // return semantics is like in Execute()
    bool Read(TTransactionContext& txc, const TActorContext& ctx, TReadIteratorState& state) {
        const auto& tableId = state.PathId.LocalPathId;
        if (state.PathId.OwnerId == Self->GetPathOwnerId()) {
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << state.PathId.LocalPathId);
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
                        << ", requested table schemaversion " << state.SchemaVersion);
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
                        << state.PathId.LocalPathId);
                return true;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        ui64 ownerId = state.PathId.OwnerId;
        TSnapshotKey snapshotKey(
            ownerId,
            tableId,
            state.ReadVersion.Step,
            state.ReadVersion.TxId);

        bool isMvccVersion = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
        bool allowMvcc = isMvccVersion && !Self->IsFollower();
        if (!Self->GetSnapshotManager().FindAvailable(snapshotKey) && !allowMvcc) {
            SetStatusError(
                Result->Record,
                Ydb::StatusIds::ABORTED,
                TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                     << state.ReadVersion << " shard " << Self->TabletID()
                     << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                     << (Self->IsFollower() ? " RO replica" : ""));

            return true;
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

        Reader.reset(new TReader(state, *BlockBuilder, TableInfo));
        return Reader->Read(txc, ctx);
    }

    void PrepareValidationInfo(const TActorContext&, const TReadIteratorState& state) {
        TTableId tableId(state.PathId.OwnerId, state.PathId.LocalPathId, state.SchemaVersion);

        TVector<NScheme::TTypeId> keyTypes;

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

        ValidationInfo.Loaded = true;
    }

    void AcquireLock(const TActorContext& ctx, TReadIteratorState& state) {
        auto& sysLocks = Self->SysLocksTable();
        auto& locker = sysLocks.GetLocker();

        const auto lockTxId = state.Request->Record.GetLockTxId();
        const auto lockNodeId = state.Request->Record.GetLockNodeId();
        TTableId tableId(state.PathId.OwnerId, state.PathId.LocalPathId, state.SchemaVersion);
        TLockInfo::TPtr lock;

        state.LockTxId = lockTxId;

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
                    TRangeKey rangeKey = locker.MakeRange(tableId, lockRange);
                    lock = locker.AddRangeLock(lockTxId, lockNodeId, rangeKey, state.ReadVersion);
                } else {
                    TPointKey pointKey = locker.MakePoint(tableId, key.GetCells());
                    lock = locker.AddPointLock(lockTxId, lockNodeId, pointKey, state.ReadVersion);
                }
            }
        } else {
            // since no keys, then we must have ranges (has been checked initially)
            for (size_t i = 0; i < state.Request->Ranges.size(); ++i) {
                auto range = state.Request->Ranges[i].ToTableRange();
                TRangeKey rangeKey = locker.MakeRange(tableId, range);
                lock = locker.AddRangeLock(lockTxId, lockNodeId, rangeKey, state.ReadVersion);
            }
        }

        ui64 counter;
        ui64 lockId;
        bool isBroken;
        if (lock) {
            counter = lock->GetCounter(state.ReadVersion);
            lockId = lock->GetLockId();
            isBroken = lock->IsBroken(state.ReadVersion);
        } else {
            counter = TSysTables::TLocksTable::TLock::ErrorNotSet;
            lockId = lockTxId;
            isBroken = true;
        }

        if (!isBroken && Reader->HasInvisibleRowSkips()) {
            locker.BreakLock(lockTxId, TRowVersion::Min());
            isBroken = true;
            counter = TSysTables::TLocksTable::TLock::ErrorAlreadyBroken;
        }

        sysLocks.UpdateCounters(counter);

        NKikimrTxDataShard::TLock *addLock;
        if (!isBroken) {
            addLock = Result->Record.AddTxLocks();
        } else {
            addLock = Result->Record.AddBrokenTxLocks();
        }

        addLock->SetLockId(lockId);
        addLock->SetDataShard(Self->TabletID());
        addLock->SetGeneration(Self->Generation());
        addLock->SetCounter(counter);
        addLock->SetSchemeShard(state.PathId.OwnerId);
        addLock->SetPathId(state.PathId.LocalPathId);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
            << " Acquired lock# " << lockId << ", counter# " << counter
            << " for " << state.PathId);

        state.Lock = lock; // note that might be nullptr
    }
};

class TDataShard::TTxReadViaPipeline : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    TEvDataShard::TEvRead::TPtr Ev;
    TReadIteratorId ReadId;

    TOperation::TPtr Op;
    TVector<EExecutionUnitKind> CompleteList;

public:
    TTxReadViaPipeline(TDataShard* ds, TEvDataShard::TEvRead::TPtr ev)
        : TBase(ds)
        , Ev(std::move(ev))
        , ReadId(Ev->Sender, Ev->Get()->Record.GetReadId())
    {}

    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxReadViaPipeline execute"
            << ": at tablet# " << Self->TabletID());

        auto it = Self->ReadIterators.find(ReadId);
        if (it == Self->ReadIterators.end()) {
            // iterator aborted
            return true;
        }

        auto& state = *it->second;

        // If tablet is in follower mode then we should sync scheme
        // before we build and check operation.
        if (Self->IsFollower()) {
            NKikimrTxDataShard::TError::EKind status = NKikimrTxDataShard::TError::OK;
            TString errMessage;

            if (!Self->SyncSchemeOnFollower(txc, ctx, status, errMessage)) {
                return false;
            }

            if (status != NKikimrTxDataShard::TError::OK) {
                std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
                SetStatusError(
                    result->Record,
                    Ydb::StatusIds::INTERNAL_ERROR,
                    TStringBuilder() << "Failed to sync follower: " << errMessage);
                result->Record.SetReadId(ReadId.ReadId);
                SendViaSession(state.SessionId, ReadId.Sender, Self->SelfId(), result.release());

                return true;
            }
        }

        if (Ev) {
            const ui64 tieBreaker = Self->NextTieBreakerIndex++;
            Op = new TReadOperation(Self, tieBreaker, ctx.Now(), tieBreaker, Ev);
            Op->BuildExecutionPlan(false);
            Self->Pipeline.GetExecutionUnit(Op->GetCurrentUnit()).AddOperation(Op);

            Ev = nullptr;
        }

        auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);
        if (!CompleteList.empty()) {
            return true;
        } else if (status == EExecutionStatus::Restart) {
            return false;
        } else {
            Op = nullptr;
            return true;
        }
    }

    void Complete(const TActorContext& ctx) override {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxReadViaPipeline(" << GetTxType() << ") Complete"
            << ": at tablet# " << Self->TabletID());

        Self->Pipeline.RunCompleteList(Op, CompleteList, ctx);
        if (Self->Pipeline.CanRunAnotherOp()) {
            Self->PlanQueue.Progress(ctx);
        }
    }
};

class TDataShard::TTxReadContinue : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    TEvDataShard::TEvReadContinue::TPtr Ev;

    std::unique_ptr<TEvDataShard::TEvReadResult> Result;
    std::unique_ptr<IBlockBuilder> BlockBuilder;
    TShortTableInfo TableInfo;
    std::unique_ptr<TReader> Reader;

public:
    TTxReadContinue(TDataShard* ds, TEvDataShard::TEvReadContinue::TPtr ev)
        : TBase(ds)
        , Ev(ev)
    {}

    // note that intentionally the same as TEvRead
    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        // note that we don't need to check shard state here:
        // 1. Since TTxReadContinue scheduled, shard was ready.
        // 2. If shards changes the state, it must cancel iterators and we will
        // not find our readId ReadIterators.
        TReadIteratorId readId(Ev->Get()->Reader, Ev->Get()->ReadId);
        auto it = Self->ReadIterators.find(readId);
        if (it == Self->ReadIterators.end()) {
            // read has been aborted
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ReadContinue for reader# "
                << Ev->Get()->Reader << ", readId# " << Ev->Get()->ReadId << " didn't found state");
            return true;
        }

        Y_ASSERT(it->second);
        auto& state = *it->second;

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ReadContinue for reader# "
            << Ev->Get()->Reader << ", readId# " << Ev->Get()->ReadId
            << ", firstUnprocessedQuery# " << state.FirstUnprocessedQuery);

        Result.reset(new TEvDataShard::TEvReadResult());

        const auto& tableId = state.PathId.LocalPathId;
        if (state.PathId.OwnerId == Self->GetPathOwnerId()) {
            auto it = Self->TableInfos.find(tableId);
            if (it == Self->TableInfos.end()) {
                SetStatusError(
                    Result->Record,
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Unknown table id: " << state.PathId.LocalPathId);
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
                        << ", requested table schemaversion " << state.SchemaVersion);
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
                        << state.PathId.LocalPathId);
                SendResult(ctx);
                return true;
            }
            TableInfo = TShortTableInfo(state.PathId.LocalPathId, *schema);
        }

        ui64 ownerId = state.PathId.OwnerId;
        TSnapshotKey snapshotKey(
            ownerId,
            tableId,
            state.ReadVersion.Step,
            state.ReadVersion.TxId);

        bool isMvccVersion = state.ReadVersion >= Self->GetSnapshotManager().GetLowWatermark();
        bool allowMvcc = isMvccVersion && !Self->IsFollower();
        if (!Self->GetSnapshotManager().FindAvailable(snapshotKey) && !allowMvcc) {
            SetStatusError(
                Result->Record,
                Ydb::StatusIds::ABORTED,
                TStringBuilder() << "Table id " << tableId << " lost snapshot at "
                     << state.ReadVersion << " shard " << Self->TabletID()
                     << " with lowWatermark " << Self->GetSnapshotManager().GetLowWatermark()
                     << (Self->IsFollower() ? " RO replica" : ""));
            SendResult(ctx);
            return true;
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

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
            << " ReadContinue: reader# " << Ev->Get()->Reader << ", readId# " << Ev->Get()->ReadId
            << ", FirstUnprocessedQuery# " << state.FirstUnprocessedQuery);

        Reader.reset(new TReader(state, *BlockBuilder, TableInfo));
        if (Reader->Read(txc, ctx)) {
            SendResult(ctx);
            return true;
        }
        return false;
    }

    void Complete(const TActorContext&) override {
        // nothing to do
    }

    void SendResult(const TActorContext& ctx) {
        const auto* request = Ev->Get();
        TReadIteratorId readId(request->Reader, request->ReadId);
        auto it = Self->ReadIterators.find(readId);
        Y_VERIFY(it != Self->ReadIterators.end());
        Y_VERIFY(it->second);
        auto& state = *it->second;

        if (!Result) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " TTxReadContinue::Execute() finished without Result, aborting");
            Self->DeleteReadIterator(it);

            Result.reset(new TEvDataShard::TEvReadResult());
            SetStatusError(Result->Record, Ydb::StatusIds::ABORTED, "Iterator aborted");
            Result->Record.SetReadId(readId.ReadId);
            Self->SendImmediateReadResult(request->Reader, Result.release(), 0, state.SessionId);
            return;
        }

        // error happened and status set
        auto& record = Result->Record;
        if (record.HasStatus()) {
            record.SetSeqNo(state.SeqNo + 1);
            record.SetReadId(readId.ReadId);
            Self->SendImmediateReadResult(request->Reader, Result.release(), 0, state.SessionId);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " TTxReadContinue::Execute() finished with error, aborting: " << record.DebugString());
            Self->DeleteReadIterator(it);
            return;
        }

        if (state.Lock && !state.ReportedLockBroken) {
            bool isBroken = false;
            ui64 counter;
            ui64 lockId;
            if (state.Lock->IsBroken(state.ReadVersion)) {
                isBroken = true;
                counter = state.Lock->GetCounter(state.ReadVersion);
                lockId = state.Lock->GetLockId();

            } else if (Reader->HasInvisibleRowSkips()) {
                isBroken = true;
                counter = TSysTables::TLocksTable::TLock::ErrorBroken;
                lockId = state.LockTxId;

                auto& sysLocks = Self->SysLocksTable();
                auto& locker = sysLocks.GetLocker();
                locker.BreakLock(state.LockTxId, TRowVersion::Min());
                sysLocks.UpdateCounters(counter);
            }

            if (isBroken) {
                state.ReportedLockBroken = true;
                NKikimrTxDataShard::TLock *addLock = record.AddBrokenTxLocks();
                addLock->SetLockId(lockId);
                addLock->SetDataShard(Self->TabletID());
                addLock->SetGeneration(Self->Generation());
                addLock->SetCounter(counter);
                addLock->SetSchemeShard(state.PathId.OwnerId);
                addLock->SetPathId(state.PathId.LocalPathId);

                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                    << " TTxReadContinue::Execute() found broken lock# " << lockId);
            }
        }

        Y_ASSERT(Reader);
        Y_ASSERT(BlockBuilder);

        Reader->FillResult(*Result);
        Self->SendImmediateReadResult(request->Reader, Result.release(), 0, state.SessionId);

        if (Reader->HasUnreadQueries()) {
            Y_ASSERT(it->second);
            auto& state = *it->second;
            Reader->UpdateState(state);
            if (!state.IsExhausted()) {
                ctx.Send(
                    Self->SelfId(),
                    new TEvDataShard::TEvReadContinue(request->Reader, request->ReadId));
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                    << " Read quota exhausted for " << request->Reader << "," << request->ReadId);
            }
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " read iterator# " << readId
                << " finished in ReadContinue");
            Self->DeleteReadIterator(it);
        }
    }
};

void TDataShard::Handle(TEvDataShard::TEvRead::TPtr& ev, const TActorContext& ctx) {
    if (MediatorStateWaiting) {
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    // note that ins some cases we mutate this request below
    const auto* request = ev->Get();
    const auto& record = request->Record;
    if (Y_UNLIKELY(!record.HasReadId())) {
        std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_REQUEST, "Missing ReadId");
        ctx.Send(ev->Sender, result.release());
        return;
    }

    TReadIteratorId readId(ev->Sender, record.GetReadId());

    auto replyWithError = [&] (auto code, const auto& msg) {
        std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
        SetStatusError(
            result->Record,
            code,
            msg);
        result->Record.SetReadId(readId.ReadId);
        ctx.Send(ev->Sender, result.release());
    };

    if (Pipeline.HasDrop()) {
        replyWithError(
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "Request " << readId.ReadId << " rejected, because pipeline is in process of drop");
        return;
    }

    size_t totalInFly =
        ReadIteratorsInFly() + TxInFly() + ImmediateInFly()
            + MediatorStateWaitingMsgs.size() + ProposeQueue.Size() + TxWaiting();
    if (totalInFly > GetMaxTxInFly()) {
        replyWithError(
            Ydb::StatusIds::OVERLOADED,
            TStringBuilder() << "Request " << readId.ReadId << " rejected, MaxTxInFly was exceeded");
        return;
    }

    if (Y_UNLIKELY(ReadIterators.contains(readId))) {
        replyWithError(
            Ydb::StatusIds::ALREADY_EXISTS,
            TStringBuilder() << "Request " << readId.ReadId << " already executing");
        return;
    }

    if (!request->Keys.empty() && !request->Ranges.empty()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, "Both keys and ranges are forbidden");
        return;
    }

    if (request->Keys.empty() && request->Ranges.empty()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, "Neither keys nor ranges");
        return;
    }

    if (record.HasProgram()) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, "PushDown is not supported");
        return;
    }

    if (record.ColumnsSize() == 0) {
        replyWithError(Ydb::StatusIds::BAD_REQUEST, "Missing Columns");
        return;
    }

    TRowVersion readVersion = TRowVersion::Max();
    if (record.HasSnapshot()) {
        readVersion.Step = record.GetSnapshot().GetStep();
        readVersion.TxId = record.GetSnapshot().GetTxId();
    } else if (record.GetTableId().GetOwnerId() != TabletID() && !IsFollower()) {
        // sys table reads must be from HEAD,
        // user tables are allowed to be read from HEAD.
        //
        // TODO: currently we transform HEAD read to MVCC.
        // Instead we should try to read from HEAD and if full read
        // done in single execution - succeed, if not - drop operation
        // and transform HEAD to MVCC
        readVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly, nullptr);
        ev->Get()->Record.MutableSnapshot()->SetStep(readVersion.Step);
        ev->Get()->Record.MutableSnapshot()->SetTxId(Max<ui64>());

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " changed head to: " << readVersion.Step);
    }

    if (!IsFollower()) {
        if (record.GetTableId().GetOwnerId() != TabletID()) {
            // owner is schemeshard, read user table
            if (readVersion.IsMax()) {
                // currently not supported
                replyWithError(
                    Ydb::StatusIds::UNSUPPORTED,
                    "HEAD version is unsupported");
                return;
            }

            TSnapshotKey snapshotKey(
                record.GetTableId().GetOwnerId(),
                record.GetTableId().GetTableId(),
                readVersion.Step,
                readVersion.TxId);

            bool snapshotFound = GetSnapshotManager().FindAvailable(snapshotKey);
            if (!snapshotFound) {
                // check if there is MVCC version and maybe wait
                if (readVersion < GetSnapshotManager().GetLowWatermark()) {
                    replyWithError(
                        Ydb::StatusIds::NOT_FOUND,
                        TStringBuilder() << "MVCC read " << readVersion
                            << " bellow low watermark " << GetSnapshotManager().GetLowWatermark());
                    return;
                }

                // MVCC read is possible, but nead to check MVCC state and if we need to wait

                if (MvccSwitchState == TSwitchState::SWITCHING) {
                    Pipeline.AddWaitingReadIterator(readVersion, std::move(ev), ctx);
                    return;
                }

                auto prioritizedMvccSnapshotReads = GetEnablePrioritizedMvccSnapshotReads();
                TRowVersion unreadableEdge = Pipeline.GetUnreadableEdge(prioritizedMvccSnapshotReads);
                if (readVersion >= unreadableEdge) {
                    Pipeline.AddWaitingReadIterator(readVersion, std::move(ev), ctx);
                    return;
                }

                // we found proper MVCC snapshot
                snapshotFound = true;
            }

            if (!snapshotFound) {
                replyWithError(
                    Ydb::StatusIds::NOT_FOUND,
                    TStringBuilder() << "Neither regular nor MVCC snapshot for " << readVersion);
                return;
            }
        } else {
            // DS is owner, read system table
            if (!readVersion.IsMax()) {
                replyWithError(
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Only HEAD read from sys tables is allowed");
                return;
            }

            if (record.GetTableId().GetTableId() >= TDataShard::Schema::MinLocalTid) {
                replyWithError(
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Only sys tables can be read by localTid, table "
                        << record.GetTableId().GetTableId());
                return;
            }

            if (record.GetResultFormat() != NKikimrTxDataShard::CELLVEC) {
                replyWithError(
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Sys tables can be read only in cellvec format, but requested "
                        << (int)NKikimrTxDataShard::CELLVEC);
                return;
            }

            if (record.GetTableId().HasSchemaVersion()) {
                replyWithError(
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Requesting system stable owned " << record.GetTableId().GetOwnerId()
                        << ", localTid: " << record.GetTableId().GetTableId()
                        << ", with schema: " << record.GetTableId().GetSchemaVersion());
                return;
            }
        }
    } else {
        // follower: we can't check snapshot version, because need to sync and to sync
        // we need transaction
        if (readVersion.IsMax()) {
            replyWithError(
                Ydb::StatusIds::UNSUPPORTED,
                "HEAD version on followers is unsupported");
            return;
        }

        if (record.GetTableId().GetOwnerId() == TabletID()) {
            replyWithError(
                Ydb::StatusIds::UNSUPPORTED,
                "Systable reads on followers are not supported");
            return;
        }
    }

    // Note: iterator is correct and ready to execute

    TActorId sessionId;
    if (readId.Sender.NodeId() != SelfId().NodeId()) {
        Y_VERIFY_DEBUG(ev->InterconnectSession);
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

    ReadIterators.emplace(readId, new TReadIteratorState(sessionId));
    Executor()->Execute(new TTxReadViaPipeline(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvReadContinue::TPtr& ev, const TActorContext& ctx) {
    TReadIteratorId readId(ev->Get()->Reader, ev->Get()->ReadId);
    if (Y_UNLIKELY(!ReadIterators.contains(readId))) {
        return;
    }

    Executor()->Execute(new TTxReadContinue(this, ev), ctx);
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

        std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_REQUEST, "Missing mandatory fields in TEvReadAck");
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

    // We received ACK on message we hadn't sent yet
    if (state.SeqNo < record.GetSeqNo()) {
        auto issueStr = TStringBuilder() << TabletID() << " ReadAck from future: " << record.GetSeqNo()
            << ", current seqNo# " << state.SeqNo;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, issueStr);

        std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
        SetStatusError(result->Record, Ydb::StatusIds::BAD_SESSION, issueStr);
        result->Record.SetReadId(readId.ReadId);
        SendViaSession(state.SessionId, readId.Sender, SelfId(), result.release());

        DeleteReadIterator(it);
        return;
    }

    if (state.LastAckSeqNo && state.LastAckSeqNo >= record.GetSeqNo()) {
        // out of order, ignore
        return;
    }

    state.LastAckSeqNo = record.GetSeqNo();

    bool wasExhausted = state.IsExhausted();
    state.UpQuota(
        record.GetSeqNo(),
        record.GetMaxRows(),
        record.GetMaxBytes());

    if (wasExhausted && !state.IsExhausted()) {
        ctx.Send(
            SelfId(),
            new TEvDataShard::TEvReadContinue(ev->Sender, record.GetReadId()));
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " ReadAck: " << record
        << ", " << (wasExhausted ? "read continued" : "quota updated")
        << ", bytesLeft# " << state.Quota.Bytes << ", rowsLeft# " << state.Quota.Rows);
}

void TDataShard::Handle(TEvDataShard::TEvReadCancel::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    if (!record.HasReadId())
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " ReadCancel: " << record);

    TReadIteratorId readId(ev->Sender, record.GetReadId());
    DeleteReadIterator(readId);
}

void TDataShard::CancelReadIterators(Ydb::StatusIds::StatusCode code, const TString& issue, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " CancelReadIterators #" << ReadIterators.size());

    for (const auto& iterator: ReadIterators) {
        const auto& readIteratorId = iterator.first;
        const auto& state = iterator.second;

        std::unique_ptr<TEvDataShard::TEvReadResult> result(new TEvDataShard::TEvReadResult());
        SetStatusError(result->Record, code, issue);
        result->Record.SetReadId(iterator.first.ReadId);
        result->Record.SetSeqNo(state->SeqNo + 1);

        SendViaSession(state->SessionId, readIteratorId.Sender, SelfId(), result.release());
    }

    ReadIterators.clear();
    ReadIteratorSessions.clear();
}

void TDataShard::DeleteReadIterator(const TReadIteratorId& readId) {
    auto it = ReadIterators.find(readId);
    if (it != ReadIterators.end())
        DeleteReadIterator(it);
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
    ReadIterators.erase(it);
}

void TDataShard::ReadIteratorsOnNodeDisconnected(const TActorId& sessionId, const TActorContext &ctx) {
    auto itSession = ReadIteratorSessions.find(sessionId);
    if (itSession == ReadIteratorSessions.end())
        return;

    const auto& session = itSession->second;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
        << " closed session# " << sessionId << ", iterators# " << session.Iterators.size());

    for (const auto& readId: session.Iterators) {
        // we don't send anything to client, because it's up
        // to client to detect disconnect
        ReadIterators.erase(readId);
    }

    ReadIteratorSessions.erase(itSession);
}

} // NKikimr::NDataShard

template<>
inline void Out<NKikimr::NDataShard::TReadIteratorId>(
    IOutputStream& o,
    const NKikimr::NDataShard::TReadIteratorId& info)
{
    o << info.ToString();
}
