#include "kqp_stream_lookup_worker.h"
#include "kqp_stream_lookup_join_helpers.h"

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>

namespace NKikimr {
namespace NKqp {

constexpr ui64 MAX_IN_FLIGHT_LIMIT = 500;

namespace {
std::vector<std::pair<ui64, TOwnedTableRange>> GetRangePartitioning(const TKqpStreamLookupWorker::TPartitionInfo& partitionInfo,
    const std::vector<NScheme::TTypeInfo>& keyColumnTypes, const TOwnedTableRange& range) {

    YQL_ENSURE(partitionInfo);

    // Binary search of the index to start with.
    size_t idxStart = 0;
    size_t idxFinish = partitionInfo->size();
    while ((idxFinish - idxStart) > 1) {
        size_t idxCur = (idxFinish + idxStart) / 2;
        const auto& partCur = (*partitionInfo)[idxCur].Range->EndKeyPrefix.GetCells();
        YQL_ENSURE(partCur.size() <= keyColumnTypes.size());
        int cmp = CompareTypedCellVectors(partCur.data(), range.From.data(), keyColumnTypes.data(),
                                          std::min(partCur.size(), range.From.size()));
        if (cmp < 0) {
            idxStart = idxCur;
        } else {
            idxFinish = idxCur;
        }
    }

    std::vector<TCell> minusInf(keyColumnTypes.size());

    std::vector<std::pair<ui64, TOwnedTableRange>> rangePartition;
    for (size_t idx = idxStart; idx < partitionInfo->size(); ++idx) {
        TTableRange partitionRange{
            idx == 0 ? minusInf : (*partitionInfo)[idx - 1].Range->EndKeyPrefix.GetCells(),
            idx == 0 ? true : !(*partitionInfo)[idx - 1].Range->IsInclusive,
            (*partitionInfo)[idx].Range->EndKeyPrefix.GetCells(),
            (*partitionInfo)[idx].Range->IsInclusive
        };

        if (range.Point) {
            int intersection = ComparePointAndRange(
                range.From,
                partitionRange,
                keyColumnTypes,
                keyColumnTypes);

            if (intersection == 0) {
                rangePartition.emplace_back((*partitionInfo)[idx].ShardId, range);
            } else if (intersection < 0) {
                break;
            }
        } else {
            int intersection = CompareRanges(range, partitionRange, keyColumnTypes);

            if (intersection == 0) {
                auto rangeIntersection = Intersect(keyColumnTypes, range, partitionRange);
                rangePartition.emplace_back((*partitionInfo)[idx].ShardId, rangeIntersection);
            } else if (intersection < 0) {
                break;
            }
        }
    }

    return rangePartition;
}

struct TReadState {
    std::vector<TOwnedTableRange> PendingKeys;

    TMaybe<TOwnedCellVec> LastProcessedKey;
    ui32 FirstUnprocessedQuery = 0;
    ui64 LastSeqNo = 0;
};

void UpdateContinuationData(const NKikimrTxDataShard::TEvReadResult& record, TReadState& state) {
    YQL_ENSURE(record.HasContinuationToken(), "Successful TEvReadResult should contain continuation token");
    NKikimrTxDataShard::TReadContinuationToken continuationToken;
    bool parseResult = continuationToken.ParseFromString(record.GetContinuationToken());
    YQL_ENSURE(parseResult, "Failed to parse continuation token");
    state.FirstUnprocessedQuery = continuationToken.GetFirstUnprocessedQuery();

    if (continuationToken.HasLastProcessedKey()) {
        TSerializedCellVec lastKey(continuationToken.GetLastProcessedKey());
        state.LastProcessedKey = TOwnedCellVec(lastKey.GetCells());
    } else {
        state.LastProcessedKey.Clear();
    }
}

}  // !namespace

TKqpStreamLookupWorker::TKqpStreamLookupWorker(TLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory)
    : TypeEnv(typeEnv)
    , HolderFactory(holderFactory)
    , Settings(std::move(settings)) {
    KeyColumnTypes.resize(Settings.KeyColumns.size());
    for (const auto& [_, columnInfo] : Settings.KeyColumns) {
        YQL_ENSURE(columnInfo.KeyOrder < static_cast<i64>(KeyColumnTypes.size()));
        KeyColumnTypes[columnInfo.KeyOrder] = columnInfo.PType;
    }
}

TKqpStreamLookupWorker::~TKqpStreamLookupWorker() {
}

std::string TKqpStreamLookupWorker::GetTablePath() const {
    return Settings.TablePath;
}

TTableId TKqpStreamLookupWorker::GetTableId() const {
    return Settings.TableId;
}

class TKqpLookupRows : public TKqpStreamLookupWorker {
public:
    TKqpLookupRows(TLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory)
        : TKqpStreamLookupWorker(std::move(settings), typeEnv, holderFactory)
    {
    }

    virtual ~TKqpLookupRows() {}

    void AddInputRow(NUdf::TUnboxedValue inputRow) final {
        NMiniKQL::TStringProviderBackend backend;
        std::vector<TCell> keyCells(Settings.LookupKeyColumns.size());
        for (size_t colId = 0; colId < Settings.LookupKeyColumns.size(); ++colId) {
            const auto* lookupKeyColumn = Settings.LookupKeyColumns[colId];
            YQL_ENSURE(lookupKeyColumn->KeyOrder < static_cast<i64>(keyCells.size()));
            // when making a cell we don't really need to make a copy of data, because
            // TOwnedCellVec will make its' own copy.
            keyCells[lookupKeyColumn->KeyOrder] = MakeCell(lookupKeyColumn->PType,
                inputRow.GetElement(colId), backend, /* copy */ false);
        }

        AddInputRowImpl(std::move(keyCells));
    }

    void AddInputRow(TConstArrayRef<TCell> inputRow) final {
        NMiniKQL::TStringProviderBackend backend;
        std::vector<TCell> keyCells(Settings.LookupKeyColumns.size());
        for (size_t colId = 0; colId < Settings.LookupKeyColumns.size(); ++colId) {
            const auto* lookupKeyColumn = Settings.LookupKeyColumns[colId];
            YQL_ENSURE(lookupKeyColumn->KeyOrder < static_cast<i64>(keyCells.size()));
            keyCells[lookupKeyColumn->KeyOrder] = inputRow[colId];
        }

        AddInputRowImpl(std::move(keyCells));
    }

    virtual void AddInputRowImpl(std::vector<TCell> keyCells) {
        if (keyCells.size() < Settings.KeyColumns.size()) {
            // build prefix range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf])
            std::vector<TCell> fromCells(Settings.KeyColumns.size());
            for (size_t i = 0; i < keyCells.size(); ++i) {
                fromCells[i] = keyCells[i];
            }

            bool fromInclusive = true;
            bool toInclusive = false;

            UnprocessedKeys.emplace_back(fromCells, fromInclusive, keyCells, toInclusive);
        } else {
            // full pk, build point
            UnprocessedKeys.emplace_back(std::move(keyCells));
        }
    }

    std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui64& newReadId) final {
        auto it = ReadStateByReadId.find(prevReadId);
        if (it == ReadStateByReadId.end()) {
            return {};
        }

        std::vector<TOwnedTableRange> unprocessedRanges;
        std::vector<TOwnedTableRange> unprocessedPoints;

        TReadState state = std::move(it->second);
        auto& ranges = state.PendingKeys;

        ReadStateByReadId.erase(it);

        ui32 firstUnprocessedQuery = state.FirstUnprocessedQuery;
        const auto& lastProcessedKey = state.LastProcessedKey;

        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < ranges.size());
            auto unprocessedRange = ranges[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            unprocessedRanges.emplace_back(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);
            ++firstUnprocessedQuery;
        }

        for (ui32 i = firstUnprocessedQuery; i < ranges.size(); ++i) {
            if (ranges[i].Point) {
                unprocessedPoints.emplace_back(std::move(ranges[i]));
            } else {
                unprocessedRanges.emplace_back(std::move(ranges[i]));
            }
        }

        std::vector<THolder<TEvDataShard::TEvRead>> requests;
        requests.reserve(unprocessedPoints.size() + unprocessedRanges.size());

        if (!unprocessedPoints.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedPoints);
            requests.emplace_back(std::move(request));
            YQL_ENSURE(ReadStateByReadId.emplace(
                newReadId,
                TReadState{
                    .PendingKeys = std::move(unprocessedPoints),
                }).second);
        }

        if (!unprocessedRanges.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedRanges);
            requests.emplace_back(std::move(request));
            YQL_ENSURE(ReadStateByReadId.emplace(
                newReadId,
                TReadState{
                    .PendingKeys = std::move(unprocessedRanges),
                }).second);
        }

        return requests;
    }

    TReadList BuildRequests(const TPartitionInfo& partitioning, ui64& readId) final {
        YQL_ENSURE(partitioning);

        std::unordered_map<ui64, std::vector<TOwnedTableRange>> rangesPerShard;
        std::unordered_map<ui64, std::vector<TOwnedTableRange>> pointsPerShard;

        while (!UnprocessedKeys.empty()) {
            auto range = std::move(UnprocessedKeys.front());
            UnprocessedKeys.pop_front();

            auto partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(), range);
            for (auto [shardId, range] : partitions) {
                if (range.Point) {
                    pointsPerShard[shardId].push_back(std::move(range));
                } else {
                    rangesPerShard[shardId].push_back(std::move(range));
                }
            }
        }

        std::vector<std::pair<ui64, THolder<TEvDataShard::TEvRead>>> readRequests;
        readRequests.reserve(rangesPerShard.size() + pointsPerShard.size());

        for (auto& [shardId, points] : pointsPerShard) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++readId, request, points);
            readRequests.emplace_back(shardId, std::move(request));
            YQL_ENSURE(ReadStateByReadId.emplace(
                readId,
                TReadState{
                    .PendingKeys = std::move(points),
                }).second);
        }

        for (auto& [shardId, ranges] : rangesPerShard) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++readId, request, ranges);
            readRequests.emplace_back(shardId, std::move(request));
            YQL_ENSURE(ReadStateByReadId.emplace(
                readId,
                TReadState{
                    .PendingKeys = std::move(ranges),
                }).second);
        }

        return readRequests;
    }

    void AddResult(TShardReadResult result) final {
        const auto& record = result.ReadResult->Get()->Record;
        YQL_ENSURE(record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);

        auto it = ReadStateByReadId.find(record.GetReadId());
        YQL_ENSURE(it != ReadStateByReadId.end());

        if (!record.GetFinished()) {
            UpdateContinuationData(record, it->second);
        }

        ReadResults.emplace_back(std::move(result));
    }

    bool IsOverloaded() final {
        return false;
    }

    TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) final {
        TReadResultStats resultStats;
        batch.clear();

        while (!ReadResults.empty() && !resultStats.SizeLimitExceeded) {
            auto& result = ReadResults.front();
            for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
                const auto& resultRow = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
                YQL_ENSURE(resultRow.size() <= Settings.Columns.size(), "Result columns mismatch");

                NUdf::TUnboxedValue* rowItems = nullptr;
                auto row = HolderFactory.CreateDirectArrayHolder(Settings.Columns.size(), rowItems);

                i64 rowSize = 0;
                i64 storageRowSize = 0;
                for (size_t colIndex = 0, resultColIndex = 0; colIndex < Settings.Columns.size(); ++colIndex) {
                    const auto& column = Settings.Columns[colIndex];
                    if (IsSystemColumn(column.Name)) {
                        NMiniKQL::FillSystemColumn(rowItems[colIndex], result.ShardId, column.Id, column.PType);
                        rowSize += sizeof(NUdf::TUnboxedValue);
                    } else {
                        YQL_ENSURE(resultColIndex < resultRow.size());
                        storageRowSize += resultRow[resultColIndex].Size();
                        rowItems[colIndex] = NMiniKQL::GetCellValue(resultRow[resultColIndex], column.PType);
                        rowSize += NMiniKQL::GetUnboxedValueSize(rowItems[colIndex], column.PType).AllocatedBytes;
                        ++resultColIndex;
                    }
                }

                if (rowSize + (i64)resultStats.ResultBytesCount > freeSpace) {
                    resultStats.SizeLimitExceeded = true;
                }

                if (resultStats.ResultRowsCount && resultStats.SizeLimitExceeded) {
                    row.DeleteUnreferenced();
                    break;
                }

                batch.push_back(std::move(row));
                storageRowSize = std::max(storageRowSize, (i64)8);

                resultStats.ReadRowsCount += 1;
                resultStats.ReadBytesCount += storageRowSize;
                resultStats.ResultRowsCount += 1;
                resultStats.ResultBytesCount += storageRowSize;
            }

            if (result.UnprocessedResultRow == result.ReadResult->Get()->GetRowsCount()) {
                if (result.ReadResult->Get()->Record.GetFinished()) {
                    // delete finished read
                    auto it = ReadStateByReadId.find(result.ReadResult->Get()->Record.GetReadId());
                    ReadStateByReadId.erase(it);
                }

                ReadResults.pop_front();
            }
        }

        return resultStats;
    }

    TReadResultStats ReadAllResult(std::function<void(TConstArrayRef<TCell>)> reader) final {
        TReadResultStats resultStats;

        while (!ReadResults.empty()) {
            auto& result = ReadResults.front();
            for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
                const auto& resultRow = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
                YQL_ENSURE(resultRow.size() <= Settings.Columns.size(), "Result columns mismatch");

                const i64 storageRowSize = EstimateSize(resultRow);

                reader(resultRow);

                resultStats.ReadRowsCount += 1;
                resultStats.ReadBytesCount += storageRowSize;
                resultStats.ResultRowsCount += 1;
                resultStats.ResultBytesCount += storageRowSize;
            }

            if (result.UnprocessedResultRow == result.ReadResult->Get()->GetRowsCount()) {
                if (result.ReadResult->Get()->Record.GetFinished()) {
                    // delete finished read
                    auto it = ReadStateByReadId.find(result.ReadResult->Get()->Record.GetReadId());
                    ReadStateByReadId.erase(it);
                }

                ReadResults.pop_front();
            }
        }

        return resultStats;
    }

    bool AllRowsProcessed() final {
        return UnprocessedKeys.empty()
            && ReadStateByReadId.empty()
            && ReadResults.empty();
    }

    void ResetRowsProcessing(ui64 readId) final {
        auto it = ReadStateByReadId.find(readId);
        if (it == ReadStateByReadId.end()) {
            return;
        }
        auto& keys = it->second.PendingKeys;

        ui32 firstUnprocessedQuery = it->second.FirstUnprocessedQuery;
        const auto& lastProcessedKey = it->second.LastProcessedKey;

        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < keys.size());
            auto unprocessedRange = keys[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            UnprocessedKeys.emplace_back(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);
            ++firstUnprocessedQuery;
        }

        for (ui32 keyIdx = firstUnprocessedQuery; keyIdx < keys.size(); ++keyIdx) {
            UnprocessedKeys.emplace_back(std::move(keys[keyIdx]));
        }

        ReadStateByReadId.erase(it);
    }

private:
    void FillReadRequest(ui64 readId, THolder<TEvDataShard::TEvRead>& request, const std::vector<TOwnedTableRange>& ranges) {
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(Settings.TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(Settings.TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(Settings.TableId.SchemaVersion);

        for (const auto& column : Settings.Columns) {
            if (!IsSystemColumn(column.Name)) {
                record.AddColumns(column.Id);
            }
        }

        YQL_ENSURE(!ranges.empty());
        if (ranges.front().Point) {
            request->Keys.reserve(ranges.size());
            for (auto& range : ranges) {
                YQL_ENSURE(range.Point);
                request->Keys.emplace_back(TSerializedCellVec(range.From));
            }
        } else {
            request->Ranges.reserve(ranges.size());
            for (auto& range : ranges) {
                YQL_ENSURE(!range.Point);

                if (range.To.size() < Settings.KeyColumns.size()) {
                    // absent cells mean infinity => in prefix notation `To` should be inclusive
                    request->Ranges.emplace_back(TSerializedTableRange(range.From, range.InclusiveFrom, range.To, true));
                } else {
                    request->Ranges.emplace_back(TSerializedTableRange(range));
                }
            }
        }
    }

private:
    std::deque<TOwnedTableRange> UnprocessedKeys;
    std::unordered_map<ui64, TReadState> ReadStateByReadId;
    std::deque<TShardReadResult> ReadResults;
};

class TKqpJoinRows : public TKqpStreamLookupWorker {
public:
    TKqpJoinRows(TLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const NYql::NDqProto::TTaskInput& inputDesc)
        : TKqpStreamLookupWorker(std::move(settings), typeEnv, holderFactory)
        , InputDesc(inputDesc) {

        // read columns should contain join key and result columns
        for (auto joinKey : Settings.LookupKeyColumns) {
            ReadColumns.emplace(joinKey->Name, *joinKey);
        }

        for (auto column : Settings.Columns) {
            ReadColumns.emplace(column.Name, column);
        }
    }

    struct TUnprocessedLeftRow {
        TOwnedCellVec JoinKey;
        NUdf::TUnboxedValue InputLeftRow;
        ui64 RowSeqNo;

        explicit TUnprocessedLeftRow(TOwnedCellVec joinKey, NUdf::TUnboxedValue inputLeftRow, ui64 rowSeqNo)
            : JoinKey(std::move(joinKey))
            , InputLeftRow(std::move(inputLeftRow))
            , RowSeqNo(rowSeqNo)
        {}
    };

    void AddInputRow(NUdf::TUnboxedValue inputRow) final {
        auto joinKey = inputRow.GetElement(0);
        std::vector<TCell> joinKeyCells(Settings.LookupKeyColumns.size());
        NMiniKQL::TStringProviderBackend backend;

        ui64 rowSeqNo;
        bool firstRow = true;
        bool lastRow = true;
        if (IsInputTriplet()) {
            auto value = inputRow.GetElement(2).Get<ui64>();
            auto cookie = TStreamLookupJoinRowCookie::Decode(value);
            rowSeqNo = cookie.RowSeqNo;
            firstRow = cookie.FirstRow;
            lastRow = cookie.LastRow;
        } else {
            rowSeqNo = InputRowSeqNo++;
        }

        if (joinKey.HasValue()) {
            for (size_t colId = 0; colId < Settings.LookupKeyColumns.size(); ++colId) {
                const auto* joinKeyColumn = Settings.LookupKeyColumns[colId];
                YQL_ENSURE(joinKeyColumn->KeyOrder < static_cast<i64>(joinKeyCells.size()));
                // when making a cell we don't really need to make a copy of data, because
                // TOwnedCellVec will make its' own copy.
                joinKeyCells[joinKeyColumn->KeyOrder] = MakeCell(joinKeyColumn->PType,
                    joinKey.GetElement(colId), backend,  /* copy */ false);
            }
        }

        ResultRowsBySeqNo[rowSeqNo].AcceptLeftRow(firstRow, lastRow);
        UnprocessedRows.emplace_back(
            TUnprocessedLeftRow(
                std::move(TOwnedCellVec(joinKeyCells)),
                std::move(inputRow.GetElement(1)),
                rowSeqNo
            )
        );
    }

    void AddInputRow(TConstArrayRef<TCell>) final {
        YQL_ENSURE(false);
    }

    bool IsOverloaded() final {
        return UnprocessedRows.size() >= MAX_IN_FLIGHT_LIMIT || PendingLeftRowsByKey.size() >= MAX_IN_FLIGHT_LIMIT || ResultRowsBySeqNo.size() >= MAX_IN_FLIGHT_LIMIT;
    }

    std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui64& newReadId) final {
        auto readIt = ReadStateByReadId.find(prevReadId);
        if (readIt == ReadStateByReadId.end()) {
            return {};
        }

        std::vector<TOwnedTableRange> unprocessedRanges;
        std::vector<TOwnedTableRange> unprocessedPoints;

        TReadState state = std::move(readIt->second);
        auto& ranges = state.PendingKeys;

        ReadStateByReadId.erase(readIt);

        ui32 firstUnprocessedQuery = state.FirstUnprocessedQuery;
        const auto& lastProcessedKey = state.LastProcessedKey;

        for (ui32 i = 0; i < firstUnprocessedQuery; ++i) {
            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(ranges[i]));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(prevReadId);
        }

        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < ranges.size());
            auto unprocessedRange = ranges[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(unprocessedRange));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(prevReadId);

            TOwnedTableRange range(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);

            unprocessedRanges.emplace_back(std::move(range));
            ++firstUnprocessedQuery;
        }

        for (ui32 i = firstUnprocessedQuery; i < ranges.size(); ++i) {
            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(ranges[i]));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(prevReadId);

            if (ranges[i].Point) {
                unprocessedPoints.emplace_back(std::move(ranges[i]));
            } else {
                unprocessedRanges.emplace_back(std::move(ranges[i]));
            }
        }

        std::vector<THolder<TEvDataShard::TEvRead>> readRequests;

        if (!unprocessedPoints.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedPoints);
            readRequests.emplace_back(std::move(request));

            for (const auto& point : unprocessedPoints) {
                auto rowIt = PendingLeftRowsByKey.find(point.From);
                YQL_ENSURE(rowIt != PendingLeftRowsByKey.end());
                rowIt->second.PendingReads.insert(newReadId);
            }

            YQL_ENSURE(ReadStateByReadId.emplace(
                newReadId,
                TReadState{
                    .PendingKeys = std::move(unprocessedPoints),
                }).second);
        }

        if (!unprocessedRanges.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedRanges);
            readRequests.emplace_back(std::move(request));

            for (const auto& range : unprocessedRanges) {
                auto rowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
                YQL_ENSURE(rowIt != PendingLeftRowsByKey.end());
                rowIt->second.PendingReads.insert(newReadId);
            }

            YQL_ENSURE(ReadStateByReadId.emplace(
                newReadId,
                TReadState{
                    .PendingKeys = std::move(unprocessedRanges),
                }).second);
        }

        return readRequests;
    }

    TReadList BuildRequests(const TPartitionInfo& partitioning, ui64& readId) final {
        YQL_ENSURE(partitioning);

        std::unordered_map<ui64, std::vector<TOwnedTableRange>> rangesPerShard;
        std::unordered_map<ui64, std::vector<TOwnedTableRange>> pointsPerShard;

        while (!UnprocessedKeys.empty()) {
            auto range = std::move(UnprocessedKeys.front());
            UnprocessedKeys.pop_front();

            auto partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(), range);
            for (auto [shardId, range] : partitions) {
                YQL_ENSURE(PendingLeftRowsByKey.contains(ExtractKeyPrefix(range)));

                if (range.Point) {
                    pointsPerShard[shardId].push_back(std::move(range));
                } else {
                    rangesPerShard[shardId].push_back(std::move(range));
                }
            }
        }

        while (!UnprocessedRows.empty()) {
            const auto& unprocessedRow = UnprocessedRows.front();

            if (PendingLeftRowsByKey.contains(unprocessedRow.JoinKey)) {
                // TODO: skip key duplicate
                break;
            }

            auto isKeyAllowed = [&](const TOwnedCellVec& cellVec) {
                auto allowNullKeysPrefixSize = Settings.AllowNullKeysPrefixSize;
                if (allowNullKeysPrefixSize >= cellVec.size()) {
                    // all lookup key components can contain null
                    return true;
                }

                // otherwise we can use nulls only for first allowNullKeysPrefixSize key components
                for (size_t i = 0; i < cellVec.size(); ++i) {
                    if (cellVec[i].IsNull() && i >= allowNullKeysPrefixSize) {
                        return false;
                    }
                }

                return true;
            };

            if (isKeyAllowed(unprocessedRow.JoinKey)) {
                std::vector <std::pair<ui64, TOwnedTableRange>> partitions;
                if (unprocessedRow.JoinKey.size() < Settings.KeyColumns.size()) {
                    // build prefix range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf])
                    std::vector <TCell> fromCells(Settings.KeyColumns.size() - unprocessedRow.JoinKey.size());
                    fromCells.insert(fromCells.begin(), unprocessedRow.JoinKey.begin(), unprocessedRow.JoinKey.end());
                    bool fromInclusive = true;
                    bool toInclusive = false;

                    partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(),
                        TOwnedTableRange(fromCells, fromInclusive, unprocessedRow.JoinKey, toInclusive)
                    );
                } else {
                    // full pk, build point
                    partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(), TOwnedTableRange(unprocessedRow.JoinKey));
                }

                for (auto[shardId, range] : partitions) {
                    if (range.Point) {
                        pointsPerShard[shardId].push_back(std::move(range));
                    } else {
                        rangesPerShard[shardId].push_back(std::move(range));
                    }
                }
            }

            PendingLeftRowsByKey.insert(
                std::make_pair(std::move(unprocessedRow.JoinKey), TLeftRowInfo(
                    std::move(unprocessedRow.InputLeftRow), unprocessedRow.RowSeqNo, GetLeftRowType())));
            UnprocessedRows.pop_front();
        }

        std::vector<std::pair<ui64, THolder<TEvDataShard::TEvRead>>> requests;
        requests.reserve(rangesPerShard.size() + pointsPerShard.size());

        for (auto& [shardId, points] : pointsPerShard) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++readId, request, points);
            requests.emplace_back(shardId, std::move(request));

            for (const auto& point : points) {
                auto rowIt = PendingLeftRowsByKey.find(point.From);
                YQL_ENSURE(rowIt != PendingLeftRowsByKey.end());
                rowIt->second.PendingReads.insert(readId);
            }

            YQL_ENSURE(ReadStateByReadId.emplace(
                readId,
                TReadState{
                    .PendingKeys = std::move(points),
                }).second);
        }

        for (auto& [shardId, ranges] : rangesPerShard) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++readId, request, ranges);
            requests.emplace_back(shardId, std::move(request));

            for (const auto& range : ranges) {
                auto rowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
                YQL_ENSURE(rowIt != PendingLeftRowsByKey.end());
                rowIt->second.PendingReads.insert(readId);
            }

            YQL_ENSURE(ReadStateByReadId.emplace(
                readId,
                TReadState{
                    .PendingKeys = std::move(ranges),
                }).second);
        }

        return requests;
    }

    void AddResult(TShardReadResult result) final {
        const auto& record = result.ReadResult->Get()->Record;
        YQL_ENSURE(record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);

        auto pendingKeysIt = ReadStateByReadId.find(record.GetReadId());
        YQL_ENSURE(pendingKeysIt != ReadStateByReadId.end());

        for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
            const auto& row = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
            // result can contain fewer columns because of system columns
            YQL_ENSURE(row.size() <= ReadColumns.size(), "Result columns mismatch");

            std::vector<TCell> joinKeyCells(Settings.LookupKeyColumns.size());
            for (size_t joinKeyColumn = 0; joinKeyColumn < Settings.LookupKeyColumns.size(); ++joinKeyColumn) {
                auto columnIt = ReadColumns.find(Settings.LookupKeyColumns[joinKeyColumn]->Name);
                YQL_ENSURE(columnIt != ReadColumns.end());
                joinKeyCells[Settings.LookupKeyColumns[joinKeyColumn]->KeyOrder] = row[std::distance(ReadColumns.begin(), columnIt)];
            }

            auto leftRowIt = PendingLeftRowsByKey.find(joinKeyCells);
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());

            TReadResultStats rowStats;
            auto& resultRows = ResultRowsBySeqNo[leftRowIt->second.SeqNo];
            auto resultRow = leftRowIt->second.TryBuildResultRow(
                HolderFactory, Settings, ReadColumns, row, rowStats, resultRows.FirstRowInBatch(), false, result.ShardId);
            if (resultRow.HasValue()) {
                resultRows.AddRow(std::move(rowStats), std::move(resultRow));
                YQL_ENSURE(IsRowSeqNoValid(leftRowIt->second.SeqNo));
            }
        }

        if (record.GetFinished()) {
            for (const auto& key : pendingKeysIt->second.PendingKeys) {
                auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(key));
                YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
                leftRowIt->second.PendingReads.erase(record.GetReadId());
            }

            ReadStateByReadId.erase(pendingKeysIt);
        } else {
            UpdateContinuationData(record, pendingKeysIt->second);
        }
    }

    bool AllRowsProcessed() final {
        return UnprocessedRows.empty()
            && UnprocessedKeys.empty()
            && ReadStateByReadId.empty()
            && ResultRowsBySeqNo.empty()
            && PendingLeftRowsByKey.empty();
    }

    void ResetRowsProcessing(ui64 readId) final {
        auto readIt = ReadStateByReadId.find(readId);
        if (readIt == ReadStateByReadId.end()) {
            return;
        }

        auto& ranges = readIt->second.PendingKeys;

        ui32 firstUnprocessedQuery = readIt->second.FirstUnprocessedQuery;
        const auto& lastProcessedKey = readIt->second.LastProcessedKey;
        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < ranges.size());
            auto unprocessedRange = ranges[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            TOwnedTableRange range(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);

            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(readId);

            UnprocessedKeys.emplace_back(std::move(range));
            ++firstUnprocessedQuery;
        }

        for (ui32 i = firstUnprocessedQuery; i < ranges.size(); ++i) {
            auto& range = ranges[i];
            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(readId);

            UnprocessedKeys.emplace_back(std::move(range));
        }

        ReadStateByReadId.erase(readIt);
    }

    TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) final {
        TReadResultStats resultStats;
        batch.clear();

        // we should process left rows that haven't received last row flags.
        for (auto leftRowIt = PendingLeftRowsByKey.begin(); leftRowIt != PendingLeftRowsByKey.end();) {
            if (leftRowIt->second.PendingReads.empty()) {
                TReadResultStats rowStats;
                auto& result = ResultRowsBySeqNo[leftRowIt->second.SeqNo];
                result.FinishLeftRow();
                auto resultRow = leftRowIt->second.TryBuildResultRow(HolderFactory, Settings, ReadColumns, {}, rowStats, result.FirstRowInBatch(), result.ProcessedAllLeftRows());
                YQL_ENSURE(IsRowSeqNoValid(leftRowIt->second.SeqNo));
                result.AddRow(std::move(rowStats), std::move(resultRow));
                PendingLeftRowsByKey.erase(leftRowIt++);
            } else {
                ++leftRowIt;
            }
        }

        auto getNextResult = [&]() {
            if (!ShoulKeepRowsOrder()) {
                return ResultRowsBySeqNo.begin();
            }

            return ResultRowsBySeqNo.find(CurrentResultSeqNo);
        };

        while (!resultStats.SizeLimitExceeded) {
            auto resultIt = getNextResult();
            if (resultIt == ResultRowsBySeqNo.end()) {
                break;
            }

            auto& result = resultIt->second;

            while(!result.Rows.empty()) {
                TResultBatch::TResultRow& row = result.Rows.front();

                if (resultStats.ResultRowsCount && resultStats.ResultBytesCount + row.Stats.ResultBytesCount > (ui64)freeSpace) {
                    resultStats.SizeLimitExceeded = true;
                    break;
                }

                batch.emplace_back(std::move(row.Data));
                result.Rows.pop_front();
                resultStats.Add(row.Stats);
            }

            if (result.Completed()) {
                ResultRowsBySeqNo.erase(resultIt);
                ++CurrentResultSeqNo;
            } else {
                break;
            }
        }

        return resultStats;
    }

    TReadResultStats ReadAllResult(std::function<void(TConstArrayRef<TCell>)>) final {
        YQL_ENSURE(false);
    }

    ~TKqpJoinRows() {
        UnprocessedRows.clear();
        PendingLeftRowsByKey.clear();
        ResultRowsBySeqNo.clear();
    }
private:
    struct TLeftRowInfo {
        explicit TLeftRowInfo(NUdf::TUnboxedValue row, ui64 seqNo, NMiniKQL::TStructType* leftRowType)
        : Row(std::move(row))
        , SeqNo(seqNo)
        , LeftRowType(leftRowType)
        , LeftRowSize(NYql::NDq::TDqDataSerializer::EstimateSize(Row, leftRowType))
        {
        }

        NUdf::TUnboxedValue Row;
        std::unordered_set<ui64> PendingReads;
        bool RightRowExist = false;
        bool LastRowReceived = false;
        const ui64 SeqNo;
        NMiniKQL::TStructType* LeftRowType = nullptr;
        ui64 LeftRowSize = 0;

        NUdf::TUnboxedValue RightRow;
        ui64 RightRowSize = 0;
        i64 StorageReadBytes = 0;

        NUdf::TUnboxedValue TryBuildResultRow(const NMiniKQL::THolderFactory& HolderFactory, const TLookupSettings& Settings,
            const std::map<std::string, TSysTables::TTableColumnInfo>& ReadColumns, TConstArrayRef<TCell> rightRow,
            TReadResultStats& rowStats, bool firstRow, bool lastRow, TMaybe<ui64> shardId = {})
        {
            if (lastRow) {
                LastRowReceived = lastRow;
            }

            NUdf::TUnboxedValue resultRow;

            if (RightRow.HasValue() || lastRow) {
                bool hasValue = RightRow.HasValue();
                NUdf::TUnboxedValue* resultRowItems = nullptr;
                resultRow = HolderFactory.CreateDirectArrayHolder(3, resultRowItems);
                resultRowItems[0] = Row;
                resultRowItems[1] = std::move(RightRow);
                auto rowCookie = TStreamLookupJoinRowCookie{.RowSeqNo=SeqNo, .LastRow=lastRow, .FirstRow=firstRow};
                resultRowItems[2] = NUdf::TUnboxedValuePod(rowCookie.Encode());
                rowStats.ReadRowsCount += (hasValue ? 1 : 0);
                rowStats.ReadBytesCount += StorageReadBytes;
                rowStats.ResultRowsCount += 1;
                rowStats.ResultBytesCount += LeftRowSize + RightRowSize;
            }

            if (!rightRow.empty()) {
                RightRowExist = true;
                AttachRightRow(
                    HolderFactory, Settings, ReadColumns, rightRow,
                    shardId);
            }

            return resultRow;
        }

        void AttachRightRow(const NMiniKQL::THolderFactory& HolderFactory, const TLookupSettings& Settings,
            const std::map<std::string, TSysTables::TTableColumnInfo>& ReadColumns,
            TConstArrayRef<TCell> rightRow,
            TMaybe<ui64> shardId = {})
        {
            NUdf::TUnboxedValue* rightRowItems = nullptr;
            RightRow = HolderFactory.CreateDirectArrayHolder(Settings.Columns.size(), rightRowItems);

            for (size_t colIndex = 0; colIndex < Settings.Columns.size(); ++colIndex) {
                const auto& column = Settings.Columns[colIndex];
                auto it = ReadColumns.find(column.Name);
                YQL_ENSURE(it != ReadColumns.end());

                if (IsSystemColumn(column.Name)) {
                    YQL_ENSURE(shardId);
                    NMiniKQL::FillSystemColumn(rightRowItems[colIndex], *shardId, column.Id, column.PType);
                    RightRowSize += sizeof(NUdf::TUnboxedValue);
                } else {
                    StorageReadBytes += rightRow[std::distance(ReadColumns.begin(), it)].Size();
                    rightRowItems[colIndex] = NMiniKQL::GetCellValue(rightRow[std::distance(ReadColumns.begin(), it)],
                        column.PType);
                    RightRowSize += NMiniKQL::GetUnboxedValueSize(rightRowItems[colIndex], column.PType).AllocatedBytes;
                }
            }
        }

        bool Completed() {
            return PendingReads.empty() && RightRowExist && LastRowReceived;
        }
    };

    struct TResultBatch {
        struct TResultRow {
            NUdf::TUnboxedValue Data;
            TReadResultStats Stats;
        };

        std::deque<TResultRow> Rows;
        ui32 FirstUnprocessedRow = 0;
        ui64 ProcessedRows = 0;
        ui64 ProcessingLeftRows = 0;
        bool FirstRow = false;
        bool LastRow = false;

        bool Finished() const {
            return Rows.empty();
        }

        bool ProcessedAllLeftRows() const {
            return ProcessingLeftRows == 0;
        }

        void FinishLeftRow() {
            --ProcessingLeftRows;
        }

        void AcceptLeftRow(bool firstRow, bool lastRow) {
            ++ProcessingLeftRows;
            if (firstRow)
                FirstRow = true;

            if (lastRow)
                LastRow = true;
        }

        bool Completed() const {
            return Rows.empty() && FirstRow && LastRow && ProcessingLeftRows == 0;
        }

        void AddRow(TReadResultStats&& stats, NUdf::TUnboxedValue&& data) {
            if (data.HasValue()) {
                Rows.emplace_back(std::move(data), std::move(stats));
                ++ProcessedRows;
            }
        }

        bool FirstRowInBatch() const {
            return ProcessedRows == 0;
        }
    };

    bool ShoulKeepRowsOrder() const {
        return Settings.KeepRowsOrder;
    }

    bool IsRowSeqNoValid(const ui64& seqNo) const {
        if (!ShoulKeepRowsOrder()) {
            return true;
        }

        // we should check row seqNo only if we need to keep the order
        return seqNo >= CurrentResultSeqNo;
    }

    void FillReadRequest(ui64 readId, THolder<TEvDataShard::TEvRead>& request, const std::vector<TOwnedTableRange>& ranges) {
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(Settings.TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(Settings.TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(Settings.TableId.SchemaVersion);

        for (const auto& [name, column] : ReadColumns) {
            if (!IsSystemColumn(name)) {
                record.AddColumns(column.Id);
            }
        }

        YQL_ENSURE(!ranges.empty());
        if (ranges.front().Point) {
            request->Keys.reserve(ranges.size());
            for (auto& range : ranges) {
                YQL_ENSURE(range.Point);
                request->Keys.emplace_back(TSerializedCellVec(range.From));
            }
        } else {
            request->Ranges.reserve(ranges.size());
            for (auto& range : ranges) {
                YQL_ENSURE(!range.Point);
                if (range.To.size() < Settings.KeyColumns.size()) {
                    // Absent cells mean infinity. So in prefix notation `To` should be inclusive.
                    request->Ranges.emplace_back(TSerializedTableRange(range.From, range.InclusiveFrom, range.To, true));
                } else {
                    request->Ranges.emplace_back(TSerializedTableRange(range));
                }
            }
        }
    }

    TConstArrayRef<TCell> ExtractKeyPrefix(const TOwnedTableRange& range) {
        if (range.From.size() == Settings.LookupKeyColumns.size()) {
            return range.From;
        }

        return range.From.subspan(0, Settings.LookupKeyColumns.size());
    }

    bool IsInputTriplet() {
        if (InputTupleType) {
            return InputTupleType->GetElementsCount() == 3;
        }

        auto inputTypeNode = NMiniKQL::DeserializeNode(TStringBuf{InputDesc.GetTransform().GetInputType()}, TypeEnv);
        YQL_ENSURE(inputTypeNode, "Failed to deserialize stream lookup transform output type");

        auto inputType = static_cast<NMiniKQL::TType*>(inputTypeNode);
        YQL_ENSURE(inputType->GetKind() == NMiniKQL::TType::EKind::Tuple, "Unexpected stream lookup output type");

        InputTupleType = AS_TYPE(NMiniKQL::TTupleType, inputType);
        return InputTupleType->GetElementsCount() == 3;
    }

    NMiniKQL::TStructType* GetLeftRowType() {
        if (LeftRowType) {
            // KIKIMR-23296: avoid allocating separate type structure for each lookup
            return LeftRowType;
        }
        YQL_ENSURE(InputDesc.HasTransform());

        auto outputTypeNode = NMiniKQL::DeserializeNode(TStringBuf{InputDesc.GetTransform().GetOutputType()}, TypeEnv);
        YQL_ENSURE(outputTypeNode, "Failed to deserialize stream lookup transform output type");

        auto outputType = static_cast<NMiniKQL::TType*>(outputTypeNode);
        YQL_ENSURE(outputType->GetKind() == NMiniKQL::TType::EKind::Tuple, "Unexpected stream lookup output type");

        const auto outputTupleType = AS_TYPE(NMiniKQL::TTupleType, outputType);
        YQL_ENSURE(outputTupleType->GetElementsCount() == 3);

        const auto outputLeftRowType = outputTupleType->GetElementType(0);
        YQL_ENSURE(outputLeftRowType->GetKind() == NMiniKQL::TType::EKind::Struct);

        LeftRowType = AS_TYPE(NMiniKQL::TStructType, outputLeftRowType);
        return LeftRowType;
    }

private:
    const NYql::NDqProto::TTaskInput& InputDesc;
    std::map<std::string, TSysTables::TTableColumnInfo> ReadColumns;
    std::deque<TUnprocessedLeftRow> UnprocessedRows;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    std::unordered_map<ui64, TReadState> ReadStateByReadId;
    absl::flat_hash_map<TOwnedCellVec, TLeftRowInfo, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> PendingLeftRowsByKey;
    std::unordered_map<ui64, TResultBatch> ResultRowsBySeqNo;
    ui64 InputRowSeqNo = 0;
    ui64 CurrentResultSeqNo = 0;
    NMiniKQL::TStructType* LeftRowType = nullptr;
    NKikimr::NMiniKQL::TTupleType* InputTupleType = nullptr;
};

std::unique_ptr<TKqpStreamLookupWorker> CreateStreamLookupWorker(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc) {

    TLookupSettings preparedSettings;
    preparedSettings.TablePath = std::move(settings.GetTable().GetPath());
    preparedSettings.TableId = MakeTableId(settings.GetTable());

    preparedSettings.AllowNullKeysPrefixSize = settings.HasAllowNullKeysPrefixSize() ? settings.GetAllowNullKeysPrefixSize() : 0;
    preparedSettings.KeepRowsOrder = settings.HasKeepRowsOrder() && settings.GetKeepRowsOrder();
    preparedSettings.LookupStrategy = settings.GetLookupStrategy();

    preparedSettings.KeyColumns.reserve(settings.GetKeyColumns().size());
    i32 keyOrder = 0;
    for (const auto& keyColumn : settings.GetKeyColumns()) {
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
        preparedSettings.KeyColumns.emplace(
            keyColumn.GetName(),
            TSysTables::TTableColumnInfo{
                keyColumn.GetName(),
                keyColumn.GetId(),
                typeInfo,
                keyColumn.GetTypeInfo().GetPgTypeMod(),
                keyOrder++
            }
        );
    }

    preparedSettings.LookupKeyColumns.reserve(settings.GetLookupKeyColumns().size());
    for (const auto& lookupKey : settings.GetLookupKeyColumns()) {
        auto columnIt = preparedSettings.KeyColumns.find(lookupKey);
        YQL_ENSURE(columnIt != preparedSettings.KeyColumns.end());
        preparedSettings.LookupKeyColumns.push_back(&columnIt->second);
    }

    preparedSettings.Columns.reserve(settings.GetColumns().size());
    for (const auto& column : settings.GetColumns()) {
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(column.GetTypeId(), column.GetTypeInfo());
        preparedSettings.Columns.emplace_back(TSysTables::TTableColumnInfo{
            column.GetName(),
            column.GetId(),
            typeInfo,
            column.GetTypeInfo().GetPgTypeMod()
        });
    }

    switch (settings.GetLookupStrategy()) {
        case NKqpProto::EStreamLookupStrategy::LOOKUP:
            return std::make_unique<TKqpLookupRows>(std::move(preparedSettings), typeEnv, holderFactory);
        case NKqpProto::EStreamLookupStrategy::JOIN:
        case NKqpProto::EStreamLookupStrategy::SEMI_JOIN:
            return std::make_unique<TKqpJoinRows>(std::move(preparedSettings), typeEnv, holderFactory, inputDesc);
        default:
            return {};
    }
}

std::unique_ptr<TKqpStreamLookupWorker> CreateLookupWorker(TLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory) {
    AFL_ENSURE(settings.LookupStrategy == NKqpProto::EStreamLookupStrategy::LOOKUP);
    AFL_ENSURE(!settings.KeepRowsOrder);
    AFL_ENSURE(!settings.AllowNullKeysPrefixSize);
    return std::make_unique<TKqpLookupRows>(std::move(settings), typeEnv, holderFactory);
}

} // namespace NKqp
} // namespace NKikimr
