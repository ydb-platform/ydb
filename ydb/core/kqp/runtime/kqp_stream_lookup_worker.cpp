#include "kqp_stream_lookup_worker.h"

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


}  // !namespace

TKqpStreamLookupWorker::TKqpStreamLookupWorker(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc)
    : Settings(std::move(settings))
    , TypeEnv(typeEnv)
    , HolderFactory(holderFactory)
    , InputDesc(inputDesc)
    , TablePath(Settings.GetTable().GetPath())
    , TableId(MakeTableId(Settings.GetTable())) {

    KeyColumns.reserve(Settings.GetKeyColumns().size());
    i32 keyOrder = 0;
    for (const auto& keyColumn : Settings.GetKeyColumns()) {
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());

        KeyColumns.emplace(
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

    LookupKeyColumns.reserve(Settings.GetLookupKeyColumns().size());
    for (const auto& lookupKey : Settings.GetLookupKeyColumns()) {
        auto columnIt = KeyColumns.find(lookupKey);
        YQL_ENSURE(columnIt != KeyColumns.end());
        LookupKeyColumns.push_back(&columnIt->second);
    }

    Columns.reserve(Settings.GetColumns().size());
    for (const auto& column : Settings.GetColumns()) {
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(column.GetTypeId(), column.GetTypeInfo());

        Columns.emplace_back(TSysTables::TTableColumnInfo{
            column.GetName(),
            column.GetId(),
            typeInfo,
            column.GetTypeInfo().GetPgTypeMod()
        });
    }

    KeyColumnTypes.resize(KeyColumns.size());
    for (const auto& [_, columnInfo] : KeyColumns) {
        YQL_ENSURE(columnInfo.KeyOrder < static_cast<i64>(KeyColumnTypes.size()));
        KeyColumnTypes[columnInfo.KeyOrder] = columnInfo.PType;
    }
}

TKqpStreamLookupWorker::~TKqpStreamLookupWorker() {
}

std::string TKqpStreamLookupWorker::GetTablePath() const {
    return TablePath;
}

TTableId TKqpStreamLookupWorker::GetTableId() const {
    return TableId;
}

class TKqpLookupRows : public TKqpStreamLookupWorker {
public:
    TKqpLookupRows(NKikimrKqp::TKqpStreamLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const NYql::NDqProto::TTaskInput& inputDesc)
        : TKqpStreamLookupWorker(std::move(settings), typeEnv, holderFactory, inputDesc) {
    }

    virtual ~TKqpLookupRows() {}

    void AddInputRow(NUdf::TUnboxedValue inputRow) final {
        NMiniKQL::TStringProviderBackend backend;
        std::vector<TCell> keyCells(LookupKeyColumns.size());
        for (size_t colId = 0; colId < LookupKeyColumns.size(); ++colId) {
            const auto* lookupKeyColumn = LookupKeyColumns[colId];
            YQL_ENSURE(lookupKeyColumn->KeyOrder < static_cast<i64>(keyCells.size()));
            // when making a cell we don't really need to make a copy of data, because
            // TOwnedCellVec will make its' own copy.
            keyCells[lookupKeyColumn->KeyOrder] = MakeCell(lookupKeyColumn->PType,
                inputRow.GetElement(colId), backend, /* copy */ false);
        }

        if (keyCells.size() < KeyColumns.size()) {
            // build prefix range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf])
            std::vector<TCell> fromCells(KeyColumns.size());
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

    std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui32 firstUnprocessedQuery,
        TMaybe<TOwnedCellVec> lastProcessedKey, ui64& newReadId) final {

        auto it = PendingKeysByReadId.find(prevReadId);
        if (it == PendingKeysByReadId.end()) {
            return {};
        }

        std::vector<TOwnedTableRange> unprocessedRanges;
        std::vector<TOwnedTableRange> unprocessedPoints;

        auto& ranges = it->second;

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

        PendingKeysByReadId.erase(it);

        std::vector<THolder<TEvDataShard::TEvRead>> requests;
        requests.reserve(unprocessedPoints.size() + unprocessedRanges.size());

        if (!unprocessedPoints.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedPoints);
            requests.emplace_back(std::move(request));
            PendingKeysByReadId.insert({newReadId, std::move(unprocessedPoints)});
        }

        if (!unprocessedRanges.empty()) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++newReadId, request, unprocessedRanges);
            requests.emplace_back(std::move(request));
            PendingKeysByReadId.insert({newReadId, std::move(unprocessedRanges)});
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
            PendingKeysByReadId.insert({readId, std::move(points)});
        }

        for (auto& [shardId, ranges] : rangesPerShard) {
            THolder<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
            FillReadRequest(++readId, request, ranges);
            readRequests.emplace_back(shardId, std::move(request));
            PendingKeysByReadId.insert({readId, std::move(ranges)});
        }

        return readRequests;
    }

    void AddResult(TShardReadResult result) final {
        const auto& record = result.ReadResult->Get()->Record;
        YQL_ENSURE(record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);

        auto it = PendingKeysByReadId.find(record.GetReadId());
        YQL_ENSURE(it != PendingKeysByReadId.end());

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
                YQL_ENSURE(resultRow.size() <= Columns.size(), "Result columns mismatch");

                NUdf::TUnboxedValue* rowItems = nullptr;
                auto row = HolderFactory.CreateDirectArrayHolder(Columns.size(), rowItems);

                i64 rowSize = 0;
                i64 storageRowSize = 0;
                for (size_t colIndex = 0, resultColIndex = 0; colIndex < Columns.size(); ++colIndex) {
                    const auto& column = Columns[colIndex];
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
                    auto it = PendingKeysByReadId.find(result.ReadResult->Get()->Record.GetReadId());
                    PendingKeysByReadId.erase(it);
                }

                ReadResults.pop_front();
            }
        }

        return resultStats;
    }

    bool AllRowsProcessed() final {
        return UnprocessedKeys.empty()
            && PendingKeysByReadId.empty()
            && ReadResults.empty();
    }

    void ResetRowsProcessing(ui64 readId, ui32 firstUnprocessedQuery, TMaybe<TOwnedCellVec> lastProcessedKey) final {
        auto it = PendingKeysByReadId.find(readId);
        if (it == PendingKeysByReadId.end()) {
            return;
        }

        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < it->second.size());
            auto unprocessedRange = it->second[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            UnprocessedKeys.emplace_back(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);
            ++firstUnprocessedQuery;
        }

        for (ui32 keyIdx = firstUnprocessedQuery; keyIdx < it->second.size(); ++keyIdx) {
            UnprocessedKeys.emplace_back(std::move(it->second[keyIdx]));
        }

        PendingKeysByReadId.erase(it);
    }

private:
    void FillReadRequest(ui64 readId, THolder<TEvDataShard::TEvRead>& request, const std::vector<TOwnedTableRange>& ranges) {
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (const auto& column : Columns) {
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

                if (range.To.size() < KeyColumns.size()) {
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
    std::unordered_map<ui64, std::vector<TOwnedTableRange>> PendingKeysByReadId;
    std::deque<TShardReadResult> ReadResults;
};

class TKqpJoinRows : public TKqpStreamLookupWorker {
public:
    TKqpJoinRows(NKikimrKqp::TKqpStreamLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const NYql::NDqProto::TTaskInput& inputDesc)
        : TKqpStreamLookupWorker(std::move(settings), typeEnv, holderFactory, inputDesc) {

        // read columns should contain join key and result columns
        for (auto joinKey : LookupKeyColumns) {
            ReadColumns.emplace(joinKey->Name, *joinKey);
        }

        for (auto column : Columns) {
            ReadColumns.emplace(column.Name, column);
        }
    }

    void AddInputRow(NUdf::TUnboxedValue inputRow) final {
        auto joinKey = inputRow.GetElement(0);
        std::vector<TCell> joinKeyCells(LookupKeyColumns.size());
        NMiniKQL::TStringProviderBackend backend;

        if (joinKey.HasValue()) {
            for (size_t colId = 0; colId < LookupKeyColumns.size(); ++colId) {
                const auto* joinKeyColumn = LookupKeyColumns[colId];
                YQL_ENSURE(joinKeyColumn->KeyOrder < static_cast<i64>(joinKeyCells.size()));
                // when making a cell we don't really need to make a copy of data, because
                // TOwnedCellVec will make its' own copy.
                joinKeyCells[joinKeyColumn->KeyOrder] = MakeCell(joinKeyColumn->PType,
                    joinKey.GetElement(colId), backend,  /* copy */ false);
            }
        }

        UnprocessedRows.emplace_back(std::make_pair(TOwnedCellVec(joinKeyCells), std::move(inputRow.GetElement(1))));
    }

    bool IsOverloaded() final {
        return UnprocessedRows.size() >= MAX_IN_FLIGHT_LIMIT || PendingLeftRowsByKey.size() >= MAX_IN_FLIGHT_LIMIT || ResultRowsBySeqNo.size() >= MAX_IN_FLIGHT_LIMIT;
    }

    std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui32 firstUnprocessedQuery,
        TMaybe<TOwnedCellVec> lastProcessedKey, ui64& newReadId) final {

        auto readIt = PendingKeysByReadId.find(prevReadId);
        if (readIt == PendingKeysByReadId.end()) {
            return {};
        }

        std::vector<TOwnedTableRange> unprocessedRanges;
        std::vector<TOwnedTableRange> unprocessedPoints;

        auto& ranges = readIt->second;

        if (lastProcessedKey) {
            YQL_ENSURE(firstUnprocessedQuery < ranges.size());
            auto unprocessedRange = ranges[firstUnprocessedQuery];
            YQL_ENSURE(!unprocessedRange.Point);

            TOwnedTableRange range(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);

            auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());
            leftRowIt->second.PendingReads.erase(prevReadId);

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

        PendingKeysByReadId.erase(readIt);

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

            PendingKeysByReadId.insert({newReadId, std::move(unprocessedPoints)});
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

            PendingKeysByReadId.insert({newReadId, std::move(unprocessedRanges)});
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
            auto [joinKey, leftData] = UnprocessedRows.front();

            if (PendingLeftRowsByKey.contains(joinKey)) {
                // TODO: skip key duplicate
                break;
            }

            auto isKeyAllowed = [&](const TOwnedCellVec& cellVec) {
                auto allowNullKeysPrefixSize = Settings.HasAllowNullKeysPrefixSize() ? Settings.GetAllowNullKeysPrefixSize() : 0;
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

            UnprocessedRows.pop_front();
            if (isKeyAllowed(joinKey)) {
                std::vector <std::pair<ui64, TOwnedTableRange>> partitions;
                if (joinKey.size() < KeyColumns.size()) {
                    // build prefix range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf])
                    std::vector<TCell> fromCells(KeyColumns.size() - joinKey.size());
                    fromCells.insert(fromCells.begin(), joinKey.begin(), joinKey.end());
                    bool fromInclusive = true;
                    bool toInclusive = false;

                    partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(),
                        TOwnedTableRange(fromCells, fromInclusive, joinKey, toInclusive)
                    );
                } else {
                    // full pk, build point
                    partitions = GetRangePartitioning(partitioning, GetKeyColumnTypes(), TOwnedTableRange(joinKey));
                }

                for (auto[shardId, range] : partitions) {
                    if (range.Point) {
                        pointsPerShard[shardId].push_back(std::move(range));
                    } else {
                        rangesPerShard[shardId].push_back(std::move(range));
                    }
                }
            }

            PendingLeftRowsByKey.insert(std::make_pair(std::move(joinKey), TLeftRowInfo{std::move(leftData), InputRowSeqNo++}));
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

            PendingKeysByReadId.insert({readId, std::move(points)});
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

            PendingKeysByReadId.insert({readId, std::move(ranges)});
        }

        return requests;
    }

    void AddResult(TShardReadResult result) final {
        const auto& record = result.ReadResult->Get()->Record;
        YQL_ENSURE(record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);

        auto pendingKeysIt = PendingKeysByReadId.find(record.GetReadId());
        YQL_ENSURE(pendingKeysIt != PendingKeysByReadId.end());

        for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
            const auto& row = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
            // result can contain fewer columns because of system columns
            YQL_ENSURE(row.size() <= ReadColumns.size(), "Result columns mismatch");

            std::vector<TCell> joinKeyCells(LookupKeyColumns.size());
            for (size_t joinKeyColumn = 0; joinKeyColumn < LookupKeyColumns.size(); ++joinKeyColumn) {
                auto columnIt = ReadColumns.find(LookupKeyColumns[joinKeyColumn]->Name);
                YQL_ENSURE(columnIt != ReadColumns.end());
                joinKeyCells[LookupKeyColumns[joinKeyColumn]->KeyOrder] = row[std::distance(ReadColumns.begin(), columnIt)];
            }

            auto leftRowIt = PendingLeftRowsByKey.find(joinKeyCells);
            YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());

            if (Settings.GetLookupStrategy() == NKqpProto::EStreamLookupStrategy::SEMI_JOIN && leftRowIt->second.RightRowExist) {
                // semi join should return one result row per key
                continue;
            }

            TReadResultStats rowStats;
            auto resultRow = TryBuildResultRow(leftRowIt->second, row, rowStats, result.ShardId);
            YQL_ENSURE(IsRowSeqNoValid(leftRowIt->second.SeqNo));
            ResultRowsBySeqNo[leftRowIt->second.SeqNo].Rows.emplace_back(std::move(resultRow), std::move(rowStats));
        }

        if (record.GetFinished()) {
            for (const auto& key : pendingKeysIt->second) {
                auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(key));
                if (leftRowIt != PendingLeftRowsByKey.end()) {
                    leftRowIt->second.PendingReads.erase(record.GetReadId());

                    // row is considered processed when all reads are finished
                    // and at least one right row is found
                    const bool leftRowProcessed = leftRowIt->second.PendingReads.empty()
                        && leftRowIt->second.RightRowExist;
                    if (leftRowProcessed) {
                        YQL_ENSURE(IsRowSeqNoValid(leftRowIt->second.SeqNo));
                        ResultRowsBySeqNo[leftRowIt->second.SeqNo].Completed = true;
                        PendingLeftRowsByKey.erase(leftRowIt);
                    }
                }
            }

            PendingKeysByReadId.erase(pendingKeysIt);
        }
    }

    bool AllRowsProcessed() final {
        return UnprocessedRows.empty()
            && UnprocessedKeys.empty()
            && PendingKeysByReadId.empty()
            && ResultRowsBySeqNo.empty()
            && PendingLeftRowsByKey.empty();
    }

    void ResetRowsProcessing(ui64 readId, ui32 firstUnprocessedQuery, TMaybe<TOwnedCellVec> lastProcessedKey) final {
        auto readIt = PendingKeysByReadId.find(readId);
        if (readIt == PendingKeysByReadId.end()) {
            return;
        }

        auto& ranges = readIt->second;

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

        PendingKeysByReadId.erase(readIt);
    }

    TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) final {
        TReadResultStats resultStats;
        batch.clear();

        // we should process left rows that haven't matches on the right
        for (auto leftRowIt = PendingLeftRowsByKey.begin(); leftRowIt != PendingLeftRowsByKey.end();) {
            const bool leftRowShouldBeProcessed = leftRowIt->second.PendingReads.empty()
                && !leftRowIt->second.RightRowExist;

            if (leftRowShouldBeProcessed) {
                TReadResultStats rowStats;
                auto resultRow = TryBuildResultRow(leftRowIt->second, {}, rowStats);
                YQL_ENSURE(IsRowSeqNoValid(leftRowIt->second.SeqNo));
                auto& result = ResultRowsBySeqNo[leftRowIt->second.SeqNo];
                result.Rows.emplace_back(std::move(resultRow), std::move(rowStats));
                result.Completed = true;
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
            for (; result.FirstUnprocessedRow < result.Rows.size(); ++result.FirstUnprocessedRow) {
                auto& row = result.Rows[result.FirstUnprocessedRow];

                if (resultStats.ResultRowsCount && resultStats.ResultBytesCount + row.Stats.ResultBytesCount > (ui64)freeSpace) {
                    resultStats.SizeLimitExceeded = true;
                    break;
                }

                batch.emplace_back(std::move(row.Data));
                resultStats.Add(row.Stats);
            }

            if (result.FirstUnprocessedRow == result.Rows.size()) {
                if (ShoulKeepRowsOrder()) {
                    // we can increment seqNo only if current result is completed
                    if (result.Completed) {
                        ResultRowsBySeqNo.erase(resultIt);
                        ++CurrentResultSeqNo;
                    } else {
                        break;
                    }
                } else {
                     ResultRowsBySeqNo.erase(resultIt);
                }
            }
        }

        return resultStats;
    }

    ~TKqpJoinRows() {
        UnprocessedRows.clear();
        PendingLeftRowsByKey.clear();
        ResultRowsBySeqNo.clear();
    }
private:
    struct TLeftRowInfo {
        TLeftRowInfo(NUdf::TUnboxedValue row, ui64 seqNo) : Row(std::move(row)), SeqNo(seqNo) {
        }

        NUdf::TUnboxedValue Row;
        std::unordered_set<ui64> PendingReads;
        bool RightRowExist = false;
        const ui64 SeqNo;
    };

    struct TResultBatch {
        struct TResultRow {
            NUdf::TUnboxedValue Data;
            TReadResultStats Stats;
        };

        std::vector<TResultRow> Rows;
        ui32 FirstUnprocessedRow = 0;
        bool Completed = false;
    };

    bool ShoulKeepRowsOrder() const {
        return Settings.HasKeepRowsOrder() && Settings.GetKeepRowsOrder();
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

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

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
                if (range.To.size() < KeyColumns.size()) {
                    // Absent cells mean infinity. So in prefix notation `To` should be inclusive.
                    request->Ranges.emplace_back(TSerializedTableRange(range.From, range.InclusiveFrom, range.To, true));
                } else {
                    request->Ranges.emplace_back(TSerializedTableRange(range));
                }
            }
        }
    }

    TConstArrayRef<TCell> ExtractKeyPrefix(const TOwnedTableRange& range) {
        if (range.From.size() == LookupKeyColumns.size()) {
            return range.From;
        }

        return range.From.subspan(0, LookupKeyColumns.size());
    }

    NMiniKQL::TStructType* GetLeftRowType() const {
        YQL_ENSURE(InputDesc.HasTransform());

        auto outputTypeNode = NMiniKQL::DeserializeNode(TStringBuf{InputDesc.GetTransform().GetOutputType()}, TypeEnv);
        YQL_ENSURE(outputTypeNode, "Failed to deserialize stream lookup transform output type");

        auto outputType = static_cast<NMiniKQL::TType*>(outputTypeNode);
        YQL_ENSURE(outputType->GetKind() == NMiniKQL::TType::EKind::Tuple, "Unexpected stream lookup output type");

        const auto outputTupleType = AS_TYPE(NMiniKQL::TTupleType, outputType);
        YQL_ENSURE(outputTupleType->GetElementsCount() == 2);

        const auto outputLeftRowType = outputTupleType->GetElementType(0);
        YQL_ENSURE(outputLeftRowType->GetKind() == NMiniKQL::TType::EKind::Struct);

        return AS_TYPE(NMiniKQL::TStructType, outputLeftRowType);
    }

    NUdf::TUnboxedValue TryBuildResultRow(TLeftRowInfo& leftRowInfo, TConstArrayRef<TCell> rightRow,
        TReadResultStats& rowStats, TMaybe<ui64> shardId = {}) {

        NUdf::TUnboxedValue* resultRowItems = nullptr;
        auto resultRow = HolderFactory.CreateDirectArrayHolder(2, resultRowItems);

        ui64 leftRowSize = 0;
        ui64 rightRowSize = 0;

        resultRowItems[0] = leftRowInfo.Row;
        auto leftRowType = GetLeftRowType();
        YQL_ENSURE(leftRowType);

        i64 storageReadBytes = 0;

        leftRowSize = NYql::NDq::TDqDataSerializer::EstimateSize(leftRowInfo.Row, leftRowType);

        if (!rightRow.empty()) {
            leftRowInfo.RightRowExist = true;

            NUdf::TUnboxedValue* rightRowItems = nullptr;
            resultRowItems[1] = HolderFactory.CreateDirectArrayHolder(Columns.size(), rightRowItems);

            for (size_t colIndex = 0; colIndex < Columns.size(); ++colIndex) {
                const auto& column = Columns[colIndex];
                auto it = ReadColumns.find(column.Name);
                YQL_ENSURE(it != ReadColumns.end());

                if (IsSystemColumn(column.Name)) {
                    YQL_ENSURE(shardId);
                    NMiniKQL::FillSystemColumn(rightRowItems[colIndex], *shardId, column.Id, column.PType);
                    rightRowSize += sizeof(NUdf::TUnboxedValue);
                } else {
                    storageReadBytes += rightRow[std::distance(ReadColumns.begin(), it)].Size();
                    rightRowItems[colIndex] = NMiniKQL::GetCellValue(rightRow[std::distance(ReadColumns.begin(), it)],
                        column.PType);
                    rightRowSize += NMiniKQL::GetUnboxedValueSize(rightRowItems[colIndex], column.PType).AllocatedBytes;
                }
            }
        } else {
            resultRowItems[1] = NUdf::TUnboxedValuePod();
        }

        rowStats.ReadRowsCount += (leftRowInfo.RightRowExist ? 1 : 0);
        // TODO: use datashard statistics KIKIMR-16924
        rowStats.ReadBytesCount += storageReadBytes;
        rowStats.ResultRowsCount += 1;
        rowStats.ResultBytesCount += leftRowSize + rightRowSize;

        return resultRow;
    }

private:
    std::map<std::string, TSysTables::TTableColumnInfo> ReadColumns;
    std::deque<std::pair<TOwnedCellVec, NUdf::TUnboxedValue>> UnprocessedRows;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    std::unordered_map<ui64, std::vector<TOwnedTableRange>> PendingKeysByReadId;
    absl::flat_hash_map<TOwnedCellVec, TLeftRowInfo, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> PendingLeftRowsByKey;
    std::unordered_map<ui64, TResultBatch> ResultRowsBySeqNo;
    ui64 InputRowSeqNo = 0;
    ui64 CurrentResultSeqNo = 0;
};

std::unique_ptr<TKqpStreamLookupWorker> CreateStreamLookupWorker(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc) {

    switch (settings.GetLookupStrategy()) {
        case NKqpProto::EStreamLookupStrategy::LOOKUP:
            return std::make_unique<TKqpLookupRows>(std::move(settings), typeEnv, holderFactory, inputDesc);
        case NKqpProto::EStreamLookupStrategy::JOIN:
        case NKqpProto::EStreamLookupStrategy::SEMI_JOIN:
            return std::make_unique<TKqpJoinRows>(std::move(settings), typeEnv, holderFactory, inputDesc);
        default:
            return {};
    }
}

} // namespace NKqp
} // namespace NKikimr
