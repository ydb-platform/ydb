#include "kqp_stream_lookup_worker.h"

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/yql/minikql/mkql_node_serialization.h>

namespace NKikimr {
namespace NKqp {

namespace {
std::vector<std::pair<ui64, TOwnedTableRange>> GetRangePartitioning(const TKqpStreamLookupWorker::TPartitionInfo& partitionInfo,
    const std::vector<NScheme::TTypeInfo>& keyColumnTypes, const TOwnedTableRange& range) {

    YQL_ENSURE(partitionInfo);

    std::vector<TCell> minusInf(keyColumnTypes.size());

    std::vector<std::pair<ui64, TOwnedTableRange>> rangePartition;
    for (size_t idx = 0; idx < partitionInfo->size(); ++idx) {
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

NScheme::TTypeInfo UnpackTypeInfo(NKikimr::NMiniKQL::TType* type) {
    YQL_ENSURE(type);

    if (type->GetKind() == NMiniKQL::TType::EKind::Pg) {
        auto pgType = static_cast<NMiniKQL::TPgType*>(type);
        auto pgTypeId = pgType->GetTypeId();
        return NScheme::TTypeInfo(NScheme::NTypeIds::Pg, NPg::TypeDescFromPgTypeId(pgTypeId));
    } else {
        bool isOptional = false;
        auto dataType = NMiniKQL::UnpackOptionalData(type, isOptional);
        return NScheme::TTypeInfo(dataType->GetSchemeType());
    }
}

struct THashableKey {
    TConstArrayRef<TCell> Cells;

    template <typename H>
    friend H AbslHashValue(H h, const THashableKey& key) {
        h = H::combine(std::move(h), key.Cells.size());
        for (const TCell& cell : key.Cells) {
            h = H::combine(std::move(h), cell.IsNull());
            if (!cell.IsNull()) {
                h = H::combine(std::move(h), cell.Size());
                h = H::combine_contiguous(std::move(h), cell.Data(), cell.Size());
            }
        }
        return h;
    }
};

struct TKeyHash {
    using is_transparent = void;

    bool operator()(TConstArrayRef<TCell> key) const {
        return absl::Hash<THashableKey>()(THashableKey{ key });
    }
};

struct TKeyEq {
    using is_transparent = void;

    bool operator()(TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) const {
        if (a.size() != b.size()) {
            return false;
        }

        const TCell* pa = a.data();
        const TCell* pb = b.data();
        if (pa == pb) {
            return true;
        }

        size_t left = a.size();
        while (left > 0) {
            if (pa->IsNull()) {
                if (!pb->IsNull()) {
                    return false;
                }
            } else {
                if (pb->IsNull()) {
                    return false;
                }
                if (pa->Size() != pb->Size()) {
                    return false;
                }
                if (pa->Size() > 0 && ::memcmp(pa->Data(), pb->Data(), pa->Size()) != 0) {
                    return false;
                }
            }
            ++pa;
            ++pb;
            --left;
        }

        return true;
    }
};
}  // !namespace

TKqpStreamLookupWorker::TKqpStreamLookupWorker(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc)
    : TypeEnv(typeEnv)
    , HolderFactory(holderFactory)
    , InputDesc(inputDesc)
    , TablePath(settings.GetTable().GetPath())
    , TableId(MakeTableId(settings.GetTable()))
    , Strategy(settings.GetLookupStrategy()) {

    KeyColumns.reserve(settings.GetKeyColumns().size());
    i32 keyOrder = 0;
    for (const auto& keyColumn : settings.GetKeyColumns()) {
        KeyColumns.emplace(
            keyColumn.GetName(),
            TSysTables::TTableColumnInfo{
                keyColumn.GetName(),
                keyColumn.GetId(),
                NScheme::TTypeInfo{
                    static_cast<NScheme::TTypeId>(keyColumn.GetTypeId()),
                    keyColumn.GetTypeId() == NScheme::NTypeIds::Pg
                        ? NPg::TypeDescFromPgTypeId(keyColumn.GetTypeInfo().GetPgTypeId())
                        : nullptr
                },
                keyColumn.GetTypeInfo().GetPgTypeMod(),
                keyOrder++
            }
        );
    }

    LookupKeyColumns.reserve(settings.GetLookupKeyColumns().size());
    for (const auto& lookupKey : settings.GetLookupKeyColumns()) {
        auto columnIt = KeyColumns.find(lookupKey);
        YQL_ENSURE(columnIt != KeyColumns.end());
        LookupKeyColumns.push_back(&columnIt->second);
    }

    Columns.reserve(settings.GetColumns().size());
    for (const auto& column : settings.GetColumns()) {
        Columns.emplace_back(TSysTables::TTableColumnInfo{
            column.GetName(),
            column.GetId(),
            NScheme::TTypeInfo{static_cast<NScheme::TTypeId>(column.GetTypeId()),
                column.GetTypeId() == NScheme::NTypeIds::Pg
                    ? NPg::TypeDescFromPgTypeId(column.GetTypeInfo().GetPgTypeId())
                    : nullptr,
            },
            column.GetTypeInfo().GetPgTypeMod()
        });
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

std::vector<NScheme::TTypeInfo> TKqpStreamLookupWorker::GetKeyColumnTypes() const {
    std::vector<NScheme::TTypeInfo> keyColumnTypes(KeyColumns.size());
    for (const auto& [_, columnInfo] : KeyColumns) {
        YQL_ENSURE(columnInfo.KeyOrder < static_cast<i64>(keyColumnTypes.size()));
        keyColumnTypes[columnInfo.KeyOrder] = columnInfo.PType;
    }

    return keyColumnTypes;
}

class TKqpLookupRows : public TKqpStreamLookupWorker {
public:
    TKqpLookupRows(NKikimrKqp::TKqpStreamLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const NYql::NDqProto::TTaskInput& inputDesc)
        : TKqpStreamLookupWorker(std::move(settings), typeEnv, holderFactory, inputDesc) {
    }

    virtual ~TKqpLookupRows() {}

    void AddInputRow(NUdf::TUnboxedValue inputRow) final {
        std::vector<TCell> keyCells(LookupKeyColumns.size());
        for (size_t colId = 0; colId < LookupKeyColumns.size(); ++colId) {
            const auto* lookupKeyColumn = LookupKeyColumns[colId];
            YQL_ENSURE(lookupKeyColumn->KeyOrder < static_cast<i64>(keyCells.size()));
            keyCells[lookupKeyColumn->KeyOrder] = MakeCell(lookupKeyColumn->PType,
                inputRow.GetElement(colId), TypeEnv, /* copy */ true);
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

    TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) final {
        TReadResultStats resultStats;
        bool sizeLimitExceeded = false;
        batch.clear();

        while (!ReadResults.empty() && !sizeLimitExceeded) {
            auto& result = ReadResults.front();
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

                if (rowSize > freeSpace - (i64)resultStats.ResultBytesCount) {
                    row.DeleteUnreferenced();
                    sizeLimitExceeded = true;
                    break;
                }

                batch.push_back(std::move(row));

                resultStats.ReadRowsCount += 1;
                resultStats.ReadBytesCount += rowSize;
                resultStats.ResultRowsCount += 1;
                resultStats.ResultBytesCount += rowSize;
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

        if (joinKey.HasValue()) {
            for (size_t colId = 0; colId < LookupKeyColumns.size(); ++colId) {
                const auto* joinKeyColumn = LookupKeyColumns[colId];
                YQL_ENSURE(joinKeyColumn->KeyOrder < static_cast<i64>(joinKeyCells.size()));
                joinKeyCells[joinKeyColumn->KeyOrder] = MakeCell(joinKeyColumn->PType,
                    joinKey.GetElement(colId), TypeEnv, true);
            }
        }

        UnprocessedRows.emplace_back(std::make_pair(TOwnedCellVec(joinKeyCells), std::move(inputRow.GetElement(1))));
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

            unprocessedRanges.emplace_back(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);
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

            auto hasNulls = [](const TOwnedCellVec& cellVec) {
                for (const auto& cell : cellVec) {
                    if (cell.IsNull()) {
                        return true;
                    }
                }

                return false;
            };

            UnprocessedRows.pop_front();
            if (!hasNulls(joinKey)) {  // don't use nulls as lookup keys, because null != null
                std::vector <std::pair<ui64, TOwnedTableRange>> partitions;
                if (joinKey.size() < KeyColumns.size()) {
                    // build prefix range [[key_prefix, NULL, ..., NULL], [key_prefix, +inf, ..., +inf])
                    std::vector <TCell> fromCells(KeyColumns.size());
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

            PendingLeftRowsByKey.insert(std::make_pair(std::move(joinKey), TLeftRowInfo{std::move(leftData)}));
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

        auto it = PendingKeysByReadId.find(record.GetReadId());
        YQL_ENSURE(it != PendingKeysByReadId.end());

        ReadResults.emplace_back(std::move(result));
    }

    bool AllRowsProcessed() final {
        return UnprocessedRows.empty()
            && UnprocessedKeys.empty()
            && PendingKeysByReadId.empty()
            && ReadResults.empty()
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

            UnprocessedKeys.emplace_back(*lastProcessedKey, false,
                unprocessedRange.GetOwnedTo(), unprocessedRange.InclusiveTo);
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
        bool sizeLimitExceeded = false;
        batch.clear();

        while (!ReadResults.empty() && !sizeLimitExceeded) {
            auto& result = ReadResults.front();

            for (; result.UnprocessedResultRow < result.ReadResult->Get()->GetRowsCount(); ++result.UnprocessedResultRow) {
                const auto& row = result.ReadResult->Get()->GetCells(result.UnprocessedResultRow);
                YQL_ENSURE(row.size() <= ReadColumns.size(), "Result columns mismatch");

                std::vector<TCell> joinKeyCells(LookupKeyColumns.size());
                for (size_t joinKeyIdx = 0; joinKeyIdx < LookupKeyColumns.size(); ++joinKeyIdx) {
                    auto it = ReadColumns.find(LookupKeyColumns[joinKeyIdx]->Name);
                    YQL_ENSURE(it != ReadColumns.end());
                    joinKeyCells[LookupKeyColumns[joinKeyIdx]->KeyOrder] = row[std::distance(ReadColumns.begin(), it)];
                }

                auto leftRowIt = PendingLeftRowsByKey.find(joinKeyCells);
                YQL_ENSURE(leftRowIt != PendingLeftRowsByKey.end());

                if (Strategy == NKqpProto::EStreamLookupStrategy::SEMI_JOIN && leftRowIt->second.RightRowExist) {
                    // Semi join should return one result row per key
                    continue;
                }

                TReadResultStats rowStats;
                i64 availableSpace = freeSpace - (i64)resultStats.ResultBytesCount;
                auto resultRow = TryBuildResultRow(leftRowIt->second, row, rowStats, availableSpace, result.ShardId);

                if (!resultRow.HasValue()) {
                    sizeLimitExceeded = true;
                    break;
                }

                batch.push_back(std::move(resultRow));
                resultStats.Add(rowStats);
            }

            if (result.UnprocessedResultRow == result.ReadResult->Get()->GetRowsCount()) {
                if (result.ReadResult->Get()->Record.GetFinished()) {
                    auto it = PendingKeysByReadId.find(result.ReadResult->Get()->Record.GetReadId());
                    YQL_ENSURE(it != PendingKeysByReadId.end());

                    for (const auto& range : it->second) {
                        auto leftRowIt = PendingLeftRowsByKey.find(ExtractKeyPrefix(range));
                        if (leftRowIt != PendingLeftRowsByKey.end()) {
                            leftRowIt->second.PendingReads.erase(result.ReadResult->Get()->Record.GetReadId());

                            const bool leftRowCanBeDeleted = leftRowIt->second.PendingReads.empty()
                                && leftRowIt->second.RightRowExist;
                            if (leftRowCanBeDeleted) {
                                PendingLeftRowsByKey.erase(leftRowIt);
                            }
                        }
                    }

                    PendingKeysByReadId.erase(it);
                }

                ReadResults.pop_front();
            }
        }

        if (!sizeLimitExceeded) {
            for (auto leftRowIt = PendingLeftRowsByKey.begin(); leftRowIt != PendingLeftRowsByKey.end();) {
                const bool leftRowCanBeSent = leftRowIt->second.PendingReads.empty()
                    && !leftRowIt->second.RightRowExist;

                if (leftRowCanBeSent) {
                    TReadResultStats rowStats;
                    i64 availableSpace = freeSpace - (i64)resultStats.ResultBytesCount;
                    auto resultRow = TryBuildResultRow(leftRowIt->second, {}, rowStats, availableSpace);

                    if (!resultRow.HasValue()) {
                        break;
                    }

                    batch.push_back(std::move(resultRow));
                    resultStats.Add(rowStats);
                    PendingLeftRowsByKey.erase(leftRowIt++);
                } else {
                    ++leftRowIt;
                }
            }
        }

        return resultStats;
    }

    ~TKqpJoinRows() {
        UnprocessedRows.clear();
        PendingLeftRowsByKey.clear();
    }
private:
    struct TLeftRowInfo {
        TLeftRowInfo(NUdf::TUnboxedValue row) : Row(std::move(row)) {
        }

        NUdf::TUnboxedValue Row;
        std::unordered_set<ui64> PendingReads;
        bool RightRowExist = false;
    };

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
         TReadResultStats& rowStats, i64 freeSpace, TMaybe<ui64> shardId = {}) {

        NUdf::TUnboxedValue* resultRowItems = nullptr;
        auto resultRow = HolderFactory.CreateDirectArrayHolder(2, resultRowItems);

        ui64 leftRowSize = 0;
        ui64 rightRowSize = 0;

        resultRowItems[0] = leftRowInfo.Row;
        auto leftRowType = GetLeftRowType();
        YQL_ENSURE(leftRowType);

        for (size_t i = 0; i < leftRowType->GetMembersCount(); ++i) {
            auto columnTypeInfo = UnpackTypeInfo(leftRowType->GetMemberType(i));
            leftRowSize += NMiniKQL::GetUnboxedValueSize(leftRowInfo.Row.GetElement(i), columnTypeInfo).AllocatedBytes;
        }

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
        rowStats.ReadBytesCount += rightRowSize;
        rowStats.ResultRowsCount += 1;
        rowStats.ResultBytesCount += leftRowSize + rightRowSize;

        if (rowStats.ResultBytesCount > (ui64)freeSpace) {
            resultRow.DeleteUnreferenced();
            rowStats.Clear();
        }

        return resultRow;
    }

private:
    std::map<std::string, TSysTables::TTableColumnInfo> ReadColumns;
    std::deque<std::pair<TOwnedCellVec, NUdf::TUnboxedValue>> UnprocessedRows;
    std::deque<TOwnedTableRange> UnprocessedKeys;
    std::unordered_map<ui64, std::vector<TOwnedTableRange>> PendingKeysByReadId;
    absl::flat_hash_map<TOwnedCellVec, TLeftRowInfo, TKeyHash, TKeyEq> PendingLeftRowsByKey;
    std::deque<TShardReadResult> ReadResults;
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
