#include "kqp_stream_lock_worker.h"

#include <algorithm>
#include <util/generic/hash.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/data_events/events.h>

namespace NKikimr {
namespace NKqp {

TKqpStreamLockWorker::TKqpStreamLockWorker(TKqpStreamLockSettings&& settings)
    : Settings(std::move(settings))
{
    KeyColumnTypes.resize(Settings.KeyColumns.size());
    KeyColumnIds.resize(Settings.KeyColumns.size());

    for (size_t i = 0; i < Settings.KeyColumns.size(); ++i) {
        const auto& col = Settings.KeyColumns[i];
        KeyColumnIds[i] = col.GetId();
        
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
            col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        KeyColumnTypes[i] = typeInfoMod.TypeInfo;
    }

    ColumnTypes.resize(Settings.Columns.size());
    ColumnIds.resize(Settings.Columns.size());

    for (size_t i = 0; i < Settings.Columns.size(); ++i) {
        const auto& col = Settings.Columns[i];
        ColumnIds[i] = col.GetId();
        
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
            col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        ColumnTypes[i] = typeInfoMod.TypeInfo;
    }

    KeyColumnPositions.resize(KeyColumnIds.size());
    for (size_t keyIdx = 0; keyIdx < KeyColumnIds.size(); ++keyIdx) {
        auto it = std::find(ColumnIds.begin(), ColumnIds.end(), KeyColumnIds[keyIdx]);
        YQL_ENSURE(it != ColumnIds.end(), "Key column not found in Columns");
        KeyColumnPositions[keyIdx] = std::distance(ColumnIds.begin(), it);
    }
}

TKqpStreamLockWorker::~TKqpStreamLockWorker() {
    Clear();
}

void TKqpStreamLockWorker::Clear() {
    InputRows.clear();
    BatchesByRequestId.clear();
}

void TKqpStreamLockWorker::AddInputRow(NUdf::TUnboxedValue row) {
    std::vector<TCell> cells(ColumnTypes.size());
    NMiniKQL::TStringProviderBackend backend;
    
    for (size_t colIdx = 0; colIdx < ColumnTypes.size(); ++colIdx) {
        auto value = row.GetElement(colIdx);
        cells[colIdx] = MakeCell(ColumnTypes[colIdx], value, backend, true);
    }
    
    InputRows.emplace_back(std::move(cells));
}

void TKqpStreamLockWorker::AddInputRow(TConstArrayRef<TCell> inputRow) {
    if (inputRow.size() != ColumnTypes.size()) {
        // TODO: tmp workaround for unique index locking
        TVector<TCell> cells(inputRow.begin(), inputRow.end());
        while (cells.size() < ColumnTypes.size()) {
            cells.push_back(TCell());
        }

        InputRows.emplace_back(TOwnedCellVec::Make(cells));
    } else {
        AFL_ENSURE(inputRow.size() == ColumnTypes.size());
        InputRows.emplace_back(TOwnedCellVec::Make(inputRow));
    }
}

TVector<TCell> TKqpStreamLockWorker::SerializeKeysToCellVec(const std::vector<TOwnedCellVec>& keys) {
    const size_t keyColumnCount = KeyColumnTypes.size();
    const size_t batchSize = keys.size();

    TVector<TCell> allCells;
    allCells.reserve(batchSize * keyColumnCount);

    for (size_t i = 0; i < batchSize; ++i) {
        for (const auto& cell : keys[i]) {
            allCells.push_back(cell);
        }
    }

    return allCells;
}

NUdf::TUnboxedValue TKqpStreamLockWorker::ConvertRowToUnboxedValue(const TOwnedCellVec& row) const {
    AFL_ENSURE(row.size() == ColumnTypes.size());
    NUdf::TUnboxedValue* rowItems = nullptr;
    auto result = Settings.HolderFactory.CreateDirectArrayHolder(ColumnTypes.size(), rowItems);

    for (size_t colIdx = 0; colIdx < ColumnTypes.size(); ++colIdx) {
        rowItems[colIdx] = NMiniKQL::GetCellValue(row[colIdx], ColumnTypes[colIdx]);
    }

    return result;
}

THolder<NEvents::TDataEvents::TEvLockRows> TKqpStreamLockWorker::BuildLockRequestMessage(
    ui64 requestId,
    const TVector<TCell>& allCells,
    size_t batchSize,
    size_t keyColumnCount)
{
    auto lockRequest = MakeHolder<NEvents::TDataEvents::TEvLockRows>(requestId);
    lockRequest->Record.SetLockId(Settings.LockTxId);
    lockRequest->Record.SetLockNodeId(Settings.LockNodeId);
    lockRequest->Record.SetLockMode(Settings.LockMode);

    TTableId tableId(Settings.Table.GetOwnerId(), Settings.Table.GetTableId(), Settings.Table.GetVersion());
    lockRequest->SetTableId(tableId);

    lockRequest->Record.MutableSnapshot()->SetStep(Settings.Snapshot.GetStep());
    lockRequest->Record.MutableSnapshot()->SetTxId(Settings.Snapshot.GetTxId());

    for (ui32 colId : KeyColumnIds) {
        lockRequest->Record.AddColumnIds(colId);
    }

    lockRequest->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);

    TString matrix = TSerializedCellMatrix::Serialize(allCells, batchSize, keyColumnCount);
    lockRequest->SetCellMatrix(std::move(matrix));

    return lockRequest;
}

TKqpStreamLockWorker::TLockRequestList TKqpStreamLockWorker::BuildLockRequests(
    const TPartitionInfo& partitioning, ui64& requestId)
{
    TLockRequestList requests;

    if (!partitioning || InputRows.empty()) {
        return requests;
    }

    const size_t keyColumnCount = KeyColumnTypes.size();
    THashMap<ui64, TVector<std::pair<ui64, TOwnedCellVec>>> keysByShard;

    for (ui64 rowIndex = 0; rowIndex < InputRows.size(); ++rowIndex) {
        const auto& row = InputRows[rowIndex];
        
        TVector<TCell> keyCellsData;
        keyCellsData.reserve(keyColumnCount);
        for (size_t keyIdx = 0; keyIdx < KeyColumnIds.size(); ++keyIdx) {
            size_t colPos = KeyColumnPositions[keyIdx];
            keyCellsData.push_back(row[colPos]);
        }
        TOwnedCellVec keyCells = TOwnedCellVec::Make(keyCellsData);

        auto shardIter = std::lower_bound(
            partitioning->begin(),
            partitioning->end(),
            TArrayRef(keyCells.data(), keyCells.size()),
            [this](const auto& partition, const auto& key) {
                const auto& range = *partition.Range;
                return 0 > CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                    range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
            });

        AFL_ENSURE(shardIter != partitioning->end());
        ui64 shardId = shardIter->ShardId;
        keysByShard[shardId].push_back({rowIndex, std::move(keyCells)});
    }

    for (auto& [shardId, keys] : keysByShard) {
        AFL_ENSURE(!keys.empty());

        ui64 currentRequestId = requestId++;

        size_t batchSize = keys.size();

        TRowBatchInfo batchInfo;
        batchInfo.BatchSize = batchSize;
        batchInfo.ShardId = shardId;
        batchInfo.Rows.reserve(batchSize);
        batchInfo.Keys.reserve(batchSize);
        for (auto& [rowIndex, keyCells] : keys) {
            batchInfo.Rows.emplace_back(std::move(InputRows[rowIndex]));
            batchInfo.Keys.emplace_back(std::move(keyCells));
        }

        auto allCells = SerializeKeysToCellVec(batchInfo.Keys);
        auto lockRequest = BuildLockRequestMessage(currentRequestId, allCells, batchSize, keyColumnCount);

        BatchesByRequestId[currentRequestId] = std::move(batchInfo);

        requests.emplace_back(shardId, std::move(lockRequest));
    }

    InputRows.clear();

    return requests;
}

TKqpStreamLockWorker::TLockRequestList TKqpStreamLockWorker::RebuildLockRequest(
    ui64 prevRequestId, ui64& newRequestId)
{
    TLockRequestList requests;

    auto batchIt = BatchesByRequestId.find(prevRequestId);
    if (batchIt == BatchesByRequestId.end()) {
        return requests;
    }

    auto& oldBatchInfo = batchIt->second;
    size_t batchSize = oldBatchInfo.BatchSize;
    ui64 shardId = oldBatchInfo.ShardId;

    const size_t keyColumnCount = KeyColumnTypes.size();
    auto allCells = SerializeKeysToCellVec(oldBatchInfo.Keys);

    if (allCells.empty()) {
        return requests;
    }

    ui64 currentRequestId = newRequestId++;

    auto lockRequest = BuildLockRequestMessage(currentRequestId, allCells, batchSize, keyColumnCount);

    TRowBatchInfo batchInfo;
    batchInfo.BatchSize = batchSize;
    batchInfo.ShardId = shardId;
    batchInfo.Rows = std::move(oldBatchInfo.Rows);
    batchInfo.Keys = std::move(oldBatchInfo.Keys);
    BatchesByRequestId[currentRequestId] = std::move(batchInfo);

    BatchesByRequestId.erase(prevRequestId);

    requests.emplace_back(shardId, std::move(lockRequest));

    return requests;
}

void TKqpStreamLockWorker::AddLockResult(ui64 requestId, NEvents::TDataEvents::TEvLockRowsResult* result) {
    auto requestIt = BatchesByRequestId.find(requestId);
    if (requestIt == BatchesByRequestId.end()) {
        return;
    }

    auto& batchInfo = requestIt->second;

    const auto& record = result->Record;
    
    const auto& lockedKeys = record.GetLockedKeys();
    const auto& modifiedKeys = record.GetModifiedKeys();

    THashSet<ui64> lockedSet(lockedKeys.begin(), lockedKeys.end());
    THashSet<ui64> modifiedSet(modifiedKeys.begin(), modifiedKeys.end());

    batchInfo.LockedFlags.resize(batchInfo.BatchSize, false);
    batchInfo.ModifiedFlags.resize(batchInfo.BatchSize, false);
    for (size_t i = 0; i < batchInfo.BatchSize; ++i) {
        AFL_ENSURE(lockedSet.contains(i)); // TODO: that's wrong, deleted rows shouldn't be locked
        batchInfo.LockedFlags[i] = true;
        batchInfo.ModifiedFlags[i] = modifiedSet.contains(i);
    }

    batchInfo.LockResultReceived = true;
}

void TKqpStreamLockWorker::ProcessRowsByLockResult(ui64 requestId, TProcessRowCallback callback) {
    auto it = BatchesByRequestId.find(requestId);
    if (it == BatchesByRequestId.end()) {
        return;
    }

    auto& batchInfo = it->second;
    AFL_ENSURE(batchInfo.LockResultReceived);

    for (size_t i = 0; i < batchInfo.BatchSize; ++i) {
        if (!batchInfo.LockedFlags[i]) {
            continue;
        }
        bool modified = batchInfo.ModifiedFlags[i];
        NUdf::TUnboxedValue row = ConvertRowToUnboxedValue(batchInfo.Rows[i]);
        callback(row, modified);
    }

    BatchesByRequestId.erase(it);
}

void TKqpStreamLockWorker::ProcessRowsByLockResult(ui64 requestId, TProcessRowCallbackOwned callback) {
    auto it = BatchesByRequestId.find(requestId);
    if (it == BatchesByRequestId.end()) {
        return;
    }

    auto& batchInfo = it->second;
    AFL_ENSURE(batchInfo.LockResultReceived);

    for (size_t i = 0; i < batchInfo.BatchSize; ++i) {
        if (!batchInfo.LockedFlags[i]) {
            continue;
        }
        bool modified = batchInfo.ModifiedFlags[i];
        callback(batchInfo.Rows[i], modified);
    }

    BatchesByRequestId.erase(it);
}

std::unique_ptr<TKqpStreamLockWorker> CreateStreamLockWorker(
    TKqpStreamLockSettings&& settings)
{
    return std::make_unique<TKqpStreamLockWorker>(std::move(settings));
}

} // namespace NKqp
} // namespace NKikimr
