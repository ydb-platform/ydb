#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <memory>
#include <vector>
#include <unordered_map>
#include <map>

namespace NScheme {
class TOwnedCellVec;
}

namespace NKikimr {
namespace NKqp {

struct TKqpStreamLockSettings {
    TKqpStreamLockSettings(const NMiniKQL::THolderFactory& holderFactory)
        : HolderFactory(holderFactory)
    {}

    NKqpProto::TKqpPhyTableId Table;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    ui64 LockTxId = 0;
    ui32 LockNodeId = 0;
    NKikimrDataEvents::ELockMode LockMode = NKikimrDataEvents::ELockMode::PESSIMISTIC_EXCLUSIVE;
    TString Database;
    NKikimrDataEvents::TMvccSnapshot Snapshot;
    ui64 QuerySpanId = 0;
    const NMiniKQL::THolderFactory& HolderFactory;
};

class TKqpStreamLockWorker {
public:
    using TLockRequestList = std::vector<std::pair<ui64, THolder<NEvents::TDataEvents::TEvLockRows>>>;
    using TPartitionInfo = std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>;

    struct TRowBatchInfo {
        size_t BatchSize = 0;
        ui64 ShardId = 0;
        std::vector<TOwnedCellVec> Rows;
        std::vector<TOwnedCellVec> Keys;
        TVector<bool> ModifiedFlags;
        TVector<bool> LockedFlags;
        bool LockResultReceived = false;
    };

public:
    TKqpStreamLockWorker(TKqpStreamLockSettings&& settings);

    ~TKqpStreamLockWorker();

    void AddInputRow(NUdf::TUnboxedValue row);

    void AddInputRow(TConstArrayRef<TCell> inputRow);

    TLockRequestList BuildLockRequests(const TPartitionInfo& partitioning, ui64& requestId);

    TLockRequestList RebuildLockRequest(ui64 prevRequestId, ui64& newRequestId);

    void AddLockResult(ui64 requestId, NEvents::TDataEvents::TEvLockRowsResult* result);

    ui64 GetRowCount() const { return InputRows.size(); }

    using TProcessRowCallback = std::function<void(NUdf::TUnboxedValue row, bool modified)>;
    void ProcessRowsByLockResult(ui64 requestId, TProcessRowCallback callback);

    using TProcessRowCallbackOwned = std::function<void(const TOwnedCellVec& row, bool modified)>;
    void ProcessRowsByLockResult(ui64 requestId, TProcessRowCallbackOwned callback);

    void Clear();

private:
    TOwnedCellVec SerializeRowKey(const NUdf::TUnboxedValue& row);

    TVector<TCell> SerializeKeysToCellVec(const std::vector<TOwnedCellVec>& keys);

    NUdf::TUnboxedValue ConvertRowToUnboxedValue(const TOwnedCellVec& row) const;

    THolder<NEvents::TDataEvents::TEvLockRows> BuildLockRequestMessage(
        ui64 requestId,
        const TVector<TCell>& allCells,
        size_t batchSize,
        size_t keyColumnCount);

    const TKqpStreamLockSettings Settings;

    std::vector<NScheme::TTypeInfo> KeyColumnTypes;
    std::vector<NScheme::TTypeInfo> ColumnTypes;
    TVector<ui32> KeyColumnIds;
    TVector<ui32> ColumnIds;
    std::vector<size_t> KeyColumnPositions;

    std::vector<TOwnedCellVec> InputRows;
    std::unordered_map<ui64, TRowBatchInfo> BatchesByRequestId;
};

} // namespace NKqp
} // namespace NKikimr

namespace NKikimr {
namespace NKqp {

std::unique_ptr<TKqpStreamLockWorker> CreateStreamLockWorker(
    TKqpStreamLockSettings&& settings);

} // namespace NKqp
} // namespace NKikimr
