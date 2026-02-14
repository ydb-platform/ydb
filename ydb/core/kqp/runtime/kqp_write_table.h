#pragma once

#include <util/generic/ptr.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NKqp {

using TRowsRef = TConstArrayRef<TConstArrayRef<TCell>>;

class IDataBatch : public TThrRefBase {
public:
    virtual TString SerializeToString() const = 0;
    virtual i64 GetSerializedMemory() const = 0;
    virtual i64 GetMemory() const = 0;
    virtual bool IsEmpty() const = 0;
    virtual size_t GetRowsCount() const = 0;

    virtual std::shared_ptr<void> ExtractBatch() = 0;

    virtual void AttachAlloc(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) = 0;
    virtual void DetachAlloc() = 0;
    virtual bool AttachedAlloc() const = 0;
};

using IDataBatchPtr = TIntrusivePtr<IDataBatch>;

class IRowsBatcher : public TThrRefBase {
public:
    virtual bool IsEmpty() const = 0;
    virtual i64 GetMemory() const = 0;

    virtual void AddCell(const TCell& cell) = 0;
    virtual void AddRow() = 0;
    virtual IDataBatchPtr Flush() = 0;
};

using IRowsBatcherPtr = TIntrusivePtr<IRowsBatcher>;

IRowsBatcherPtr CreateRowsBatcher(
    size_t columnsCount,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc = nullptr);

class IDataBatcher : public TThrRefBase {
public:
    virtual void AddData(const NMiniKQL::TUnboxedValueBatch& data) = 0;
    virtual i64 GetMemory() const = 0;
    virtual IDataBatchPtr Build() = 0;
};

using IDataBatcherPtr = TIntrusivePtr<IDataBatcher>;

IDataBatcherPtr CreateRowDataBatcher(
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    std::vector<ui32> writeIndex,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc = nullptr,
    std::vector<ui32> readIndex = {});

IDataBatcherPtr CreateColumnDataBatcher(
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    std::vector<ui32> writeIndex,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc = nullptr,
    std::vector<ui32> readIndex = {});

class IDataBatchProjection : public TThrRefBase {
public:
    virtual void AddRow(TConstArrayRef<TCell> row) = 0;
    virtual IDataBatchPtr Flush() = 0;
};

using IDataBatchProjectionPtr = TIntrusivePtr<IDataBatchProjection>;

IDataBatchProjectionPtr CreateDataBatchProjection(
    TConstArrayRef<ui32> indexes,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc);

std::vector<ui32> GetIndexes(
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> additionalInputColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> outputColumns,
    const bool preferAdditionalInputColumns);

bool IsEqual(
    TConstArrayRef<TCell> cells,
    const std::vector<ui32>& newIndexes,
    const std::vector<ui32>& oldIndexes,
    TConstArrayRef<NScheme::TTypeInfo> types);

std::vector<TConstArrayRef<TCell>> GetRows(
    const NKikimr::NKqp::IDataBatchPtr& batch);

std::vector<TConstArrayRef<TCell>> CutColumns(
    const std::vector<TConstArrayRef<TCell>>& rows, const ui32 columnsCount);

std::vector<ui32> BuildDefaultMap(
    const THashSet<TStringBuf>& defaultColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> lookupColumns);

ui32 CountLocalDefaults(
    const THashSet<TStringBuf>& defaultColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> lookupColumns);

class TUniqueSecondaryKeyCollector {
public:
    TUniqueSecondaryKeyCollector(
        const TConstArrayRef<NScheme::TTypeInfo> primaryKeyColumnTypes,
        const TConstArrayRef<NScheme::TTypeInfo> secondaryKeyColumnTypes,
        const TConstArrayRef<ui32> secondaryKeyColumns,
        const TConstArrayRef<ui32> secondaryTableKeyColumns,
        const TConstArrayRef<ui32> primaryKeyInSecondaryTableKeyColumns);

    bool AddRow(const TConstArrayRef<TCell> row);
    bool AddSecondaryTableRow(const TConstArrayRef<TCell> row);

    using TKeysSet = THashSet<TConstArrayRef<TCell>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals>;

    TKeysSet BuildUniqueSecondaryKeys();

private:
    bool AddRowImpl();

    const TConstArrayRef<NScheme::TTypeInfo> PrimaryKeyColumnTypes;
    const TConstArrayRef<NScheme::TTypeInfo> SecondaryKeyColumnTypes;
    const TConstArrayRef<ui32> SecondaryKeyColumns;
    const TConstArrayRef<ui32> SecondaryTableKeyColumns;
    const TConstArrayRef<ui32> PrimaryKeyInSecondaryTableKeyColumns;

    std::vector<std::vector<TCell>> Cells;
    TKeysSet UniqueCellsSet;
    THashMap<TConstArrayRef<TCell>, size_t, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> PrimaryToSecondary;
    THashMap<TConstArrayRef<TCell>, size_t, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> SecondaryToPrimary;
};

class IShardedWriteController : public TThrRefBase {
public:
    virtual void OnPartitioningChanged(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) = 0;
    virtual void OnPartitioningChanged(
        const std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>& partitioning) = 0;

    using TWriteToken = ui64;

    // Data ordering invariant:
    // For two writes A and B:
    // A happend before B <=> Close(A) happend before Open(B) otherwise Priority(A) < Priority(B).

    virtual void Open(
        const TWriteToken token,
        const TTableId TableId,
        const NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& keyColumns,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumns,
        const ui32 defaultColumnsCount,
        const i64 priority) = 0;
    virtual void Write(
        const TWriteToken token,
        IDataBatchPtr&& data) = 0;
    virtual void Close(TWriteToken token) = 0;

    virtual void CleanupClosedTokens() = 0;

    virtual void FlushBuffer(const TWriteToken token) = 0;
    virtual void FlushBuffers() = 0;

    // Set the QuerySpanId for a write token (for TLI lock-break attribution).
    virtual void SetTokenQuerySpanId(TWriteToken token, ui64 querySpanId) = 0;
    // Get the QuerySpanId of the first pending batch for a shard (0 if none).
    virtual ui64 GetFirstBatchQuerySpanId(ui64 shardId) const = 0;

    virtual void Close() = 0;

    virtual void AddCoveringMessages() = 0;

    struct TPendingShardInfo {
        ui64 ShardId;
        bool HasRead;
        // QuerySpanId of the batch that triggered this shard update (for TLI attribution).
        ui64 QuerySpanId = 0;
    };
    virtual void ForEachPendingShard(std::function<void(const TPendingShardInfo&)>&& callback) const = 0;
    virtual std::vector<TPendingShardInfo> ExtractShardUpdates() = 0;

    virtual ui64 GetShardsCount() const = 0;
    virtual TVector<ui64> GetShardsIds() const = 0;

    struct TMessageMetadata {
        ui64 Cookie = 0;
        ui64 OperationsCount = 0;
        bool IsFinal = false;
        ui64 SendAttempts = 0;
        ui64 NextOverloadSeqNo = 0;
    };
    virtual std::optional<TMessageMetadata> GetMessageMetadata(ui64 shardId) = 0;

    struct TSerializationResult {
        i64 TotalDataSize = 0;
        TVector<ui64> PayloadIndexes;
    };

    virtual TSerializationResult SerializeMessageToPayload(ui64 shardId, NKikimr::NEvents::TDataEvents::TEvWrite& evWrite) = 0;

    struct TMessageAcknowledgedResult {
        ui64 DataSize = 0;
        bool IsShardEmpty = 0;
    };

    virtual std::optional<TMessageAcknowledgedResult> OnMessageAcknowledged(ui64 shardId, ui64 cookie) = 0;
    virtual void OnMessageSent(ui64 shardId, ui64 cookie) = 0;

    virtual void ResetRetries(ui64 shardId, ui64 cookie) = 0;

    virtual i64 GetMemory() const = 0;

    virtual bool IsAllWritesClosed() const = 0;
    virtual bool IsAllWritesFinished() const = 0;

    virtual bool IsReady() const = 0;
    virtual bool IsEmpty() const = 0;
};

using IShardedWriteControllerPtr = TIntrusivePtr<IShardedWriteController>;


struct TShardedWriteControllerSettings {
    i64 MemoryLimitTotal = 0;
    bool Inconsistent = false;
};

IShardedWriteControllerPtr CreateShardedWriteController(
    const TShardedWriteControllerSettings& settings,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc);

}
}
