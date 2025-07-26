#pragma once

#include <util/generic/ptr.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/protos/kqp.pb.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NKqp {

class IDataBatch : public TThrRefBase {
public:
    virtual TString SerializeToString() const = 0;
    virtual i64 GetSerializedMemory() const = 0;
    virtual i64 GetMemory() const = 0;
    virtual bool IsEmpty() const = 0;

    virtual std::shared_ptr<void> ExtractBatch() = 0;

    virtual void AttachAlloc(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) = 0;
    virtual void DetachAlloc() = 0;
    virtual bool AttachedAlloc() const = 0;
};

using IDataBatchPtr = TIntrusivePtr<IDataBatch>;

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
    virtual IDataBatchPtr Project(const IDataBatchPtr& data) const = 0;
};

using IDataBatchProjectionPtr = TIntrusivePtr<IDataBatchProjection>;

IDataBatchProjectionPtr CreateDataBatchProjection(
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    const TConstArrayRef<ui32> inputWriteIndex,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> outputColumns,
    const TConstArrayRef<ui32> outputWriteIndex,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc);

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
        std::vector<ui32>&& writeIndexes,
        const i64 priority) = 0;
    virtual void Write(TWriteToken token, IDataBatchPtr&& data) = 0;
    virtual void Close(TWriteToken token) = 0;

    virtual void CleanupClosedTokens() = 0;

    virtual void FlushBuffers() = 0;

    virtual void Close() = 0;

    virtual void AddCoveringMessages() = 0;

    struct TPendingShardInfo {
        ui64 ShardId;
        bool HasRead;
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
