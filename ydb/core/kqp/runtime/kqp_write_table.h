#pragma once

#include <util/generic/ptr.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NKqp {

class IShardedWriteController : public TThrRefBase {
public:
    virtual void OnPartitioningChanged(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) = 0;
    virtual void OnPartitioningChanged(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        NSchemeCache::TSchemeCacheRequest::TEntry&& partitionsEntry) = 0;

    virtual void AddData(NMiniKQL::TUnboxedValueBatch&& data) = 0;
    virtual void Close() = 0;

    virtual TVector<ui64> GetPendingShards() const = 0;

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
    virtual NKikimrDataEvents::EDataFormat GetDataFormat() = 0;
    virtual std::vector<ui32> GetWriteColumnIds() = 0;

    virtual std::optional<i64> OnMessageAcknowledged(ui64 shardId, ui64 cookie) = 0;
    virtual void OnMessageSent(ui64 shardId, ui64 cookie) = 0;

    virtual void ResetRetries(ui64 shardId, ui64 cookie) = 0;

    virtual i64 GetMemory() const = 0;

    virtual bool IsClosed() const = 0;
    virtual bool IsFinished() const = 0;

    virtual bool IsReady() const = 0;
};

using IShardedWriteControllerPtr = TIntrusivePtr<IShardedWriteController>;


struct TShardedWriteControllerSettings {
    i64 MemoryLimitTotal;
    i64 MemoryLimitPerMessage;
    i64 MaxBatchesPerMessage;
};

IShardedWriteControllerPtr CreateShardedWriteController(
    const TShardedWriteControllerSettings& settings,
    TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumns,
    const NMiniKQL::TTypeEnvironment& typeEnv,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc);

}
}
