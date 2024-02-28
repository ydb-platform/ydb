#pragma once

#include <util/generic/ptr.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NKqp {

// TODO: batching policy

class IPayloadSerializer : public TThrRefBase {
public:
    virtual void AddData(NMiniKQL::TUnboxedValueBatch&& data) = 0;

    virtual NKikimrDataEvents::EDataFormat GetDataFormat() = 0;
    virtual std::vector<ui32> GetWriteColumnIds() = 0;

    virtual std::optional<TStringBuf> GetBatch(const ui64 shard) = 0;
    virtual void NextBatch(const ui64 shard) = 0;

    virtual i64 GetMemoryInFlight() = 0;
};

using IPayloadSerializerPtr = TIntrusivePtr<IPayloadSerializer>;

IPayloadSerializerPtr CreateColumnShardPayloadSerializer(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
    const NMiniKQL::TTypeEnvironment& typeEnv);

//IPayloadSerializerPtr CreateDataShardPayloadSerializer();

}
}
