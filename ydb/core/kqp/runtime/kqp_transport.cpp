#include "kqp_transport.h"
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/result_set_format/kqp_result_set_builders.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NKikimr {
namespace NKqp {

using namespace NMiniKQL;
using namespace NYql;

TKqpProtoBuilder::TSelfHosted::TSelfHosted(const IFunctionRegistry& funcRegistry)
    : Alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
    , TypeEnv(Alloc)
    , MemInfo("KqpProtoBuilder")
    , HolderFactory(Alloc.Ref(), MemInfo)
{
}

TKqpProtoBuilder::TKqpProtoBuilder(const IFunctionRegistry& funcRegistry)
    : SelfHosted(MakeHolder<TSelfHosted>(funcRegistry))
{
    Alloc = &SelfHosted->Alloc;
    TypeEnv = &SelfHosted->TypeEnv;
    HolderFactory = &SelfHosted->HolderFactory;

    Alloc->Release();
}

TKqpProtoBuilder::TKqpProtoBuilder(TScopedAlloc* alloc, TTypeEnvironment* typeEnv, THolderFactory* holderFactory)
    : Alloc(alloc)
    , TypeEnv(typeEnv)
    , HolderFactory(holderFactory)
{
}

TKqpProtoBuilder::~TKqpProtoBuilder() {
    if (SelfHosted) {
        SelfHosted->Alloc.Acquire();
    }
}

void TKqpProtoBuilder::BuildYdbResultSet(
    Ydb::ResultSet& resultSet,
    TVector<NYql::NDq::TDqSerializedBatch>&& data,
    NKikimr::NMiniKQL::TType* mkqlSrcRowType,
    const NFormats::TFormatsSettings& formatsSettings,
    bool fillSchema,
    const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    auto valuePackerVersion = NMiniKQL::EValuePackerVersion::V0;
    if (!data.empty()) {
        transportVersion = static_cast<NDqProto::EDataTransportVersion>(data.front().Proto.GetTransportVersion());
        valuePackerVersion = NDq::FromProto(data.front().Proto.GetValuePackerVersion());
    }

    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion, valuePackerVersion);
    NFormats::BuildResultSetFromBatches(&resultSet, formatsSettings, fillSchema, mkqlSrcRowType,
        dataSerializer, std::move(data), columnOrder, columnHints);
}

} // namespace NKqp
} // namespace NKikimr
