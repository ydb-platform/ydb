#include "kqp_transport.h"
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

Ydb::ResultSet TKqpProtoBuilder::BuildYdbResultSet(
    TVector<NYql::NDq::TDqSerializedBatch>&& data,
    NKikimr::NMiniKQL::TType* mkqlSrcRowType,
    const TVector<ui32>* columnOrder)
{
    YQL_ENSURE(mkqlSrcRowType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct);
    const auto* mkqlSrcRowStructType = static_cast<const TStructType*>(mkqlSrcRowType);

    Ydb::ResultSet resultSet;

    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        auto* column = resultSet.add_columns();
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];
        column->set_name(TString(mkqlSrcRowStructType->GetMemberName(memberIndex)));
        ExportTypeToProto(mkqlSrcRowStructType->GetMemberType(memberIndex), *column->mutable_type());
    }

    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    if (!data.empty()) {
        transportVersion = static_cast<NDqProto::EDataTransportVersion>(data.front().Proto.GetTransportVersion());
    }
    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion);
    for (auto& part : data) {
        if (part.RowCount()) {
            TUnboxedValueBatch rows(mkqlSrcRowType);
            dataSerializer.Deserialize(std::move(part), mkqlSrcRowType, rows);
            rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                ExportValueToProto(mkqlSrcRowType, value, *resultSet.add_rows(), columnOrder);
            });
        }
    }

    return resultSet;
}

} // namespace NKqp
} // namespace NKikimr
