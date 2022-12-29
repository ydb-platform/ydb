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

Ydb::ResultSet TKqpProtoBuilder::BuildYdbResultSet(const TVector<NDqProto::TData>& data,
    const NKikimrMiniKQL::TType& srcRowType, const NKikimrMiniKQL::TType* dstRowType)
{
    YQL_ENSURE(srcRowType.GetKind() == NKikimrMiniKQL::Struct);

    auto* mkqlSrcRowType = NMiniKQL::ImportTypeFromProto(srcRowType, *TypeEnv);
    auto* mkqlSrcRowStructType = static_cast<TStructType*>(mkqlSrcRowType);

    Ydb::ResultSet resultSet;

    if (dstRowType) {
        YQL_ENSURE(dstRowType->GetKind() == NKikimrMiniKQL::Struct);

        for (auto& member : dstRowType->GetStruct().GetMember()) {
            auto* column = resultSet.add_columns();
            column->set_name(member.GetName());
            ConvertMiniKQLTypeToYdbType(member.GetType(), *column->mutable_type());
        }
    } else {
        for (auto& member : srcRowType.GetStruct().GetMember()) {
            auto* column = resultSet.add_columns();
            column->set_name(member.GetName());
            ConvertMiniKQLTypeToYdbType(member.GetType(), *column->mutable_type());
        }
    }

    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    if (!data.empty()) {
        switch (data.front().GetTransportVersion()) {
            case 10000: {
                transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_YSON_1_0;
                break;
            }
            case 20000: {
                transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0;
                break;
            }
            case 30000: {
                transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_ARROW_1_0;
                break;
            }
            default:
                transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
        }
    }
    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion);

    for (auto& part : data) {
        if (part.GetRows()) {
            TUnboxedValueVector rows;
            dataSerializer.Deserialize(part, mkqlSrcRowType, rows);

            TVector<ui32> columnOrder;
            if (dstRowType) {
                for(auto& dstMember: dstRowType->GetStruct().GetMember()) {
                    columnOrder.push_back(mkqlSrcRowStructType->GetMemberIndex(dstMember.GetName()));
                }
            }

            for (auto& row : rows) {
                ExportValueToProto(mkqlSrcRowType, row, *resultSet.add_rows(), &columnOrder);
            }
        }
    }

    return resultSet;
}

} // namespace NKqp
} // namespace NKikimr
