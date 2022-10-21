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

NKikimrMiniKQL::TType TKqpProtoBuilder::ApplyColumnHints(const NKikimrMiniKQL::TType& srcRowType,
    const TVector<TString>& columnHints)
{
    YQL_ENSURE(srcRowType.GetKind() == NKikimrMiniKQL::Struct);
    YQL_ENSURE(srcRowType.GetStruct().MemberSize() == columnHints.size(),
        "" << srcRowType.GetStruct().MemberSize() << " != " << columnHints.size());

    TMap<TString, size_t> memberIndices;
    for (size_t i = 0; i < srcRowType.GetStruct().MemberSize(); ++i) {
        memberIndices[srcRowType.GetStruct().GetMember(i).GetName()] = i;
    }

    NKikimrMiniKQL::TType dstRowType;
    dstRowType.SetKind(NKikimrMiniKQL::Struct);

    for (auto& columnName : columnHints) {
        auto* memberIndex = memberIndices.FindPtr(columnName);
        YQL_ENSURE(memberIndex);

        auto* newMember = dstRowType.MutableStruct()->AddMember();
        newMember->SetName(columnName);
        newMember->MutableType()->CopyFrom(srcRowType.GetStruct().GetMember(*memberIndex).GetType());
    }

    return dstRowType;
}

void TKqpProtoBuilder::BuildValue(const TVector<NDqProto::TData>& data, const NKikimrMiniKQL::TType& valueType,
    NKikimrMiniKQL::TResult* result)
{
    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    result->MutableType()->CopyFrom(valueType);

    auto mkqlType = ImportTypeFromProto(result->GetType(), *TypeEnv);

    TUnboxedValueVector buffer;
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
        dataSerializer.Deserialize(part, mkqlType, buffer);
    }
    YQL_ENSURE(buffer.size() == 1, "Actual buffer size: " << data.size());

    ExportValueToProto(mkqlType, buffer[0], *result->MutableValue());
}

void TKqpProtoBuilder::BuildValue(TUnboxedValueVector& rows, const NKikimrMiniKQL::TType& valueType,
    NKikimrMiniKQL::TResult* result)
{
    YQL_ENSURE(rows.size() == 1, "Actual buffer size: " << rows.size());

    result->MutableType()->CopyFrom(valueType);
    auto mkqlType = ImportTypeFromProto(result->GetType(), *TypeEnv);
    ExportValueToProto(mkqlType, rows[0], *result->MutableValue());
}

void TKqpProtoBuilder::BuildStream(const TVector<NDqProto::TData>& data, const NKikimrMiniKQL::TType& srcRowType,
    const NKikimrMiniKQL::TType* dstRowType, NKikimrMiniKQL::TResult* result)
{
    YQL_ENSURE(srcRowType.GetKind() == NKikimrMiniKQL::Struct);

    auto* mkqlSrcRowType = NMiniKQL::ImportTypeFromProto(srcRowType, *TypeEnv);
    auto* mkqlSrcRowStructType = static_cast<TStructType*>(mkqlSrcRowType);

    result->MutableType()->SetKind(NKikimrMiniKQL::List);
    auto* newRowType = result->MutableType()->MutableList()->MutableItem();

    TListType* mkqlSrcRowsType = nullptr;
    TMap<TStringBuf, ui32> memberIndices;

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

    if (dstRowType) {
        YQL_ENSURE(dstRowType->GetKind() == NKikimrMiniKQL::Struct);
        newRowType->CopyFrom(*dstRowType);

        for (ui32 index = 0; index < srcRowType.GetStruct().MemberSize(); ++index) {
            memberIndices[srcRowType.GetStruct().GetMember(index).GetName()] = index;
        }
    } else {
        newRowType->CopyFrom(srcRowType);
        mkqlSrcRowsType = TListType::Create(mkqlSrcRowType, *TypeEnv);
    }

    for (auto& part : data) {
        if (part.GetRows() == 0) {
            continue;
        }

        TUnboxedValueVector rows;
        dataSerializer.Deserialize(part, mkqlSrcRowType, rows);

        if (dstRowType) {
            for (auto& srcRow : rows) {
                auto* dstRow = result->MutableValue()->MutableList()->Add()->MutableStruct();
                for (auto& dstMember : dstRowType->GetStruct().GetMember()) {
                    ui32* srcMemberIndex = memberIndices.FindPtr(dstMember.GetName());
                    YQL_ENSURE(srcMemberIndex);

                    auto* memberType = mkqlSrcRowStructType->GetMemberType(*srcMemberIndex);
                    ExportValueToProto(memberType, srcRow.GetElement(*srcMemberIndex), *dstRow->Add());
                }
            }
        } else {
            ExportValueToProto(mkqlSrcRowsType, HolderFactory->VectorAsArray(rows), *result->MutableValue());
        }
    }
}

void TKqpProtoBuilder::BuildStream(TUnboxedValueVector& rows, const NKikimrMiniKQL::TType& srcRowType,
    const NKikimrMiniKQL::TType* dstRowType, NKikimrMiniKQL::TResult* result)
{
    YQL_ENSURE(srcRowType.GetKind() == NKikimrMiniKQL::Struct);

    auto* mkqlSrcRowType = NMiniKQL::ImportTypeFromProto(srcRowType, *TypeEnv);
    auto* mkqlSrcRowStructType = static_cast<TStructType*>(mkqlSrcRowType);

    result->MutableType()->SetKind(NKikimrMiniKQL::List);
    auto* newRowType = result->MutableType()->MutableList()->MutableItem();

    if (dstRowType) {
        YQL_ENSURE(dstRowType->GetKind() == NKikimrMiniKQL::Struct);
        newRowType->CopyFrom(*dstRowType);

        TMap<TStringBuf, ui32> memberIndices;
        for (ui32 index = 0; index < srcRowType.GetStruct().MemberSize(); ++index) {
            memberIndices[srcRowType.GetStruct().GetMember(index).GetName()] = index;
        }

        for (auto& srcRow : rows) {
            auto* dstRow = result->MutableValue()->MutableList()->Add()->MutableStruct();
            for (auto& dstMember : dstRowType->GetStruct().GetMember()) {
                ui32* srcMemberIndex = memberIndices.FindPtr(dstMember.GetName());
                YQL_ENSURE(srcMemberIndex);

                auto* memberType = mkqlSrcRowStructType->GetMemberType(*srcMemberIndex);
                ExportValueToProto(memberType, srcRow.GetElement(*srcMemberIndex), *dstRow->Add());
            }
        }
    } else {
        newRowType->CopyFrom(srcRowType);
        auto mkqlSrcRowsType = TListType::Create(mkqlSrcRowType, *TypeEnv);

        THolder<TGuard<TScopedAlloc>> guard;
        if (SelfHosted) {
            guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
        }

        ExportValueToProto(mkqlSrcRowsType, HolderFactory->VectorAsArray(rows), *result->MutableValue());
    }
}

Ydb::ResultSet TKqpProtoBuilder::BuildYdbResultSet(const TVector<NDqProto::TData>& data,
    const NKikimrMiniKQL::TType& srcRowType, const NKikimrMiniKQL::TType* dstRowType)
{
    YQL_ENSURE(srcRowType.GetKind() == NKikimrMiniKQL::Struct);

    auto* mkqlSrcRowType = NMiniKQL::ImportTypeFromProto(srcRowType, *TypeEnv);
    auto* mkqlSrcRowStructType = static_cast<TStructType*>(mkqlSrcRowType);

    Ydb::ResultSet resultSet;

    TMap<TStringBuf, ui32> memberIndices;

    if (dstRowType) {
        YQL_ENSURE(dstRowType->GetKind() == NKikimrMiniKQL::Struct);

        for (ui32 index = 0; index < mkqlSrcRowStructType->GetMembersCount(); ++index) {
            memberIndices[mkqlSrcRowStructType->GetMemberName(index)] = index;
        }

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

            if (dstRowType) {
                for (auto& srcRow : rows) {
                    auto* dstRow = resultSet.add_rows();
                    dstRow->mutable_items()->Reserve(dstRowType->GetStruct().MemberSize());

                    for (auto& dstMember : dstRowType->GetStruct().GetMember()) {
                        ui32* srcMemberIndex = memberIndices.FindPtr(dstMember.GetName());
                        YQL_ENSURE(srcMemberIndex);

                        auto* memberType = mkqlSrcRowStructType->GetMemberType(*srcMemberIndex);
                        ExportValueToProto(memberType, srcRow.GetElement(*srcMemberIndex), *dstRow->mutable_items()->Add());
                    }
                }
            } else {
                for (auto& row : rows) {
                    ExportValueToProto(mkqlSrcRowType, row, *resultSet.add_rows());
                }
            }
        }
    }

    return resultSet;
}

} // namespace NKqp
} // namespace NKikimr
