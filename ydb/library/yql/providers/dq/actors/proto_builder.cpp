#include "proto_builder.h"

#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/utils/log/log.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/writer.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

namespace {
TVector<ui32> BuildColumnOrder(const TColumnOrder& order, NKikimr::NMiniKQL::TType* resultType, std::function<void(const TString&, TType*)> cb) {
    MKQL_ENSURE(resultType, "Incorrect result type");
    if (resultType->GetKind() != TType::EKind::Struct || order.Size() == 0) {
        return {};
    }

    TVector<ui32> columnOrder;
    auto structType = AS_TYPE(TStructType, resultType);
    columnOrder.resize(order.Size());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        const ui32 memberIndex = structType->GetMemberIndex(order.at(i).PhysicalName);
        const auto columnName = order.at(i).LogicalName;
        cb(columnName, structType->GetMemberType(memberIndex));
        columnOrder[i] = memberIndex;
    }
    return columnOrder;
}

} // unnamed

TProtoBuilder::TProtoBuilder(const TString& type, const TVector<TString>& columns)
    : Alloc(__LOCATION__)
    , TypeEnv(Alloc)
    , ResultType(static_cast<TType*>(DeserializeNode(type, TypeEnv)))
    , ColumnOrder(columns)
{
    Alloc.Release();
}

TProtoBuilder::~TProtoBuilder() {
    Alloc.Acquire();
}

bool TProtoBuilder::CanBuildResultSet() const {
    return ResultType->GetKind() == TType::EKind::Struct;
}

TString TProtoBuilder::BuildYson(TVector<NYql::NDq::TDqSerializedBatch>&& rows, ui64 maxBytesLimit, ui64 maxRowsLimit, bool* truncated) {
    if (truncated) {
        *truncated = false;
    }
    
    TThrowingBindTerminator t;
    ui64 size = 0;
    ui64 count = 0;
    TStringStream out;
    NYson::TYsonWriter writer((IOutputStream*)&out);
    writer.OnBeginList();
    auto columnOrder = BuildColumnOrder(ColumnOrder, ResultType, [](auto&, auto) {});

    auto full = WriteData(std::move(rows), [&](const NYql::NUdf::TUnboxedValuePod& value) {
        bool ret = (size <= maxBytesLimit && count <= maxRowsLimit);
        if (ret) {
            auto rowYson = NCommon::WriteYsonValue(value, ResultType, columnOrder.empty() ? nullptr : &columnOrder);
            size += rowYson.size();
            ++count;
            ret = (size <= maxBytesLimit && count <= maxRowsLimit);
            if (ret) {
                writer.OnListItem();
                writer.OnRaw(rowYson);
            }
        }

        return ret;
    });

    if (!full) {
        if (!truncated) {
            ythrow yexception() << "Too big yson result size: " << size << " > " << maxBytesLimit;
        } else {
            *truncated = true;
        }
    }

    writer.OnEndList();
    return out.Str();
}

bool TProtoBuilder::WriteData(NYql::NDq::TDqSerializedBatch&& data, const std::function<bool(const NYql::NUdf::TUnboxedValuePod& value)>& func) {
    TThrowingBindTerminator t;
    TGuard<TScopedAlloc> allocGuard(Alloc);

    TMemoryUsageInfo memInfo("ProtoBuilder");
    THolderFactory holderFactory(Alloc.Ref(), memInfo);
    const auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;

    NDq::TDqDataSerializer dataDeserializer(TypeEnv, holderFactory, transportVersion, NDq::FromProto(data.Proto.GetValuePackerVersion()));

    YQL_ENSURE(!ResultType->IsMulti());
    TUnboxedValueBatch buffer(ResultType);
    dataDeserializer.Deserialize(std::move(data), ResultType, buffer);

    return buffer.ForEachRow([&func](const auto& value) {
        return func(value);
    });
}

bool TProtoBuilder::WriteData(TVector<NYql::NDq::TDqSerializedBatch>&& rows, const std::function<bool(const NYql::NUdf::TUnboxedValuePod& value)>& func) {
    TThrowingBindTerminator t;
    TGuard<TScopedAlloc> allocGuard(Alloc);

    TMemoryUsageInfo memInfo("ProtoBuilder");
    THolderFactory holderFactory(Alloc.Ref(), memInfo);
    const auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    YQL_ENSURE(!ResultType->IsMulti());

    for (auto& part : rows) {
        TUnboxedValueBatch buffer(ResultType);
        NDq::TDqDataSerializer dataDeserializer(TypeEnv, holderFactory, transportVersion, NDq::FromProto(part.Proto.GetValuePackerVersion()));
        dataDeserializer.Deserialize(std::move(part), ResultType, buffer);
        if (!buffer.ForEachRow([&func](const auto& value) { return func(value); })) {
            return false;
        }
    }
    return true;
}

Ydb::ResultSet TProtoBuilder::BuildResultSet(TVector<NYql::NDq::TDqSerializedBatch>&& data) {
    Ydb::ResultSet resultSet;
    auto structType = AS_TYPE(TStructType, ResultType);
    MKQL_ENSURE(structType, "Result is not a struct");
    auto columnOrder = BuildColumnOrder(ColumnOrder, ResultType, [&](auto& name, auto type) {
        auto& column = *resultSet.add_columns();
        column.set_name(name);
        ExportTypeToProto(type, *column.mutable_type());

    });

    WriteData(std::move(data), [&](const NYql::NUdf::TUnboxedValuePod& value) {
        ExportValueToProto(ResultType, value, *resultSet.add_rows(), &columnOrder);
        return true;
    });

    return resultSet;
}

TString TProtoBuilder::GetSerializedType() const {
    auto result = SerializeNode(ResultType, TypeEnv);
    return result;
}

TString TProtoBuilder::AllocDebugInfo() {
    TGuard<TScopedAlloc> allocGuard(Alloc);
    return TStringBuilder{} << "Used:           " << Alloc.GetUsed() << '\n'
                            << "Peak used:      " << Alloc.GetPeakUsed() << '\n'
                            << "Allocated:      " << Alloc.GetAllocated() << '\n'
                            << "Peak allocated: " << Alloc.GetPeakAllocated() << '\n'
                            << "Limit:          " << Alloc.GetLimit();
}

} // NYql::NDqs
