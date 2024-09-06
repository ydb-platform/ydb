#include "proto_builder.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/writer.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

namespace {

TVector<ui32> BuildColumnOrder(const TVector<TString>& columns, NKikimr::NMiniKQL::TType* resultType) {
    MKQL_ENSURE(resultType, "Incorrect result type");
    if (resultType->GetKind() != TType::EKind::Struct || columns.empty()) {
        return {};
    }
    TColumnOrder order(columns);

    TVector<ui32> columnOrder;
    THashMap<TString, ui32> column2id;
    auto structType = AS_TYPE(TStructType, resultType);
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        const auto columnName = TString(structType->GetMemberName(i));
        column2id[columnName] = i;
    }
    columnOrder.resize(columns.size());

    int id = 0;
    for (const auto& [columnName, generated] : order) {
        columnOrder[id++] = column2id[generated];
    }
    return columnOrder;
}

} // unnamed

TProtoBuilder::TProtoBuilder(const TString& type, const TVector<TString>& columns)
    : Alloc(__LOCATION__)
    , TypeEnv(Alloc)
    , ResultType(static_cast<TType*>(DeserializeNode(type, TypeEnv)))
    , ColumnOrder(BuildColumnOrder(columns, ResultType))
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

    auto full = WriteData(std::move(rows), [&](const NYql::NUdf::TUnboxedValuePod& value) {
        bool ret = (size <= maxBytesLimit && count <= maxRowsLimit);
        if (ret) {
            auto rowYson = NCommon::WriteYsonValue(value, ResultType, ColumnOrder.empty() ? nullptr : &ColumnOrder);
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

bool TProtoBuilder::WriteYsonData(NYql::NDq::TDqSerializedBatch&& data, const std::function<bool(const TString& rawYson)>& func) {
    TThrowingBindTerminator t;
    return WriteData(std::move(data), [&](const NYql::NUdf::TUnboxedValuePod& value) {
        auto rowYson = NCommon::WriteYsonValue(value, ResultType, ColumnOrder.empty() ? nullptr : &ColumnOrder);
        return func(rowYson);
    });
}

bool TProtoBuilder::WriteData(NYql::NDq::TDqSerializedBatch&& data, const std::function<bool(const NYql::NUdf::TUnboxedValuePod& value)>& func) {
    TThrowingBindTerminator t;
    TGuard<TScopedAlloc> allocGuard(Alloc);

    TMemoryUsageInfo memInfo("ProtoBuilder");
    THolderFactory holderFactory(Alloc.Ref(), memInfo);
    const auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    NDq::TDqDataSerializer dataSerializer(TypeEnv, holderFactory, transportVersion);

    YQL_ENSURE(!ResultType->IsMulti());
    TUnboxedValueBatch buffer(ResultType);
    dataSerializer.Deserialize(std::move(data), ResultType, buffer);

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
    NDq::TDqDataSerializer dataSerializer(TypeEnv, holderFactory, transportVersion);

    YQL_ENSURE(!ResultType->IsMulti());

    for (auto& part : rows) {
        TUnboxedValueBatch buffer(ResultType);
        dataSerializer.Deserialize(std::move(part), ResultType, buffer);
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
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        auto& column = *resultSet.add_columns();
        const ui32 memberIndex = ColumnOrder.empty() ? i : ColumnOrder[i];
        column.set_name(TString(structType->GetMemberName(memberIndex)));
        ExportTypeToProto(structType->GetMemberType(memberIndex), *column.mutable_type());
    }

    WriteData(std::move(data), [&](const NYql::NUdf::TUnboxedValuePod& value) {
        ExportValueToProto(ResultType, value, *resultSet.add_rows(), &ColumnOrder);
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
