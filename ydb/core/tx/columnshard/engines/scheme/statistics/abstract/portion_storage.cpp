#include "portion_storage.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/engines/scheme/statistics/protos/data.pb.h>

namespace NKikimr::NOlap::NStatistics {

NKikimrColumnShardStatisticsProto::TScalar TPortionStorage::ScalarToProto(const arrow::Scalar& scalar) {
    NKikimrColumnShardStatisticsProto::TScalar result;
    switch (scalar.type->id()) {
        case arrow::Type::BOOL:
            result.SetBool(static_cast<const arrow::BooleanScalar&>(scalar).value);
            break;
        case arrow::Type::UINT8:
            result.SetUint8(static_cast<const arrow::UInt8Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT16:
            result.SetUint16(static_cast<const arrow::UInt16Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT32:
            result.SetUint32(static_cast<const arrow::UInt32Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT64:
            result.SetUint64(static_cast<const arrow::UInt64Scalar&>(scalar).value);
            break;
        case arrow::Type::INT8:
            result.SetInt8(static_cast<const arrow::Int8Scalar&>(scalar).value);
            break;
        case arrow::Type::INT16:
            result.SetInt16(static_cast<const arrow::Int16Scalar&>(scalar).value);
            break;
        case arrow::Type::INT32:
            result.SetInt32(static_cast<const arrow::Int32Scalar&>(scalar).value);
            break;
        case arrow::Type::INT64:
            result.SetInt64(static_cast<const arrow::Int64Scalar&>(scalar).value);
            break;
        case arrow::Type::DOUBLE:
            result.SetDouble(static_cast<const arrow::DoubleScalar&>(scalar).value);
            break;
        case arrow::Type::FLOAT:
            result.SetFloat(static_cast<const arrow::FloatScalar&>(scalar).value);
            break;
        case arrow::Type::TIMESTAMP:
        {
            auto* ts = result.MutableTimestamp();
            ts->SetValue(static_cast<const arrow::TimestampScalar&>(scalar).value);
            ts->SetUnit(static_cast<const arrow::TimestampType&>(*scalar.type).unit());
            break;
        }
        default:
            AFL_VERIFY(false)("problem", "incorrect type for statistics usage")("type", scalar.type->ToString());
    }
    return result;
}

std::shared_ptr<arrow::Scalar> TPortionStorage::ProtoToScalar(const NKikimrColumnShardStatisticsProto::TScalar& proto) {
    if (proto.HasBool()) {
        return std::make_shared<arrow::BooleanScalar>(proto.GetBool());
    } else if (proto.HasUint8()) {
        return std::make_shared<arrow::UInt8Scalar>(proto.GetUint8());
    } else if (proto.HasUint16()) {
        return std::make_shared<arrow::UInt16Scalar>(proto.GetUint16());
    } else if (proto.HasUint32()) {
        return std::make_shared<arrow::UInt32Scalar>(proto.GetUint32());
    } else if (proto.HasUint64()) {
        return std::make_shared<arrow::UInt64Scalar>(proto.GetUint64());
    } else if (proto.HasInt8()) {
        return std::make_shared<arrow::Int8Scalar>(proto.GetInt8());
    } else if (proto.HasInt16()) {
        return std::make_shared<arrow::Int16Scalar>(proto.GetInt16());
    } else if (proto.HasInt32()) {
        return std::make_shared<arrow::Int32Scalar>(proto.GetInt32());
    } else if (proto.HasInt64()) {
        return std::make_shared<arrow::Int64Scalar>(proto.GetInt64());
    } else if (proto.HasDouble()) {
        return std::make_shared<arrow::DoubleScalar>(proto.GetDouble());
    } else if (proto.HasFloat()) {
        return std::make_shared<arrow::FloatScalar>(proto.GetFloat());
    } else if (proto.HasTimestamp()) {
        arrow::TimeUnit::type unit = arrow::TimeUnit::type(proto.GetTimestamp().GetUnit());
        return std::make_shared<arrow::TimestampScalar>(proto.GetTimestamp().GetValue(), std::make_shared<arrow::TimestampType>(unit));
    }
    AFL_VERIFY(false)("problem", "incorrect statistics proto")("proto", proto.DebugString());
    return nullptr;
}

std::shared_ptr<arrow::Scalar> TPortionStorage::GetScalarVerified(const TPortionStorageCursor& cursor) const {
    AFL_VERIFY(cursor.GetScalarsPosition() < Data.size());
    AFL_VERIFY(Data[cursor.GetScalarsPosition()]);
    return Data[cursor.GetScalarsPosition()];
}

void TPortionStorage::AddScalar(const std::shared_ptr<arrow::Scalar>& scalar) {
    const auto type = scalar->type->id();
    AFL_VERIFY(type == arrow::Type::BOOL ||
        type == arrow::Type::UINT8 || type == arrow::Type::UINT16 || type == arrow::Type::UINT32 || type == arrow::Type::UINT64 ||
        type == arrow::Type::INT8 || type == arrow::Type::INT16 || type == arrow::Type::INT32 || type == arrow::Type::INT64 ||
        type == arrow::Type::DOUBLE || type == arrow::Type::TIMESTAMP || type == arrow::Type::FLOAT)
        ("problem", "incorrect_stat_type")("incoming", scalar->type->ToString());
    Data.emplace_back(scalar);
}

NKikimrColumnShardStatisticsProto::TPortionStorage TPortionStorage::SerializeToProto() const {
    NKikimrColumnShardStatisticsProto::TPortionStorage result;
    for (auto&& i : Data) {
        AFL_VERIFY(i);
        *result.AddScalars() = ScalarToProto(*i);
    }
    return result;
}

NKikimr::TConclusionStatus TPortionStorage::DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TPortionStorage& proto) {
    for (auto&& i : proto.GetScalars()) {
        Data.emplace_back(ProtoToScalar(i));
    }
    return TConclusionStatus::Success();
}

}