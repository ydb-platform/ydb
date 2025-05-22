#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/defaults/protos/data.pb.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TColumnDefaultScalarValue {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, Value);
public:
    TColumnDefaultScalarValue() = default;

    TColumnDefaultScalarValue(const std::shared_ptr<arrow::Scalar>& val)
        : Value(val)
    {

    }

    TString DebugString() const;

    bool IsCompatibleType(const std::shared_ptr<arrow::DataType>& type) const {
        if (!Value) {
            return true;
        }
        return type->Equals(Value->type);
    }

    bool IsEmpty() const {
        return !Value;
    }

    NKikimrColumnShardColumnDefaults::TColumnDefault SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardColumnDefaults::TColumnDefault& proto);
    TConclusionStatus ParseFromString(const TString& value, const NScheme::TTypeInfo& type);
};

}