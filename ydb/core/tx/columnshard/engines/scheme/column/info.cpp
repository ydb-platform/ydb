#include "info.h"

#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION

namespace NKikimr::NOlap {

TConclusionStatus TSimpleColumnInfo::DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo) {
    AFL_VERIFY(columnInfo.GetId() == ColumnId);
    if (columnInfo.HasSerializer()) {
        AFL_VERIFY(Serializer.DeserializeFromProto(columnInfo.GetSerializer()));
    } else if (columnInfo.HasCompression()) {
        Serializer.DeserializeFromProto(columnInfo.GetCompression()).Validate();
    }
    if (columnInfo.HasDefaultValue()) {
        DefaultValue.DeserializeFromProto(columnInfo.GetDefaultValue()).Validate();
    }
    if (columnInfo.HasDataAccessorConstructor()) {
        AFL_VERIFY(DataAccessorConstructor.DeserializeFromProto(columnInfo.GetDataAccessorConstructor()));
    }
    IsNullable = columnInfo.HasNotNull() ? !columnInfo.GetNotNull() : true;
    AFL_VERIFY(Serializer);
    Loader = std::make_shared<TColumnLoader>(Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
    return TConclusionStatus::Success();
}

TSimpleColumnInfo::TSimpleColumnInfo(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField,
    const NArrow::NSerialization::TSerializerContainer& serializer, const bool needMinMax, const bool isSorted, const bool isNullable,
    const std::shared_ptr<arrow::Scalar>& defaultValue, const std::optional<ui32>& pkColumnIndex, const NScheme::TTypeInfo& typeInfo)
    : ColumnId(columnId)
    , PKColumnIndex(pkColumnIndex)
    , ArrowField(arrowField)
    , TypeInfo(typeInfo)
    , Serializer(serializer)
    , NeedMinMax(needMinMax)
    , IsSorted(isSorted)
    , IsNullable(isNullable)
    , DefaultValue(defaultValue)
{
    ColumnName = ArrowField->name();
    Loader = std::make_shared<TColumnLoader>(Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
}

}   // namespace NKikimr::NOlap
