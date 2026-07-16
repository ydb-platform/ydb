#pragma once

#include "value_type.h"

#include <deque>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>

#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/types/binary_json/format.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Conversion between physical arrow storage and logical JsonValue/BinaryJson in-program representation
class IValueArrowCodec {
public:
    virtual ~IValueArrowCodec() = default;

    virtual EValueType GetValueType() const = 0;
    virtual std::shared_ptr<arrow::DataType> GetArrowType() const = 0;
    virtual bool CanBeDictionaryEncoded() const = 0;

    // Read path - wrap the physical element `index` as a logical value view (a native scalar for
    // Double/Bool/String, or a BinaryJson blob for BinaryJson).
    // The view aliases `array`, which must outlive it.
    virtual TJsonValueView ReadValueView(const arrow::Array& array, const i64 index) const = 0;
    // Approximate size of element `index` for accounting: variable for binary values, fixed for scalar types.
    virtual ui32 GetElementSize(const arrow::Array& array, const i64 index) const = 0;

    // Write path - initialize builder and write values to it.
    // reserveData makes sense only for variable-length types (BinaryJson and string).
    virtual std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 reserveData) const = 0;
    virtual void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const = 0;
};

std::shared_ptr<const IValueArrowCodec> GetCodecForValueType(const EValueType valueType);

// Element type to represent result of merging arrays with arg types
EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next);

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
