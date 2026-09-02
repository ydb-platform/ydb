#pragma once

#include "value_type.h"

#include <deque>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>

#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/types/binary_json/format.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

std::shared_ptr<arrow::DataType> GetArrowTypeForValueType(const EValueType valueType);

bool CanBeDictionaryEncoded(EValueType valueType);

// Read physical element `index` per the column's value type: a native scalar for Double/Bool/String, or a
// BinaryJson blob for BinaryJson. The view aliases `array`, which must outlive it.
TJsonValueView ArrayElementToJsonValueView(const arrow::Array& array, const i64 index, const EValueType valueType);
NBinaryJson::TBinaryJson ArrayElementToBinaryJson(const arrow::Array& array, const i64 index, const EValueType valueType);
ui32 ArrayElementSize(const arrow::Array& array, const i64 index, const EValueType valueType);

// Make an arrow builder for the value type's storage; reserveData applies only to variable-length types.
std::unique_ptr<arrow::ArrayBuilder> MakeBuilderForValueType(const EValueType valueType, const ui32 reserveItems, const ui32 reserveData);

// Decode a BinaryJson value and append it to a builder of the value type's arrow storage.
void AppendValueFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob, const EValueType valueType);

// Element type to represent result of merging arrays with arg types
EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next);

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
