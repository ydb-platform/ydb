#pragma once

#include <deque>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/types/binary_json/format.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Logical (as seen by external consumers) value type of data stored in a subcolumn.
// PERSISTED DO NOT REMOVE/REORDER: these numeric codes are written to disk as the `value_type` column of TDictStats.
enum class EValueType : ui8 {
    BinaryJson = 0,
    Double = 1,
    Bool = 2,
    String = 3,
};

// Conversion between physical arrow storage and logical JsonValue/BinaryJson in-program representation
class IValueArrowCodec {
public:
    virtual ~IValueArrowCodec() = default;

    virtual EValueType GetValueType() const = 0;
    virtual std::shared_ptr<arrow::DataType> GetArrowType() const = 0;
    // Dictionary encoding is enabled only for the variable-length types (BinaryJson blobs, raw strings);
    // for the compact native Double/Bool arrays a fixed-size dict ref would not pay off.
    virtual bool CanBeDictionaryEncoded() const = 0;

    // Read path - translate physical ref to logical
    virtual NJson::TJsonValue ReadToJson(const arrow::Array& array, const i64 index) const = 0;
    virtual NBinaryJson::TBinaryJson ReadToBinaryJson(const arrow::Array& array, const i64 index) const = 0;

    // Write path - init and write a logical value to builder
    // reserveData makes sense only for variable-length types (BinaryJson and string).
    virtual std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 reserveData) const = 0;
    virtual void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const = 0;
};

std::shared_ptr<const IValueArrowCodec> GetCodecForValueType(const EValueType valueType);

// Element type to represent result of merging arrays with arg types
EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next);

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
