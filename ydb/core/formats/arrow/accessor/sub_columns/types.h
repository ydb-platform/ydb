#pragma once

#include <deque>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/types/binary_json/format.h>

// Type conversions between BinaryJson, dedicated scalar types and arrow storage types.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Logical (as seen by external consumers) value type of data stored in a subcolumn.
// PERSISTED: these numeric codes are written to disk as the `value_type` column of TDictStats.
enum class EValueType : ui8 {
    BinaryJson = 0,
    Double = 1,
    Bool = 2,
    String = 3,
};

std::shared_ptr<arrow::DataType> GetArrowTypeForValueType(const EValueType valueType);


// Dictionary encoding only enabled for the binary-backed types (BinaryJson blobs and raw strings).
// Integral types would trade fixed-size position in array for fixed-size dictionary ref.
// May still be good for compression, but requires further experiments.
bool DictionaryApplicableForValueType(const EValueType valueType);

// Element type to represent result of merging arrays with arg types
EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next);

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values);

// Convert json blob to its contained scalar.
// Fail on verify if blob is not of specified type.
TStringBuf ExtractStringScalar(const NBinaryJson::TBinaryJson& blob);
double ExtractDoubleScalar(const NBinaryJson::TBinaryJson& blob);
bool ExtractBoolScalar(const NBinaryJson::TBinaryJson& blob);

// Read element `index` of a materialized native column array (interpreted per valueType) as a JSON
// value (document reconstruction) or a BinaryJson blob. The array's physical arrow type must match valueType.
NJson::TJsonValue ArrayElementToJsonValue(const arrow::Array& array, const i64 index, const EValueType valueType);
NBinaryJson::TBinaryJson ArrayElementToBinaryJson(const arrow::Array& array, const i64 index, const EValueType valueType);

ui32 ArrayElementSize(const arrow::Array& array, const i64 index, const EValueType valueType);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
