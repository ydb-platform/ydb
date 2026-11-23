#pragma once

#include <ydb/core/formats/arrow/accessor/common/types.h>

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <yql/essentials/types/binary_json/format.h>

// Subcolumn builders that append a value from type-aware view or decode one from BinaryJson.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TEncodingPlainBuilder: public TTrivialArray::TPlainBuilderBase {
private:
    EValueType ValueType;

public:
    TEncodingPlainBuilder(const EValueType valueType, const ui32 reserveItems, const ui32 reserveData)
        : TPlainBuilderBase(MakeBuilderForValueType(valueType, reserveItems, reserveData))
        , ValueType(valueType)
    {
    }

    void AddFromBinaryJson(const ui32 recordIndex, const NBinaryJson::TBinaryJson& blob) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AppendValueFromBinaryJson(builder, blob, ValueType); });
    }

    void AddValue(const ui32 recordIndex, const TJsonValueView& value) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AppendValueFromView(builder, value, ValueType); });
    }
};

class TEncodingSparsedBuilder: public TSparsedArray::TSparsedBuilderBase {
private:
    EValueType ValueType;

public:
    TEncodingSparsedBuilder(const EValueType valueType, const ui32 reserveItems, const ui32 reserveData)
        : TSparsedBuilderBase(MakeBuilderForValueType(valueType, reserveItems, reserveData), GetArrowTypeForValueType(valueType), nullptr, reserveItems)
        , ValueType(valueType)
    {
    }

    void AddFromBinaryJson(const ui32 recordIndex, const NBinaryJson::TBinaryJson& blob) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AppendValueFromBinaryJson(builder, blob, ValueType); });
    }

    void AddValue(const ui32 recordIndex, const TJsonValueView& value) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AppendValueFromView(builder, value, ValueType); });
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
