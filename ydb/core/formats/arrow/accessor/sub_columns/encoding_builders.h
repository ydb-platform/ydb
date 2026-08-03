#pragma once

#include "types.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <yql/essentials/types/binary_json/format.h>

// Subcolumn builders that either copy an arrow-native value or decode one from BinaryJson.
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

    void AddArrayElement(const ui32 recordIndex, const arrow::Array& array, const ui32 position) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AFL_VERIFY(NArrow::Append(builder, array, position)); });
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

    void AddArrayElement(const ui32 recordIndex, const arrow::Array& array, const ui32 position) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AFL_VERIFY(NArrow::Append(builder, array, position)); });
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
