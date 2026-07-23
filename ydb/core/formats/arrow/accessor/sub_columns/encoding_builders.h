#pragma once

#include "types.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <ydb/library/actors/core/log.h>

#include <yql/essentials/types/binary_json/format.h>

// Subcolumn builders that either copy an arrow-native value or decode one from BinaryJson.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TEncodingPlainBuilder: public TTrivialArray::TPlainBuilderBase {
private:
    std::shared_ptr<const IValueArrowCodec> Codec;

public:
    TEncodingPlainBuilder(const std::shared_ptr<const IValueArrowCodec>& codec, const ui32 reserveItems, const ui32 reserveData)
        : TPlainBuilderBase(codec->MakeBuilder(reserveItems, reserveData))
        , Codec(codec)
    {
    }

    void AddFromBinaryJson(const ui32 recordIndex, const NBinaryJson::TBinaryJson& blob) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { Codec->AppendFromBinaryJson(builder, blob); });
    }

    void AddArrayElement(const ui32 recordIndex, const arrow::Array& array, const ui32 position) {
        AFL_VERIFY(GetBuilder().type()->id() == array.type_id())("builder", GetBuilder().type()->ToString())("array", array.type()->ToString());
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AFL_VERIFY(NArrow::Append(builder, array, position)); });
    }
};

class TEncodingSparsedBuilder: public TSparsedArray::TSparsedBuilderBase {
private:
    std::shared_ptr<const IValueArrowCodec> Codec;

public:
    TEncodingSparsedBuilder(const std::shared_ptr<const IValueArrowCodec>& codec, const ui32 reserveItems, const ui32 reserveData)
        : TSparsedBuilderBase(codec->MakeBuilder(reserveItems, reserveData), codec->GetArrowType(), nullptr, reserveItems)
        , Codec(codec)
    {
    }

    void AddFromBinaryJson(const ui32 recordIndex, const NBinaryJson::TBinaryJson& blob) {
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { Codec->AppendFromBinaryJson(builder, blob); });
    }

    void AddArrayElement(const ui32 recordIndex, const arrow::Array& array, const ui32 position) {
        AFL_VERIFY(GetValueBuilder().type()->id() == array.type_id())("builder", GetValueBuilder().type()->ToString())("array", array.type()->ToString());
        AddAt(recordIndex, [&](arrow::ArrayBuilder& builder) { AFL_VERIFY(NArrow::Append(builder, array, position)); });
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
