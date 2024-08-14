#pragma once
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TColumnSaver {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    //YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    NArrow::NSerialization::TSerializerContainer Serializer;
    std::map<ui32, NArrow::NSerialization::TSerializerContainer> SerializerBySizeUpperBorder;

public:
    TColumnSaver() = default;
    TColumnSaver(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer serializer);

    void AddSerializerWithBorder(const ui32 upperBorder, const NArrow::NSerialization::TSerializerContainer& serializer) {
        if (Serializer.IsCompatibleForExchange(serializer)) {
            AFL_VERIFY(SerializerBySizeUpperBorder.emplace(upperBorder, serializer).second);
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_serializer")("reason", "incompatible_serializers")(
                "border", upperBorder);
        }
    }

    bool IsHardPacker() const;
    TString Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const;
    TString Apply(const std::shared_ptr<arrow::RecordBatch>& data) const;
};


}