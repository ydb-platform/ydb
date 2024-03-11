#pragma once
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TColumnSaver {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::TSerializerContainer Serializer;
public:
    TColumnSaver() = default;
    TColumnSaver(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer serializer);

    bool IsHardPacker() const;

    TString Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const;

    TString Apply(const std::shared_ptr<arrow::RecordBatch>& data) const;
};


}