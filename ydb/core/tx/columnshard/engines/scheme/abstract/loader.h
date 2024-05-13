#pragma once
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TColumnLoader {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::TSerializerContainer Serializer;
    std::shared_ptr<arrow::Schema> ExpectedSchema;
    const ui32 ColumnId;
public:
    bool IsEqualTo(const TColumnLoader& item) const {
        if (!!Transformer != !!item.Transformer) {
            return false;
        } else if (!!Transformer && !Transformer->IsEqualTo(*item.Transformer)) {
            return false;
        }
        if (!Serializer.IsEqualTo(item.Serializer)) {
            return false;
        }
        return true;
    }

    TString DebugString() const;

    TColumnLoader(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer& serializer,
        const std::shared_ptr<arrow::Schema>& expectedSchema, const ui32 columnId);

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const std::shared_ptr<arrow::Field>& GetField() const;

    const std::shared_ptr<arrow::Schema>& GetExpectedSchema() const {
        return ExpectedSchema;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Apply(const TString& data) const;

    std::shared_ptr<arrow::RecordBatch> ApplyVerified(const TString& data) const;

    std::shared_ptr<arrow::Array> ApplyVerifiedColumn(const TString& data) const;
};

}
