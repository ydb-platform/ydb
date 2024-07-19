#pragma once
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TColumnLoader {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::TSerializerContainer Serializer;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, ExpectedSchema);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
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
        const std::shared_ptr<arrow::Schema>& expectedSchema, const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 columnId);

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const std::shared_ptr<arrow::Field>& GetField() const;

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Apply(const TString& data) const;

    std::shared_ptr<arrow::RecordBatch> ApplyVerified(const TString& data) const;

    std::shared_ptr<arrow::Array> ApplyVerifiedColumn(const TString& data) const;
};

}
