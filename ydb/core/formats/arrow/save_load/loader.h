#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NAccessor {

class TColumnLoader {
private:
    NSerialization::TSerializerContainer Serializer;
    NTransformation::ITransformer::TPtr Transformer;
    YDB_READONLY_DEF(NAccessor::TConstructorContainer, AccessorConstructor);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, ResultField);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    const ui32 ColumnId;

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Apply(const TString& data) const;
    std::shared_ptr<IChunkedArray> BuildAccessor(
        const std::shared_ptr<arrow::RecordBatch>& batch, const TChunkConstructionData& chunkData) const;

public:
    std::shared_ptr<IChunkedArray> BuildDefaultAccessor(const ui32 recordsCount) const;

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

    TColumnLoader(NTransformation::ITransformer::TPtr transformer, const NSerialization::TSerializerContainer& serializer,
        const NAccessor::TConstructorContainer& accessorConstructor, const std::shared_ptr<arrow::Field>& resultField,
        const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 columnId);

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const std::shared_ptr<arrow::Field>& GetField() const;

    std::shared_ptr<IChunkedArray> ApplyVerified(const TString& data, const ui32 expectedRecordsCount) const;
    std::shared_ptr<arrow::RecordBatch> ApplyRawVerified(const TString& data) const;
};

}   // namespace NKikimr::NArrow::NAccessor
