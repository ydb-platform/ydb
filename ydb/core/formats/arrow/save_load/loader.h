#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/splitter/stats.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NAccessor {

class TColumnLoader {
private:
    YDB_READONLY_DEF(NSerialization::TSerializerContainer, Serializer);
    YDB_READONLY_DEF(NAccessor::TConstructorContainer, AccessorConstructor);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, ResultField);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    const ui32 ColumnId;

    TConclusion<std::shared_ptr<IChunkedArray>> BuildAccessor(const TString& originalData, const TChunkConstructionData& chunkData) const;

public:
    std::optional<NSplitter::TSimpleSerializationStat> TryBuildColumnStat() const;

    std::shared_ptr<IChunkedArray> BuildDefaultAccessor(const ui32 recordsCount) const;

    bool IsEqualTo(const TColumnLoader& item) const;

    TString DebugString() const;

    TColumnLoader(const NSerialization::TSerializerContainer& serializer, const NAccessor::TConstructorContainer& accessorConstructor,
        const std::shared_ptr<arrow::Field>& resultField, const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 columnId);

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const std::shared_ptr<arrow::Field>& GetField() const;

    TChunkConstructionData BuildAccessorContext(const ui32 recordsCount, const std::optional<ui32>& notNullCount = std::nullopt) const;
    std::shared_ptr<IChunkedArray> ApplyVerified(
        const TString& data, const ui32 expectedRecordsCount, const std::optional<ui32>& notNullCount = std::nullopt) const;
    TConclusion<std::shared_ptr<IChunkedArray>> ApplyConclusion(
        const TString& data, const ui32 expectedRecordsCount, const std::optional<ui32>& notNullCount = std::nullopt) const;
};

}   // namespace NKikimr::NArrow::NAccessor
