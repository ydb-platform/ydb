#include "meta.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/formats/arrow/scalar/serialization.h>
#include <ydb/library/formats/arrow/switch/compare.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NMax {

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::DoBuildIndexImpl(
    TChunkedBatchReader& reader, const ui32 recordsCount, const TIndexInfo& indexInfo) const {
    const auto& column = indexInfo.GetColumnFeaturesVerified(GetColumnId());
    const auto& logicalType = column.GetTypeInfo();
    std::shared_ptr<arrow::Scalar> result;
    const bool numericDecimal = NArrow::TComparator::IsNumericDecimal(logicalType);
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    {
        TChunkedColumnReader cReader = *reader.begin();
        for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
            if (numericDecimal) {
                const auto& chunk = cReader.GetCurrentChunk();
                for (ui32 i = 0; i < chunk->GetRecordsCount(); ++i) {
                    auto current = chunk->GetScalar(i);
                    AFL_VERIFY(current);
                    if (!result || !result->is_valid || (current->is_valid && NArrow::ScalarCompare(*result, *current, logicalType) < 0)) {
                        result = std::move(current);
                    }
                }

                continue;
            }

            auto currentScalar = cReader.GetCurrentChunk()->GetMaxScalar();
            AFL_VERIFY(currentScalar);
            if (!result || NArrow::ScalarCompare(*result, *currentScalar, logicalType) == -1) {
                result = currentScalar;
            }
        }
    }

    const TString indexData = NArrow::NScalar::TSerializer::SerializePayloadToString(result).DetachResult();
    return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
}

std::shared_ptr<arrow::Scalar> TIndexMeta::GetMaxScalarVerified(const std::vector<TString>& data, const TIndexInfo& indexInfo) const {
    const auto& column = indexInfo.GetColumnFeaturesVerified(GetColumnId());
    const auto& dataType = column.GetArrowField()->type();
    const auto& logicalType = column.GetTypeInfo();
    AFL_VERIFY(data.size());
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& d : data) {
        std::shared_ptr<arrow::Scalar> current = NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(d, dataType).DetachResult();
        if (!result || NArrow::ScalarCompare(*result, *current, logicalType) == -1) {
            result = current;
        }
    }

    return result;
}

NJson::TJsonValue TIndexMeta::DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const {
    auto scalar = GetMaxScalarVerified({ data }, indexInfo);
    return scalar->ToString();
}

}   // namespace NKikimr::NOlap::NIndexes::NMax
