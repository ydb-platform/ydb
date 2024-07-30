#include "meta.h"

#include <ydb/core/formats/arrow/scalar/serialization.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NMax {

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    std::shared_ptr<arrow::Scalar> result;
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    {
        TChunkedColumnReader cReader = *reader.begin();
        for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
            auto minMax = NArrow::FindMinMaxPosition(cReader.GetCurrentChunk());
            auto currentScalar = NArrow::GetScalar(cReader.GetCurrentChunk(), minMax.second);
            if (!result || NArrow::ScalarCompare(*result, *currentScalar) == -1) {
                result = currentScalar;
            }
        }
    }
    return NArrow::NScalar::TSerializer::SerializePayloadToString(result).DetachResult();
}

void TIndexMeta::DoFillIndexCheckers(
    const std::shared_ptr<NRequest::TDataForIndexesCheckers>& /*info*/, const NSchemeShard::TOlapSchema& /*schema*/) const {
}

std::shared_ptr<arrow::Scalar> TIndexMeta::GetMaxScalarVerified(
    const std::vector<TString>& data, const std::shared_ptr<arrow::DataType>& dataType) const {
    AFL_VERIFY(data.size());
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& d : data) {
        std::shared_ptr<arrow::Scalar> current = NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(d, dataType).DetachResult();
        if (!result || NArrow::ScalarCompare(*result, *current) == -1) {
            result = current;
        }
    }
    return result;
}

NJson::TJsonValue TIndexMeta::DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const {
    AFL_VERIFY(ColumnIds.size() == 1);
    auto scalar = GetMaxScalarVerified({ data }, indexInfo.GetColumnFeaturesVerified(*ColumnIds.begin()).GetArrowField()->type());
    return scalar->ToString();
}

}   // namespace NKikimr::NOlap::NIndexes::NMax
