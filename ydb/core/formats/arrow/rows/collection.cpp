#include "collection.h"
#include "view.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow {

void TRowsCollection::Initialize(const std::shared_ptr<arrow::RecordBatch>& data) {
    std::vector<TString> rawData;
    AFL_VERIFY(data->num_rows());
    rawData.resize(data->num_rows());
    for (ui32 i = 0; i < (ui32)data->num_rows(); ++i) {
        rawData[i] = TSimpleRowViewV0::BuildString(data, i);
    }
    RawData = std::move(rawData);
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> TRowsCollection::BuildBatch(const std::shared_ptr<arrow::Schema>& schema) const {
    auto builders = NArrow::MakeBuilders(schema, RawData.size());
    TString errorMessage;
    for (auto&& s : RawData) {
        auto conclusion = TSimpleRowViewV0(s).AddToBuilders(builders, schema);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return arrow::RecordBatch::Make(schema, RawData.size(), FinishBuilders(std::move(builders)));
}

TConclusion<TString> TRowsCollection::DebugString(const std::shared_ptr<arrow::Schema>& schema) const {
    auto rb = BuildBatch(schema);
    if (rb.IsFail()) {
        return rb;
    }
    const std::string str = rb.GetResult()->ToString();
    return TString(str.data(), str.size());
}

TSimpleRow TRowsCollection::GetFirst(const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY(RawData.size());
    return TSimpleRow(RawData.front(), schema);
}

TSimpleRow TRowsCollection::GetLast(const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY(RawData.size());
    return TSimpleRow(RawData.back(), schema);
}

TSimpleRow TRowsCollection::GetRecord(const ui32 recordIndex, const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY(recordIndex < RawData.size());
    return TSimpleRow(RawData[recordIndex], schema);
}

}   // namespace NKikimr::NArrow
