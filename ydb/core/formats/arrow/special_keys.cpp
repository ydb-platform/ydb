#include "permutations.h"
#include "size_calcer.h"
#include "special_keys.h"

#include "reader/position.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NArrow {

bool TSpecialKeys::DeserializeFromString(const TString& data) {
    if (!data) {
        return false;
    }
    auto rbData = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer()->Deserialize(data));
    Initialize(rbData);
    return true;
}

TSimpleRow TSpecialKeys::GetKeyByIndex(const ui32 position) const {
    return Rows.GetRecord(position, Schema);
}

TString TSpecialKeys::SerializePayloadToString() const {
    return NArrow::NSerialization::TSerializerContainer::GetFastestSerializer()->SerializePayload(BuildBatch());
}

TString TSpecialKeys::SerializeFullToString() const {
    return NArrow::NSerialization::TSerializerContainer::GetFastestSerializer()->SerializeFull(BuildBatch());
}

ui64 TSpecialKeys::GetMemorySize() const {
    return Rows.GetMemorySize();
}

ui64 TSpecialKeys::GetDataSize() const {
    return Rows.GetDataSize();
}

TString TSpecialKeys::DebugString() const {
    return BuildBatch()->ToString();
}

std::shared_ptr<arrow::RecordBatch> TSpecialKeys::BuildBatch() const {
    return Rows.BuildBatch(Schema).GetResult();
}

void TSpecialKeys::Reallocate() {
    std::shared_ptr<arrow::Schema> schema = Schema;
    Schema = std::move(schema);
    Rows.Reallocate();
}

TFirstLastSpecialKeys::TFirstLastSpecialKeys(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<TString>& columnNames /*= {}*/) {
    Y_ABORT_UNLESS(batch);
    Y_ABORT_UNLESS(batch->num_rows());
    std::shared_ptr<arrow::RecordBatch> keyBatch = batch;
    if (columnNames.size()) {
        keyBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(batch, columnNames);
    }
    if (keyBatch->num_rows() <= 2) {
        Initialize(keyBatch);
    } else {
        std::vector<ui64> indexes = { 0 };
        if (batch->num_rows() > 1) {
            indexes.emplace_back(batch->num_rows() - 1);
        }

        Initialize(NArrow::CopyRecords(keyBatch, indexes));
    }
}

TMinMaxSpecialKeys::TMinMaxSpecialKeys(std::shared_ptr<arrow::RecordBatch> batch, const std::shared_ptr<arrow::Schema>& schema) {
    Y_ABORT_UNLESS(batch);
    Y_ABORT_UNLESS(batch->num_rows());
    Y_ABORT_UNLESS(schema);

    NMerger::TRWSortableBatchPosition record(batch, 0, schema->field_names(), {}, false);
    std::optional<NMerger::TCursor> minValue;
    std::optional<NMerger::TCursor> maxValue;
    while (true) {
        if (!minValue || record.Compare(*minValue) == std::partial_ordering::less) {
            minValue = record.BuildSortingCursor();
        }
        if (!maxValue || record.Compare(*maxValue) == std::partial_ordering::greater) {
            maxValue = record.BuildSortingCursor();
        }
        if (!record.NextPosition(1)) {
            break;
        }
    }
    Y_ABORT_UNLESS(minValue && maxValue);
    std::vector<ui64> indexes;
    indexes.emplace_back(minValue->GetPosition());
    if (maxValue->GetPosition() != minValue->GetPosition()) {
        indexes.emplace_back(maxValue->GetPosition());
    }

    std::vector<TString> columnNamesString;
    for (auto&& i : schema->field_names()) {
        columnNamesString.emplace_back(i);
    }

    auto dataBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(batch, columnNamesString);
    Initialize(NArrow::CopyRecords(dataBatch, indexes));
}

TFirstLastSpecialKeys::TFirstLastSpecialKeys(const TString& data)
    : TBase(data) {
}

TMinMaxSpecialKeys::TMinMaxSpecialKeys(const TString& data)
    : TBase(data) {
}

}   // namespace NKikimr::NArrow
