#include "result_builder.h"

#include <ydb/core/formats/arrow/common/validation.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NArrow::NMerger {

void TRecordBatchBuilder::ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema) {
    AFL_VERIFY(IsSameFieldsSequence(schema->fields(), Fields));
}

void TRecordBatchBuilder::AddRecord(const TSortableBatchPosition& position) {
    AFL_VERIFY_DEBUG(position.GetData().GetColumns().size() == Builders.size());
    AFL_VERIFY_DEBUG(IsSameFieldsSequence(position.GetData().GetFields(), Fields));
//    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "record_add_on_read")("record", position.DebugJson());
    for (ui32 i = 0; i < position.GetData().GetColumns().size(); ++i) {
        position.GetData().GetColumns()[i].AppendPositionTo(*Builders[i], position.GetPosition(), MemoryBufferLimit ? &CurrentBytesUsed : nullptr);
    }
    ++RecordsCount;
}

bool TRecordBatchBuilder::IsSameFieldsSequence(const std::vector<std::shared_ptr<arrow::Field>>& f1, const std::vector<std::shared_ptr<arrow::Field>>& f2) {
    if (f1.size() != f2.size()) {
        return false;
    }
    for (ui32 i = 0; i < f1.size(); ++i) {
        if (!f1[i]->Equals(f2[i])) {
            return false;
        }
    }
    return true;
}

TRecordBatchBuilder::TRecordBatchBuilder(const std::vector<std::shared_ptr<arrow::Field>>& fields, const std::optional<ui32> rowsCountExpectation /*= {}*/, const THashMap<std::string, ui64>& fieldDataSizePreallocated /*= {}*/)
    : Fields(fields)
{
    AFL_VERIFY(Fields.size());
    for (auto&& f : fields) {
        Builders.emplace_back(NArrow::MakeBuilder(f));
        auto it = fieldDataSizePreallocated.find(f->name());
        if (it != fieldDataSizePreallocated.end()) {
            NArrow::ReserveData(*Builders.back(), it->second);
        }
        if (rowsCountExpectation) {
            NArrow::TStatusValidator::Validate(Builders.back()->Reserve(*rowsCountExpectation));
        }
    }
}

std::shared_ptr<arrow::RecordBatch> TRecordBatchBuilder::Finalize() {
    auto schema = std::make_shared<arrow::Schema>(Fields);
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& i : Builders) {
        columns.emplace_back(NArrow::TStatusValidator::GetValid(i->Finish()));
    }
    return arrow::RecordBatch::Make(schema, columns.front()->length(), columns);
}

TString TRecordBatchBuilder::GetColumnNames() const {
    TStringBuilder result;
    for (auto&& f : Fields) {
        result << f->name() << ",";
    }
    return result;
}

}
