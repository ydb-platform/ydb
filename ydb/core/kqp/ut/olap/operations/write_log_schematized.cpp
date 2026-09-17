#include "write_log_schematized.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKqp::NSchematizedLog {

void TBaseSchematizedLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (message.Component != Component) {
        return ;
    }

    for(auto& column: Columns) {
        column->Write(message);
    }
    WrittenRecordCount++;
}

void TBaseSchematizedLogWriter::Flush() {
    if (WrittenRecordCount==0) {
        return ;
    }
    if (!TableExists) {
        return ;
    }
    auto batch = CreateCurrentBatch();
    WriteBatch(batch);
    WrittenRecordCount = 0;
}

std::shared_ptr<arrow::Schema> TBaseSchematizedLogWriter::GetArrowSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(Columns.size());
    for (const auto& column : Columns) {
        fields.emplace_back(column->MakeArrowField());
    }
    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::RecordBatch> TBaseSchematizedLogWriter::CreateCurrentBatch() {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for(auto& column: Columns) {
        arrays.push_back(column->MakeArray());
    }
    auto batch = arrow::RecordBatch::Make(GetArrowSchema(), WrittenRecordCount, arrays);
    WrittenRecordCount = 0;
    return batch;
}

}
