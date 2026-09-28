#include "base_event_log_writer.h"

#include <ydb/library/actors/struct_log/text_writer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKqp::NSchematizedLog {

TBaseEventLogWriter::TBaseEventLogWriter(
    TLogMessageFilter filter,
    TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
    : Filter(std::move(filter))
    , Columns(std::move(columns))
{
    for (std::size_t i = 0; i < Columns.size(); ++i) {
        ErrorColumn = std::dynamic_pointer_cast<TDBLogMessageErrorColumn>(Columns[i]);
        if (ErrorColumn != nullptr) {
            ErrorColumnIndex = i;
            break;
        }
    }
}

bool TBaseEventLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (Filter && !Filter(message)) {
        return false;
    }
    if (!StorageExists) {
        CreateOrUpdateStorage();
        if (!StorageExists) {
            return false;
        }
    }

    TStringBuilder columnWriteErrors;
    for (std::size_t i = 0; i < Columns.size(); ++i) {
        if (ErrorColumnIndex.has_value() && ErrorColumnIndex.value() == i) {
            continue;
        }

        const auto& column = Columns[i];
        auto result = column->Write(message);
        TStringBuilder errorText;

        switch (result.Kind) {
            case TSchematizedLogColumn::TWriteResultKind::Success:
                break;
            case TSchematizedLogColumn::TWriteResultKind::DummyValueInsteadOfNull:
                errorText << "Dummy \"" << column->Name << "\" instead of null";
                break;
            case TSchematizedLogColumn::TWriteResultKind::DummyValueInsteadOfCastError:
                errorText << "Dummy \"" << column->Name << "\" instead of not casted value "
                          << TTextWriter::EscapeFieldValue(result.Value);
                break;
            case TSchematizedLogColumn::TWriteResultKind::NullInsteadOfCastError:
                errorText << "Null \"" << column->Name << "\" instead of not casted value "
                          << TTextWriter::EscapeFieldValue(result.Value);
                break;
            case TSchematizedLogColumn::TWriteResultKind::ArrowError:
            case TSchematizedLogColumn::TWriteResultKind::UnknownError:
                break;
        }

        if (!errorText.empty()) {
            if (!columnWriteErrors.empty()) {
                columnWriteErrors << "; ";
            }
            columnWriteErrors << errorText;
        }
    }

    if (ErrorColumn != nullptr) {
        ErrorColumn->Write(columnWriteErrors);
    }
    WrittenRecordCount++;
    return true;
}

void TBaseEventLogWriter::Flush() {
    if (WrittenRecordCount == 0) {
        return;
    }
    if (!StorageExists) {
        return;
    }
    auto batch = CreateCurrentBatch();
    WriteBatch(batch);
    WrittenRecordCount = 0;
}

std::shared_ptr<arrow::Schema> TBaseEventLogWriter::GetArrowSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(Columns.size());
    for (const auto& column : Columns) {
        fields.emplace_back(column->MakeArrowField());
    }
    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::RecordBatch> TBaseEventLogWriter::CreateCurrentBatch() {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& column : Columns) {
        arrays.push_back(column->MakeArray());
    }
    auto batch = arrow::RecordBatch::Make(GetArrowSchema(), WrittenRecordCount, arrays);
    WrittenRecordCount = 0;
    return batch;
}

} // namespace NKikimr::NKqp::NSchematizedLog
