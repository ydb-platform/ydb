#include "base_event_log_writer.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/text_writer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#define YDB_LOG_THIS_FILE_COMPONENT KQP_SLOW_LOG

namespace NKikimr::NKqp::NEventLog {

TBaseEventLogWriter::TBaseEventLogWriter(
    TVector<std::shared_ptr<TEventLogColumn>> columns, ui32 maxBatchSize, const TDuration& flushInterval)
    : Columns(std::move(columns)),
    MaxBatchSize(maxBatchSize),
    FlushInterval(flushInterval)
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
    auto stateKind = State.load().Kind;
    if (stateKind == TStateKind::StorageCreateError || stateKind == TStateKind::Stop) {
        return false;
    }

    if (!Filter(message)) {
        return false;
    }

    if (CurrentBatchSize >= MaxBatchSize) {
        return false;
    }

    if (CheckFlushActorCreate) {
        if (FlushInterval) {
            NActors::TActivationContext::Register(
                new TBaseEventLogAutoFlushActor(shared_from_this(), FlushInterval));
        }
        CheckFlushActorCreate = false;
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
            case TEventLogColumn::TWriteResultKind::Success:
                break;
            case TEventLogColumn::TWriteResultKind::DummyValueInsteadOfNull:
                errorText << "Dummy \"" << column->Name << "\" instead of null";
                break;
            case TEventLogColumn::TWriteResultKind::DummyValueInsteadOfCastError:
                errorText << "Dummy \"" << column->Name << "\" instead of not casted value "
                          << TTextWriter::EscapeFieldValue(result.Value);
                break;
            case TEventLogColumn::TWriteResultKind::NullInsteadOfCastError:
                errorText << "Null \"" << column->Name << "\" instead of not casted value "
                          << TTextWriter::EscapeFieldValue(result.Value);
                break;
            case TEventLogColumn::TWriteResultKind::ArrowError:
                if (!column->WriteDummyValue()) {
                    YDB_LOG_ERROR("Arrow data write error occurs. Error writing dummy value too");
                }
                if (column->Settings.IsNotNull) {
                    errorText << "Dummy \"" << column->Name << "\" due to internal write error";
                } else {
                    errorText << "Null \"" << column->Name << "\" due to internal write error";
                }
                break;
            case TEventLogColumn::TWriteResultKind::UnknownError:
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
    CurrentBatchSize++;
    return true;
}

void TBaseEventLogWriter::Flush() {
    if (CurrentBatchSize == 0) {
        return;
    }

    // Check state
    TState oldState = State.load();
    if (oldState.Kind == TStateKind::StorageCreateError || oldState.Kind == TStateKind::Stop) {
        return ;
    }

    // Check need to create storage
    TState newState = oldState;
    oldState.Kind = TStateKind::Started;
    newState.Kind = TStateKind::StorageCreating;
    if (State.compare_exchange_strong(oldState, TState(TStateKind::StorageCreating))) {
        CreateOrUpdateStorage();
        return ;
    }

    // If storage should be created
    if (State.load().Kind != TStateKind::Working) {
        return;
    }

    auto batch = CreateCurrentBatch();
    WriteBatch(batch);
    CurrentBatchSize = 0;
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
    auto batch = arrow::RecordBatch::Make(GetArrowSchema(), CurrentBatchSize, arrays);
    CurrentBatchSize = 0;
    return batch;
}

TBaseEventLogAutoFlushActor::TBaseEventLogAutoFlushActor(
    std::shared_ptr<TBaseEventLogWriter> writer,
    TDuration flushInterval)
    : Writer(std::move(writer))
    , FlushInterval(flushInterval)
{
}

void TBaseEventLogAutoFlushActor::Bootstrap() {
    Become(&TThis::StateWork);
    ScheduleNextFlush();
}

void TBaseEventLogAutoFlushActor::ScheduleNextFlush() {
    if (FlushInterval > TDuration::Zero()) {
        Schedule(FlushInterval, new NActors::TEvents::TEvWakeup());
    }
}

void TBaseEventLogAutoFlushActor::HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&, const NActors::TActorContext&) {
    if (Writer) {
        if (Writer->GetCurrentBatchSize() > 0) {
            Writer->Flush();
        }

        if (Writer->IsAlive()) {
            ScheduleNextFlush();
        } else {
            PassAway();
        }
    } else {
        PassAway();
    }
}

} // namespace NKikimr::NKqp::NEventLog
