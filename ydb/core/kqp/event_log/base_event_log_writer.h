#pragma once

#include "log_column.h"

#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <functional>
#include <memory>

namespace NKikimr::NKqp::NEventLog {

class TBaseEventLogWriter : public NActors::NStructuredLog::ILogSink {
public:
    TBaseEventLogWriter(TVector<std::shared_ptr<TSchematizedLogColumn>> columns);

    const TVector<std::shared_ptr<TSchematizedLogColumn>>& GetColumns() const {
        return Columns;
    }

    virtual bool Filter(const NActors::NStructuredLog::TLogMessage&) = 0;

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;

    virtual void Flush();

    unsigned GetCurrentBatchSize() const {
        return CurrentBatchSize;
    }
protected:
    virtual void CreateOrUpdateStorage() = 0;
    virtual void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    std::shared_ptr<arrow::RecordBatch> CreateCurrentBatch();

    const TVector<std::shared_ptr<TSchematizedLogColumn>> Columns;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    enum class TCreationState {
        Unknown = 1,
        Creating = 2,
        Exists = 3
    };
    std::atomic<TCreationState> CreationState {TCreationState::Unknown};
    unsigned CurrentBatchSize {0};
};

} // namespace NKikimr::NKqp::NEventLog
