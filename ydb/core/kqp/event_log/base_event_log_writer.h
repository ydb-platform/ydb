#pragma once

#include "log_column.h"

#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <functional>
#include <memory>

namespace NKikimr::NKqp::NSchematizedLog {

class TBaseEventLogWriter : public NActors::NStructuredLog::ILogSink {
public:
    using TLogMessageFilter = std::function<bool(NActors::NStructuredLog::TLogMessage)>;

    TBaseEventLogWriter(TLogMessageFilter filter, TVector<std::shared_ptr<TSchematizedLogColumn>> columns);

    const TVector<std::shared_ptr<TSchematizedLogColumn>>& GetColumns() const {
        return Columns;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;
    void Flush() override;

protected:
    virtual void CreateOrUpdateStorage() = 0;
    virtual void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    std::shared_ptr<arrow::RecordBatch> CreateCurrentBatch();

    const TLogMessageFilter Filter;
    const TVector<std::shared_ptr<TSchematizedLogColumn>> Columns;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    bool StorageExists {false};
    unsigned WrittenRecordCount {0};
};

} // namespace NKikimr::NKqp::NSchematizedLog
