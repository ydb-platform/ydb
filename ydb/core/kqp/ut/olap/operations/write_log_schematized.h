#pragma once
#include "write_log_column.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <functional>
#include <memory>

namespace NKikimr::NKqp::NSchematizedLog {

class TBaseSchematizedLogWriter : public NActors::NStructuredLog::ILogSink {
public:
    using TLogMessageFilter = std::function<bool(NActors::NStructuredLog::TLogMessage)>;

    TBaseSchematizedLogWriter(TKikimrRunner& runner, TLogMessageFilter filter, TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
        : Runner(runner)
        , Filter(std::move(filter))
        , Columns(std::move(columns)) {

        for(std::size_t i = 0;i < Columns.size();i++) {
            ErrorColumn = std::dynamic_pointer_cast<TDBLogMessageErrorColumn>(Columns[i]);
            if (ErrorColumn != nullptr) {
                ErrorColumnIndex = i;
                break;
            }
        }
    }

    TKikimrRunner& GetRunner() const {
        return Runner;
    }

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

    TKikimrRunner& Runner;
    const TLogMessageFilter Filter;
    const TVector<std::shared_ptr<TSchematizedLogColumn>> Columns;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    bool StorageExists {false};
    unsigned WrittenRecordCount{0};
};

}
