#pragma once
#include "write_log_column.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <memory>

namespace NKikimr::NKqp::NSchematizedLog {

class TBaseSchematizedLogWriter : public NActors::NStructuredLog::ILogSink {
public:
    TBaseSchematizedLogWriter(TKikimrRunner& runner, NLog::EComponent component, TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
        : Runner(runner)
        , Component(component)
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

    NLog::EComponent GetComponent() const {
        return Component;
    }

    const TVector<std::shared_ptr<TSchematizedLogColumn>>& GetColumns() const {
        return Columns;
    }

    bool IsTableExists() const {
        return TableExists;
    }

    void Write(const NActors::NStructuredLog::TLogMessage&) override;
    void Flush() override;

    virtual void CreateOrUpdateStorage() = 0;
    virtual void DeleteStorageIfExists() = 0;
    virtual void CleanupStorageIfExists(TInstant before) = 0;
protected:
    virtual void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    std::shared_ptr<arrow::RecordBatch> CreateCurrentBatch();

    TKikimrRunner& Runner;
    const NLog::EComponent Component;
    const TVector<std::shared_ptr<TSchematizedLogColumn>> Columns;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    bool TableExists {false};
    unsigned WrittenRecordCount{0};
};

}
