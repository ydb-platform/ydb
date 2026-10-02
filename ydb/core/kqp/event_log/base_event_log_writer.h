#pragma once

#include "log_column.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <util/datetime/base.h>

#include <functional>
#include <memory>

namespace NKikimr::NKqp::NEventLog {

class TBaseEventLogWriter : public NActors::NStructuredLog::ILogSink,
    public std::enable_shared_from_this<TBaseEventLogWriter>   {
public:
    TBaseEventLogWriter(TVector<std::shared_ptr<TSchematizedLogColumn>> columns,
        const TDuration& flushInterval);

    const TVector<std::shared_ptr<TSchematizedLogColumn>>& GetColumns() const {
        return Columns;
    }

    virtual bool Filter(const NActors::NStructuredLog::TLogMessage&) = 0;

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;

    virtual void Flush();

    unsigned GetCurrentBatchSize() const {
        return CurrentBatchSize;
    }

    virtual bool IsAlive() const {
        return CreationState.load() != TCreationState::Stop;
    };

protected:
    virtual void CreateOrUpdateStorage() = 0;
    virtual void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    std::shared_ptr<arrow::RecordBatch> CreateCurrentBatch();

    const TVector<std::shared_ptr<TSchematizedLogColumn>> Columns;
    const TDuration FlushInterval;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    enum class TCreationState {
        Unknown = 1,
        Creating = 2,
        Exists = 3,
        Stop = 3,
    };
    std::atomic<TCreationState> CreationState {TCreationState::Unknown};
    unsigned CurrentBatchSize {0};
};

class TBaseEventLogAutoFlushActor : public NActors::TActorBootstrapped<TBaseEventLogAutoFlushActor> {
public:
    TBaseEventLogAutoFlushActor(std::shared_ptr<TBaseEventLogWriter> writer, TDuration flushInterval);

    void Bootstrap();

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        }
    }

private:
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const NActors::TActorContext& ctx);
    void ScheduleNextFlush();

    const std::shared_ptr<TBaseEventLogWriter> Writer;
    const TDuration FlushInterval;
};

} // namespace NKikimr::NKqp::NEventLog
