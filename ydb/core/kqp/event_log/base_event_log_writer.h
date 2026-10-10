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
    TBaseEventLogWriter(TVector<std::shared_ptr<TEventLogColumn>> columns,
        ui32 maxBatchSize,
        const TDuration& flushInterval);

    const TVector<std::shared_ptr<TEventLogColumn>>& GetColumns() const {
        return Columns;
    }

    virtual bool Filter(const NActors::NStructuredLog::TLogMessage&) = 0;

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;

    void Stop() override {
        State.store(TState(TStateKind::Stop));
    }

    virtual void Flush();

    unsigned GetCurrentBatchSize() const {
        return CurrentBatchSize;
    }

    virtual bool IsAlive() const {
        return State.load().Kind != TStateKind::Stop;
    };

protected:
    virtual void CreateOrUpdateStorage() = 0;
    virtual void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    std::shared_ptr<arrow::RecordBatch> CreateCurrentBatch();

    const TVector<std::shared_ptr<TEventLogColumn>> Columns;
    const ui32 MaxBatchSize;
    const TDuration FlushInterval;
    std::shared_ptr<TDBLogMessageErrorColumn> ErrorColumn;
    std::optional<std::size_t> ErrorColumnIndex;

    enum class TStateKind : std::uint8_t {
        Started = 1,
        StorageCreating = 2,
        StorageCreateError = 3,
        Working = 4,
        Stop = 5,
    };
    struct TState {
        TStateKind Kind {TStateKind::Started};
        std::uint8_t CreateAttempCount{5};

        TState() = default;
        TState(TStateKind kind): Kind(kind) {};
    };
    std::atomic<TState> State {TState(TStateKind::Started)};
    unsigned CurrentBatchSize {0};
    bool CheckFlushActorCreate {true};
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
