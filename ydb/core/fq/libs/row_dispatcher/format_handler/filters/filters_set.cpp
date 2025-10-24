#include "filters_set.h"

#include "purecalc_filter.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq::NRowDispatcher {

namespace {

class TTopicFilters : public ITopicFilters {
    class TStats {
    public:
        void AddFilterLatency(TDuration filterLatency) {
            FilterLatency_ = std::max(FilterLatency_, filterLatency);
        }

        void FillStatistics(TFiltersStatistic& statistics) {
            statistics.FilterLatency = std::exchange(FilterLatency_, TDuration::Zero());
        }

    private:
        TDuration FilterLatency_;
    };

public:
    TTopicFilters(NActors::TActorId owner, TTopicFiltersConfig config, NMonitoring::TDynamicCounterPtr counters)
        : Owner_(owner)
        , Config_(std::move(config))
        , Counters_(std::move(counters))
    {}

    void ProcessData(const TVector<ui64>& columnIndex, const TVector<ui64>& offsets, const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows) override {
        LOG_ROW_DISPATCHER_TRACE("ProcessData for " << RunHandlers_.size() << " clients, number rows: " << numberRows);

        if (!numberRows) {
            return;
        }

        const TInstant startProgram = TInstant::Now();
        for (const auto& [_, runHandler] : RunHandlers_) {
            const auto consumer = runHandler->GetConsumer();
            if (!consumer->IsStarted()) {
                continue;
            }
            if (const auto nextOffset = consumer->GetNextMessageOffset(); nextOffset && offsets[numberRows - 1] < *nextOffset) {
                LOG_ROW_DISPATCHER_TRACE("Ignore processing for " << consumer->GetClientId() << ", historical offset");
                continue;
            }

            PushToRunner(runHandler, offsets, columnIndex, values, numberRows);
        }
        Stats_.AddFilterLatency(TInstant::Now() - startProgram);
    }

    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) override {
        LOG_ROW_DISPATCHER_TRACE("Got compile response for request with id " << ev->Cookie);

        auto compileHandlerStatus = RemoveCompileProgram(ev->Cookie);
        if (compileHandlerStatus.IsFail()) {
            LOG_ROW_DISPATCHER_ERROR(compileHandlerStatus.GetError().GetErrorMessage());
            return;
        }
        const auto compileHandler = compileHandlerStatus.DetachResult();

        if (!ev->Get()->ProgramHolder) {
            compileHandler->OnCompileError(ev);
            return;
        }
        compileHandler->OnCompileResponse(ev);

        auto runHandlerStatus = AddRunProgram(compileHandler->GetConsumer(), compileHandler->GetProgram());
        if (runHandlerStatus.IsFail()) {
            LOG_ROW_DISPATCHER_ERROR(runHandlerStatus.GetError().GetErrorMessage());
            return;
        }

        StartProgram(compileHandler->GetConsumer());
    }

    TStatus AddPrograms(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) override {
        auto status = AddProgram(consumer, std::move(programHolder));
        if (!status) {
            RemoveProgram(consumer->GetClientId());
            return status;
        }

        StartProgram(consumer);

        return TStatus::Success();
    }

    void RemoveProgram(NActors::TActorId clientId) override {
        LOG_ROW_DISPATCHER_TRACE("Remove program with client id " << clientId);

        AbortCompileProgram(clientId);
        RemoveRunProgram(clientId);
    }

    void FillStatistics(TFiltersStatistic& statistics) override {
        Stats_.FillStatistics(statistics);
    }

private:
    void StartProgram(IProcessedDataConsumer::TPtr consumer) {
        if (CompileHandlers_.contains(consumer->GetClientId())) {
            return;
        }

        LOG_ROW_DISPATCHER_TRACE("Start program with client id " << consumer->GetClientId());

        consumer->OnStart();
    }

    TStatus AddProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) {
        LOG_ROW_DISPATCHER_TRACE("Create program with client id " << consumer->GetClientId());

        if (!programHolder) {
            auto runHandlerStatus = AddRunProgram(std::move(consumer), std::move(programHolder));
            return runHandlerStatus.IsSuccess() ? TStatus::Success() : runHandlerStatus.GetError();
        }

        LOG_ROW_DISPATCHER_TRACE("Create purecalc program for query '" << programHolder->GetQuery() << "' (client id: " << consumer->GetClientId() << ")");

        const auto cookie = NextCookie_++;
        auto compileHandlerStatus = AddCompileProgram(std::move(consumer), std::move(programHolder), cookie);
        if (compileHandlerStatus.IsFail()) {
            return compileHandlerStatus;
        }
        const auto compileHandler = compileHandlerStatus.DetachResult();
        compileHandler->Compile();

        return TStatus::Success();
    }

    TValueStatus<IProgramCompileHandler::TPtr> AddCompileProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder, ui64 cookie) {
        const auto clientId = consumer->GetClientId();

        const auto [inflightIter, inflightInserted] = InFlightCompilations_.emplace(cookie, clientId);
        if (!inflightInserted) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, "Got duplicated compilation event id");
        }

        auto compileHandler = CreateProgramCompileHandler(std::move(consumer), std::move(programHolder), cookie, Config_.CompileServiceId, Owner_, Counters_);
        auto [iter, inserted] = CompileHandlers_.emplace(clientId, std::move(compileHandler));
        if (!inserted) {
            InFlightCompilations_.erase(inflightIter);
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to compile new program, program with client id " << clientId << " already exists");
        }
        return iter->second;
    }

    void AbortCompileProgram(NActors::TActorId clientId) {
        const auto iter = CompileHandlers_.find(clientId);
        if (iter == CompileHandlers_.end()) {
            return;
        }

        iter->second->AbortCompilation();

        const auto cookie = iter->second->GetCookie();
        InFlightCompilations_.erase(cookie);

        CompileHandlers_.erase(iter);
    }

    TValueStatus<IProgramCompileHandler::TPtr> RemoveCompileProgram(ui64 cookie) {
        const auto requestIter = InFlightCompilations_.find(cookie);
        if (requestIter == InFlightCompilations_.end()) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Compile response ignored for id " << cookie);
        }
        const auto clientId = requestIter->second;
        InFlightCompilations_.erase(requestIter);

        const auto iter = CompileHandlers_.find(clientId);
        if (iter == CompileHandlers_.end()) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Compile response ignored for id " << cookie << ", program with client id " << clientId << " not found");
        }
        const auto result = iter->second;
        CompileHandlers_.erase(iter);

        return result;
    }

    TValueStatus<IProgramRunHandler::TPtr> AddRunProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) {
        const auto clientId = consumer->GetClientId();

        auto runHandler = CreateProgramRunHandler(std::move(consumer), std::move(programHolder), Counters_);
        const auto [iter, inserted] = RunHandlers_.emplace(clientId, std::move(runHandler));
        if (!inserted) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to run new program, program with client id " << clientId << " already exists");
        }
        return iter->second;
    }

    void RemoveRunProgram(NActors::TActorId clientId) {
        const auto iter = RunHandlers_.find(clientId);
        if (iter == RunHandlers_.end()) {
            return;
        }
        RunHandlers_.erase(iter);
    }

    void PushToRunner(IProgramRunHandler::TPtr programRunHandler, const TVector<ui64>& /* offsets */, const TVector<ui64>& columnIndex, const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows) {
        const auto consumer = programRunHandler->GetConsumer();
        const auto& columnIds = consumer->GetColumnIds();

        TVector<std::span<NYql::NUdf::TUnboxedValue>> result;
        result.reserve(columnIds.size());
        for (ui64 columnId : columnIds) {
            Y_ENSURE(columnId < columnIndex.size(), "Unexpected column id " << columnId << ", it is larger than index array size " << columnIndex.size());
            const ui64 index = columnIndex[columnId];

            Y_ENSURE(index < values.size(), "Unexpected column index " << index << ", it is larger than values array size " << values.size());
            if (const auto value = values[index]; !value.empty()) {
                result.emplace_back(value);
            } else {
                LOG_ROW_DISPATCHER_TRACE("Ignore processing for " << consumer->GetClientId() << ", client got parsing error for column " << columnId);
                return;
            }
        }

        LOG_ROW_DISPATCHER_TRACE("Pass " << numberRows << " rows to purecalc filter (client id: " << consumer->GetClientId() << ")");
        programRunHandler->ProcessData(result, numberRows);
    }

private:
    // NOLINTNEXTLINE(readability-identifier-naming)
    TStringBuf LogPrefix = "TTopicFilters: ";
    NActors::TActorId Owner_;
    TTopicFiltersConfig Config_;

    ui64 NextCookie_ = 1;  // 0 <=> compilation is not started
    std::unordered_map<ui64, NActors::TActorId> InFlightCompilations_;
    std::unordered_map<NActors::TActorId, IProgramCompileHandler::TPtr> CompileHandlers_;
    std::unordered_map<NActors::TActorId, IProgramRunHandler::TPtr> RunHandlers_;

    // Metrics
    NMonitoring::TDynamicCounterPtr Counters_;
    TStats Stats_;
};

} // anonymous namespace

ITopicFilters::TPtr CreateTopicFilters(NActors::TActorId owner, TTopicFiltersConfig config, NMonitoring::TDynamicCounterPtr counters) {
    return MakeIntrusive<TTopicFilters>(owner, std::move(config), std::move(counters));
}

}  // namespace NFq::NRowDispatcher
