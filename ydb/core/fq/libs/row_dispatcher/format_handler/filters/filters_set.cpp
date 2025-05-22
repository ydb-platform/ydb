#include "filters_set.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq::NRowDispatcher {

namespace {

class TTopicFilters : public ITopicFilters {
    struct TCounters {
        const NMonitoring::TDynamicCounterPtr Counters;

        NMonitoring::TDynamicCounters::TCounterPtr ActiveFilters;
        NMonitoring::TDynamicCounters::TCounterPtr InFlightCompileRequests;
        NMonitoring::TDynamicCounters::TCounterPtr CompileErrors;

        explicit TCounters(NMonitoring::TDynamicCounterPtr counters)
            : Counters(counters)
        {
            Register();
        }

    private:
        void Register() {
            ActiveFilters = Counters->GetCounter("ActiveFilters", false);
            InFlightCompileRequests = Counters->GetCounter("InFlightCompileRequests", false);
            CompileErrors = Counters->GetCounter("CompileErrors", true);
        }
    };

    struct TStats {
        void AddFilterLatency(TDuration filterLatency) {
            FilterLatency = std::max(FilterLatency, filterLatency);
        }

        void Clear() {
            FilterLatency = TDuration::Zero();
        }

        TDuration FilterLatency;
    };

    class TFilterHandler {
    public:
        TFilterHandler(TTopicFilters& self, IFilteredDataConsumer::TPtr consumer, IPurecalcFilter::TPtr purecalcFilter)
            : Self(self)
            , Consumer(std::move(consumer))
            , PurecalcFilter(std::move(purecalcFilter))
            , LogPrefix(TStringBuilder() << Self.LogPrefix << "TFilterHandler " << Consumer->GetFilterId() << " : ")
        {}

        ~TFilterHandler() {
            if (InFlightCompilationId) {
                Self.Counters.InFlightCompileRequests->Dec();
            } else if (FilterStarted) {
                Self.Counters.ActiveFilters->Dec();
            }
        }

        IFilteredDataConsumer::TPtr GetConsumer() const {
            return Consumer;
        }

        IPurecalcFilter::TPtr GetPurecalcFilter() const {
            return PurecalcFilter;
        }

        bool IsStarted() const {
            return FilterStarted;
        }

        void CompileFilter() {
            if (!PurecalcFilter) {
                StartFilter();
                return;
            }

            InFlightCompilationId = Self.FreeCompileId++;
            Self.Counters.InFlightCompileRequests->Inc();
            Y_ENSURE(Self.InFlightCompilations.emplace(InFlightCompilationId, Consumer->GetFilterId()).second, "Got duplicated compilation event id");

            LOG_ROW_DISPATCHER_TRACE("Send compile request with id " << InFlightCompilationId);
            NActors::TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(Self.Config.CompileServiceId, Self.Owner, PurecalcFilter->GetCompileRequest().release(), 0, InFlightCompilationId));
        }

        void AbortCompilation() {
            if (!InFlightCompilationId) {
                return;
            }

            LOG_ROW_DISPATCHER_TRACE("Send abort compile request with id " << InFlightCompilationId);
            NActors::TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(Self.Config.CompileServiceId, Self.Owner, new TEvRowDispatcher::TEvPurecalcCompileAbort(), 0, InFlightCompilationId));

            InFlightCompilationId = 0;
            Self.Counters.InFlightCompileRequests->Dec();
        }

        void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) {
            if (ev->Cookie != InFlightCompilationId) {
                LOG_ROW_DISPATCHER_DEBUG("Outdated compiler response ignored for id " << ev->Cookie << ", current compile id " << InFlightCompilationId);
                return;
            }

            Y_ENSURE(InFlightCompilationId, "Unexpected compilation response");
            InFlightCompilationId = 0;
            Self.Counters.InFlightCompileRequests->Dec();

            if (!ev->Get()->ProgramHolder) {
                auto status = TStatus::Fail(ev->Get()->Status, std::move(ev->Get()->Issues));
                LOG_ROW_DISPATCHER_ERROR("Filter compilation error: " << status.GetErrorMessage());

                Self.Counters.CompileErrors->Inc();
                Consumer->OnFilteringError(status.AddParentIssue("Failed to compile client filter"));
                return;
            }

            Y_ENSURE(PurecalcFilter, "Unexpected compilation response for client without filter");
            PurecalcFilter->OnCompileResponse(std::move(ev));

            LOG_ROW_DISPATCHER_TRACE("Filter compilation finished");
            StartFilter();
        }

    private:
        void StartFilter() {
            FilterStarted = true;
            Self.Counters.ActiveFilters->Inc();
            Consumer->OnFilterStarted();
        }

    private:
        TTopicFilters& Self;
        const IFilteredDataConsumer::TPtr Consumer;
        const IPurecalcFilter::TPtr PurecalcFilter;
        const TString LogPrefix;

        ui64 InFlightCompilationId = 0;
        bool FilterStarted = false;
    };

public:
    explicit TTopicFilters(NActors::TActorId owner, const TTopicFiltersConfig& config, NMonitoring::TDynamicCounterPtr counters)
        : Config(config)
        , Owner(owner)
        , LogPrefix("TTopicFilters: ")
        , Counters(counters)
    {}

    ~TTopicFilters() {
        Filters.clear();
    }

public:
    void FilterData(const TVector<ui64>& columnIndex, const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) override {
        LOG_ROW_DISPATCHER_TRACE("FilterData for " << Filters.size() << " clients, number rows: " << numberRows);

        const TInstant startFilter = TInstant::Now();
        for (const auto& [_, filterHandler] : Filters) {
            const auto consumer = filterHandler.GetConsumer();
            if (const auto nextOffset = consumer->GetNextMessageOffset(); !numberRows || (nextOffset && offsets[numberRows - 1] < *nextOffset)) {
                LOG_ROW_DISPATCHER_TRACE("Ignore filtering for " << consumer->GetFilterId() << ", historical offset");
                continue;
            }
            if (!filterHandler.IsStarted()) {
                LOG_ROW_DISPATCHER_TRACE("Ignore filtering for " << consumer->GetFilterId() << ", client filter is not compiled");
                continue;
            }

            PushToFilter(filterHandler, offsets, columnIndex, values, numberRows);
        }
        Stats.AddFilterLatency(TInstant::Now() - startFilter);
    }

    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) override {
        LOG_ROW_DISPATCHER_TRACE("Got compile response for request with id " << ev->Cookie);

        const auto requestIt = InFlightCompilations.find(ev->Cookie);
        if (requestIt == InFlightCompilations.end()) {
            LOG_ROW_DISPATCHER_DEBUG("Compile response ignored for id " << ev->Cookie);
            return;
        }

        const auto filterId = requestIt->second;
        InFlightCompilations.erase(requestIt);

        const auto filterIt = Filters.find(filterId);
        if (filterIt == Filters.end()) {
            LOG_ROW_DISPATCHER_DEBUG("Compile response ignored for id " << ev->Cookie << ", filter with id " << filterId << " not found");
            return;
        }

        filterIt->second.OnCompileResponse(std::move(ev));
    }

    TStatus AddFilter(IFilteredDataConsumer::TPtr filter) override {
        LOG_ROW_DISPATCHER_TRACE("Create filter with id " << filter->GetFilterId());

        IPurecalcFilter::TPtr purecalcFilter;
        if (const auto& predicate = filter->GetWhereFilter()) {
            LOG_ROW_DISPATCHER_TRACE("Create purecalc filter for predicate '" << predicate << "' (filter id: " << filter->GetFilterId() << ")");

            auto filterStatus = CreatePurecalcFilter(filter);
            if (filterStatus.IsFail()) {
                return filterStatus;
            }
            purecalcFilter = filterStatus.DetachResult();
        }

        const auto [it, inserted] = Filters.insert({filter->GetFilterId(), TFilterHandler(*this, filter, std::move(purecalcFilter))});
        if (!inserted) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to create new filter, filter with id " << filter->GetFilterId() << " already exists");
        }

        it->second.CompileFilter();
        return TStatus::Success();
    }

    void RemoveFilter(NActors::TActorId filterId) override {
        LOG_ROW_DISPATCHER_TRACE("Remove filter with id " << filterId);

        const auto it = Filters.find(filterId);
        if (it == Filters.end()) {
            return;
        }

        it->second.AbortCompilation();
        Filters.erase(it);
    }

    TFiltersStatistic GetStatistics() override {
        TFiltersStatistic statistics;
        statistics.FilterLatency = Stats.FilterLatency;
        Stats.Clear();
    
        return statistics;
    }

private:
    void PushToFilter(const TFilterHandler& filterHandler, const TVector<ui64>& /* offsets */, const TVector<ui64>& columnIndex, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) {
        const auto consumer = filterHandler.GetConsumer();
        const auto& columnIds = consumer->GetColumnIds();

        TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result;
        result.reserve(columnIds.size());
        for (ui64 columnId : columnIds) {
            Y_ENSURE(columnId < columnIndex.size(), "Unexpected column id " << columnId << ", it is larger than index array size " << columnIndex.size());
            const ui64 index = columnIndex[columnId];

            Y_ENSURE(index < values.size(), "Unexpected column index " << index << ", it is larger than values array size " << values.size());
            if (const auto value = values[index]) {
                result.emplace_back(value);
            } else {
                LOG_ROW_DISPATCHER_TRACE("Ignore filtering for " << consumer->GetFilterId() << ", client got parsing error for column " << columnId);
                return;
            }
        }

        if (const auto filter = filterHandler.GetPurecalcFilter()) {
            LOG_ROW_DISPATCHER_TRACE("Pass " << numberRows << " rows to purecalc filter (filter id: " << consumer->GetFilterId() << ")");
            filter->FilterData(result, numberRows);
        } else if (numberRows) {
            LOG_ROW_DISPATCHER_TRACE("Add " << numberRows << " rows to client " << consumer->GetFilterId() << " without filtering");
            consumer->OnFilteredBatch(0, numberRows - 1);
        }
    }

private:
    const TTopicFiltersConfig Config;
    const NActors::TActorId Owner;
    const TString LogPrefix;

    ui64 FreeCompileId = 1;  // 0 <=> compilation is not started
    std::unordered_map<ui64, NActors::TActorId> InFlightCompilations;
    std::unordered_map<NActors::TActorId, TFilterHandler> Filters;

    // Metrics
    const TCounters Counters;
    TStats Stats;
};

}  // anonymous namespace

ITopicFilters::TPtr CreateTopicFilters(NActors::TActorId owner, const TTopicFiltersConfig& config, NMonitoring::TDynamicCounterPtr counters) {
    return MakeIntrusive<TTopicFilters>(owner, config, counters);
}

}  // namespace NFq::NRowDispatcher
