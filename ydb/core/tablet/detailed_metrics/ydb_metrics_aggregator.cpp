#include "ydb_metrics_aggregator.h"

#include "metric_value_aggregator.h"
#include "ydb_metrics_target_counters_base.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NKikimr {

/**
 * The implementation of the aggregator for the YDB metrics
 * (for example, table.datashard.*), which combines the same metrics
 * from different sources into a single value.
 *
 * @tparam SimpleDesc The function, which returns the enum description for simple counters
 * @tparam CumulativeDesc The function, which returns the enum description for cumulative counters
 * @tparam PercentileDesc The function, which returns the enum description for percentile counters
 */
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
class TYdbMetricsAggregatorImpl
    : public TYdbMetricsAggregator
    , private TYdbMetricsTargetCountersBase<
        SimpleDesc,
        CumulativeDesc,
        PercentileDesc,
        false /* ParseSourceCounters */
    > {
public:
    /**
     * The constructor, which creates all the necessary target counters.
     *
     * @param[in] targetCounterGroup The counter group where the target (aggregated) counters are created
     */
    TYdbMetricsAggregatorImpl(NMonitoring::TDynamicCounterPtr targetCounterGroup) {
        // Create all target simple counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TSimpleCountersOpts,
            decltype(TargetSimpleCounters),
            [](auto /* counterOptions */, auto targetCounterGroup, auto /* index */, auto name) {
                return targetCounterGroup->GetNamedCounter(
                    "name",
                    name,
                    false /* derivative */
                );
            }
        >(
            this->SimpleCountersOpts(),
            targetCounterGroup,
            TargetSimpleCounters
        );

        // Create all target cumulative counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TCumulativeCountersOpts,
            decltype(TargetCumulativeCounters),
            [](auto /* counterOptions */, auto targetCounterGroup, auto /* index */, auto name) {
                return targetCounterGroup->GetNamedCounter(
                    "name",
                    name,
                    true /* derivative */
                );
            }
        >(
            this->CumulativeCountersOpts(),
            targetCounterGroup,
            TargetCumulativeCounters
        );

        // Create all target percentile counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TPercentileCountersOpts,
            decltype(TargetPercentileCounters),
            TYdbMetricsAggregatorImpl::CreateExplicitHistogram
        >(
            this->PercentileCountersOpts(),
            targetCounterGroup,
            TargetPercentileCounters
        );
    }

    virtual void AddSourceCountersGroup(
        const TString& sourceGroupId,
        NMonitoring::TDynamicCounterPtr sourceCounterGroup
    ) override {
        const auto result = SourceCounterGroups.try_emplace(sourceGroupId);

        Y_ABORT_UNLESS(
            result.second,
            "The source counter group %s already exists",
            sourceGroupId.c_str()
        );

        // Look up all simple counters for the given source group
        FindSourceCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TSimpleCountersOpts,
            decltype(result.first->second.SimpleCounters),
            &NMonitoring::TDynamicCounters::FindNamedCounter
        >(
            sourceGroupId,
            this->SimpleCountersOpts(),
            sourceCounterGroup,
            result.first->second.SimpleCounters
        );

        // Look up all cumulative counters for the given source group
        FindSourceCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TCumulativeCountersOpts,
            decltype(result.first->second.CumulativeCounters),
            &NMonitoring::TDynamicCounters::FindNamedCounter
        >(
            sourceGroupId,
            this->CumulativeCountersOpts(),
            sourceCounterGroup,
            result.first->second.CumulativeCounters
        );

        // Look up all percentile counters for the given source group
        FindSourceCountersForCounterType<
            typename TYdbMetricsAggregatorImpl::TPercentileCountersOpts,
            decltype(result.first->second.PercentileCounters),
            &NMonitoring::TDynamicCounters::FindNamedHistogram
        >(
            sourceGroupId,
            this->PercentileCountersOpts(),
            sourceCounterGroup,
            result.first->second.PercentileCounters
        );
    }

    virtual void RemoveSourceCountersGroup(const TString& sourceGroupId) override {
        const auto result = SourceCounterGroups.erase(sourceGroupId);

        Y_ABORT_UNLESS(
            result != 0,
            "The source counter group %s does not exist",
            sourceGroupId.c_str()
        );
    }

    virtual void RecalculateAllTargetCounters() override {
        // Recalculate the values of all simple counters
        AggregateCounterValuesForCounterType<
            decltype(TargetSimpleCounters),
            decltype(TGroupSourceCounters::SimpleCounters),
            &TGroupSourceCounters::SimpleCounters,
            TAdditiveMetricAggregator
        >(TargetSimpleCounters);

        // Recalculate the values of all cumulative counters
        AggregateCounterValuesForCounterType<
            decltype(TargetCumulativeCounters),
            decltype(TGroupSourceCounters::CumulativeCounters),
            &TGroupSourceCounters::CumulativeCounters,
            TAdditiveMetricAggregator
        >(TargetCumulativeCounters);

        // Recalculate the values of all percentile counters
        AggregateCounterValuesForCounterType<
            decltype(TargetPercentileCounters),
            decltype(TGroupSourceCounters::PercentileCounters),
            &TGroupSourceCounters::PercentileCounters,
            THistogramMetricAggregator
        >(TargetPercentileCounters);
    }

private:
    /**
     * The holder for all source counters for the given source group.
     */
    struct TGroupSourceCounters {
        /**
         * The list of source counters used for aggregating simple counters
         * from the given source group.
         *
         * @note The size matches the number of simple counters from TSimpleCountersOpts,
         *       with the order being the same as well.
         */
        std::vector<NMonitoring::TDynamicCounters::TCounterPtr> SimpleCounters;

        /**
         * The list of source counters used for aggregating cumulative counters
         * from the given source group.
         *
         * @note The size matches the number of cumulative counters from TCumulativeCountersOpts,
         *       with the order being the same as well.
         */
        std::vector<NMonitoring::TDynamicCounters::TCounterPtr> CumulativeCounters;

        /**
         * The list of source counters used for aggregating percentile counters
         * from the given source group.
         *
         * @note The size matches the number of percentile counters from TPercentileCountersOpts,
         *       with the order being the same as well.
         */
        std::vector<NMonitoring::THistogramPtr> PercentileCounters;
    };

    /**
     * Aggregate values from the source counters to the corresponding target counters
     * for the given counter type (simple, cumulative, percentile).
     *
     * @tparam TTargetCounters The type of the container with the target counters
     * @tparam TSourceCounters The type of the container with the source counters
     * @tparam SourceCountersField The field within TGroupSourceCounters, which holds source counters
     * @tparam TMetricValueAggregator The aggregator for individual metric values
     *
     * @param[in] targetCounters The container for the target counters
     */
    template <
        class TTargetCounters,
        class TSourceCounters,
        TSourceCounters TGroupSourceCounters::*SourceCountersField,
        class TMetricValueAggregator
    >
    void AggregateCounterValuesForCounterType(const TTargetCounters& targetCounters) {
        size_t index = 0;

        for (const auto& counter : targetCounters) {
            // NOTE: The destructor will update the target counter value
            TMetricValueAggregator aggregator(counter.TargetCounter);

            for (const auto& [sourceGroupId, sourceCounters] : SourceCounterGroups) {
                // NOTE: The order of all source/target counters always matches
                //       the order, in which the counters are defined in the corresponding
                //       .proto file. Thus, it is safe to use an index from the target counters
                //       to access the corresponding source counter.
                aggregator.AggregateValue((sourceCounters.*SourceCountersField)[index]);
            }

            ++index;
        }
    }

    /**
     * Find all source counters of the given counter type (simple, cumulative, percentile).
     *
     * @tparam TCounterOptions The type of the parsed enum options for source counters
     * @tparam TSourceCounters The type of the container with the source counters
     * @tparam FindSourceCounter The function, which looks up the given source counter
     *
     * @param[in] sourceGroupId The ID of the corresponding source group
     * @param[in] counterOptions The parsed enum options for the source counters
     * @param[in] sourceCounterGroup The counter group where the source counters are looked up
     * @param[in,out] sourceCounters The container where the source counters will be saved
     */
    template <
        class TCounterOptions,
        class TSourceCounters,
        TSourceCounters::value_type
        (NMonitoring::TDynamicCounters::*FindSourceCounter)(
            const TString& name,
            const TString& value
        ) const
    >
    void FindSourceCountersForCounterType(
        const TString& sourceGroupId,
        const TCounterOptions* counterOptions,
        NMonitoring::TDynamicCounterPtr sourceCounterGroup,
        TSourceCounters& sourceCounters
    ) {
        sourceCounters.clear();
        sourceCounters.reserve(counterOptions->Size);

        for (size_t i = 0; i < counterOptions->Size; ++i) {
            const char* counterName = counterOptions->GetNames()[i];

            auto sourceCounter = (sourceCounterGroup.Get()->*FindSourceCounter)(
                "name",
                counterName
            );

            Y_ABORT_UNLESS(
                sourceCounter,
                "The source counter %s does not exist (source group ID %s)",
                counterName,
                sourceGroupId.c_str()
            );

            sourceCounters.push_back(sourceCounter);
        }
    }

    /**
     * The holder for the target counter for additive counters (simple and cumulative).
     */
    struct TAggregatedCounter {
        NMonitoring::TDynamicCounters::TCounterPtr TargetCounter;
    };

    /**
     * The holder for the target counter for histogram counters (percentile).
     */
    struct TAggregatedHistogram {
        NMonitoring::THistogramPtr TargetCounter;
    };

    /**
     * The list of target counters used for aggregating simple counters.
     *
     * @note The size matches the number of simple counters from TSimpleCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TAggregatedCounter> TargetSimpleCounters;

    /**
     * The list of target counter used for aggregating cumulative counters.
     *
     * @note The size matches the number of cumulative counters from TCumulativeCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TAggregatedCounter> TargetCumulativeCounters;

    /**
     * The list of target counters used for aggregating percentile counters.
     *
     * @note The size matches the number of percentile counters from TPercentileCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TAggregatedHistogram> TargetPercentileCounters;

    /**
     * The source counters for all source groups, which are used as the source
     * for the values of the given set of target counters.
     *
     * @note The counters are keyed by the source group ID.
     */
    std::unordered_map<TString, TGroupSourceCounters> SourceCounterGroups;
};

TYdbMetricsAggregatorPtr CreateYdbMetricsAggregatorByTabletType(
    TTabletTypes::EType tabletType,
    NMonitoring::TDynamicCounterPtr targetCounterGroup
) {
    switch (tabletType) {
    case TTabletTypes::DataShard:
        return MakeIntrusive<TYdbMetricsAggregatorImpl<
            NDataShard::ESimpleDetailedCounters_descriptor,
            NDataShard::ECumulativeDetailedCounters_descriptor,
            NDataShard::EPercentileDetailedCounters_descriptor
        >>(targetCounterGroup);

    default:
        Y_ABORT("Unsupported tablet type %s", TTabletTypes::TypeToStr(tabletType));
    }
}

} // namespace NKikimr
