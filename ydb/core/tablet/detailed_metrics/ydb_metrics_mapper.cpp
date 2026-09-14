#include "ydb_metrics_mapper.h"

#include "metric_value_aggregator.h"
#include "ydb_metrics_target_counters_base.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NKikimr {

/**
 * The implementation of the mapper from tablet/executor metrics
 * to the corresponding YDB metrics (for example, table.datashard.*).
 *
 * @tparam SimpleDesc The function, which returns the enum description for simple counters
 * @tparam CumulativeDesc The function, which returns the enum description for cumulative counters
 * @tparam PercentileDesc The function, which returns the enum description for percentile counters
 */
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
class TYdbMetricsMapperImpl
    : public TYdbMetricsMapper
    , private TYdbMetricsTargetCountersBase<
        SimpleDesc,
        CumulativeDesc,
        PercentileDesc,
        true /* ParseSourceCounters */
    > {
public:
    /**
     * The constructor, which creates all the necessary target counters.
     *
     * @param[in] targetCounterGroup The counter group where the target (mapped) counters are created
     * @param[in] sourceCounterGroup The counter group where the source counters are looked up
     */
    TYdbMetricsMapperImpl(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        NMonitoring::TDynamicCounterPtr sourceCounterGroup
    )
        : SourceCounterGroup(sourceCounterGroup)
        , SourceCountersFound(false)
    {
        // Parse SourceCountersTabletTypeName and make sure it is defined
        auto fileDesc = SimpleDesc()->file();

        Y_ABORT_UNLESS(
            fileDesc->options().HasExtension(SourceCountersTabletTypeName),
            "SourceCountersTabletTypeName is not defined"
        );

        SourceTabletTypeName = fileDesc->options().GetExtension(SourceCountersTabletTypeName);

        // Create all target simple counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsMapperImpl::TSimpleCountersOpts,
            decltype(SimpleCounters),
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
            SimpleCounters
        );

        // Create all target cumulative counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsMapperImpl::TCumulativeCountersOpts,
            decltype(CumulativeCounters),
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
            CumulativeCounters
        );

        // Create all target percentile counters
        this->template CreateTargetCountersForCounterType<
            typename TYdbMetricsMapperImpl::TPercentileCountersOpts,
            decltype(PercentileCounters),
            TYdbMetricsMapperImpl::CreateExplicitHistogram
        >(
            this->PercentileCountersOpts(),
            targetCounterGroup,
            PercentileCounters
        );
    }

    /**
     * Transfer values from the source counters to the corresponding target counters.
     */
    virtual void TransferCounterValues() override {
        // Look up all source counters (free, if it is already done successfully)
        if (!FindSourceCounters()) {
            return;
        }

        // Transfer the values for all simple counters
        TransferCounterValuesForCounterType<
            decltype(SimpleCounters),
            TAdditiveMetricAggregator
        >(SimpleCounters);

        // Transfer the values for all cumulative counters
        TransferCounterValuesForCounterType<
            decltype(CumulativeCounters),
            TAdditiveMetricAggregator
        >(CumulativeCounters);

        // Transfer the values for all percentile counters
        TransferCounterValuesForCounterType<
            decltype(PercentileCounters),
            THistogramMetricAggregator
        >(PercentileCounters);
    }

private:
    /**
     * Find all source counters for all target counters of the given counter type
     * (simple, cumulative, percentile).
     *
     * @tparam TCounterOptions The type of the parsed enum options for target counters
     * @tparam TTargetCounters The type of the container with the target counters
     * @tparam FindSourceCounter The function, which looks up the given source counter
     *
     * @param[in] counterOptions The parsed enum options for the target counters
     * @param[in] parentCounterGroups The counter groups for the all source counter categories
     * @param[in,out] targetCounters The container where the source counters will be saved
     *
     * @return Indicates if all source counters were looked up successfully
     */
    template <
        class TCounterOptions,
        class TTargetCounters,
        decltype(TTargetCounters::value_type::SourceCounters)::value_type
        (NMonitoring::TDynamicCounters::*FindSourceCounter)(
            const TString& name,
            const TString& value
        ) const
    >
    bool FindSourceCountersForCounterType(
        const TCounterOptions* counterOptions,
        const std::unordered_map<
            ESourceCounterCategory,
            NMonitoring::TDynamicCounterPtr
        >& parentCounterGroups,
        TTargetCounters& targetCounters
    ) {
        for (size_t i = 0; i < counterOptions->Size; ++i) {
            const auto& allSourceCounters = counterOptions->GetSourceCounters(i);

            targetCounters[i].SourceCounters.clear();
            targetCounters[i].SourceCounters.reserve(allSourceCounters.size());

            for (const auto& sourceCounterInfo : allSourceCounters) {
                auto counterGroupIt = parentCounterGroups.find(sourceCounterInfo.GetCategory());
                NMonitoring::TDynamicCounterPtr chosenCounterGroup;

                if (counterGroupIt != parentCounterGroups.end()) {
                    chosenCounterGroup = counterGroupIt->second;
                }

                if (!chosenCounterGroup) {
                    return false;
                }

                // WARNING: If the source application/executor group already exists,
                //          the source counter should be present. If it is not, it means
                //          that the definition for this mapped counter is likely not valid.
                //
                //          However, asserting here is not a good option, because
                //          it will crash the server. One not so obvious consequence
                //          if this assert would be the fact that adding a new high level
                //          counter mapped to a new low level counter would require
                //          two separate releases: one release to add the low level counter
                //          and another release to add the high level counter mapped to it.
                //          This is too strict of a requirement.
                //
                //          The lesser of the two evils is to ignore missing source counters
                //          completely. If this is caused by a configuration error
                //          (for example, a mistake in the source counter name),
                //          this will not be detected at all and the corresponding
                //          target counter will always be zero. If this caused by a version
                //          mismatch, it will be fixed when the process is restarted.
                //
                //          The only way to catch configuration errors is to verify each
                //          counter explicitly in unit tests.
                auto sourceCounter = (chosenCounterGroup.Get()->*FindSourceCounter)(
                    "sensor", // Low level counters use "sensor" instead of "name"!
                    sourceCounterInfo.GetName()
                );

                if (!sourceCounter) {
                    // NOTE: This will ignore this error silently. SourceCountersFound will be set
                    //       to true and this source counter will never be looked up again.
                    continue;
                }

                targetCounters[i].SourceCounters.push_back(sourceCounter);
            }
        }

        return true;
    }

    /**
     * Transfer values from the source counters to the corresponding target counters
     * for the given counter type (simple, cumulative, percentile).
     *
     * @tparam TTargetCounters The type of the container with the target counters
     * @tparam TMetricValueAggregator The aggregator for individual metric values
     *
     * @param[in] targetCounters The container for the source and target counters
     */
    template <
        class TTargetCounters,
        class TMetricValueAggregator
    >
    void TransferCounterValuesForCounterType(const TTargetCounters& targetCounters) {
        for (const auto& counter : targetCounters) {
            // NOTE: The destructor will update the target counter value
            TMetricValueAggregator aggregator(counter.TargetCounter);

            for (const auto& sourceCounter : counter.SourceCounters) {
                aggregator.AggregateValue(sourceCounter);
            }
        }
    }

    /**
     * Look up the source counters for all mapped counters.
     *
     * @return Indicates if all source counters were looked up successfully
     */
    bool FindSourceCounters() {
        // Do not repeat the look up, if it was already done successfully
        if (SourceCountersFound) {
            return true;
        }

        // Look up the main counter group for the source tablet itself
        //
        // NOTE: If any of the parent groups for the source counter does not exist,
        //       it means that the corresponding counters have not been received
        //       and processed yet. If this happens, the target counters will be created,
        //       but will not be updated and will have zero values.
        auto tabletCountersGroup = SourceCounterGroup->FindSubgroup("type", SourceTabletTypeName);

        if (!tabletCountersGroup) {
            return false;
        }

        // Look up the counter groups for all application and executor counters
        const std::unordered_map<
            ESourceCounterCategory,
            NMonitoring::TDynamicCounterPtr
        > parentCounterGroups = {
            {
                ESourceCounterCategory::SCC_TABLET,
                tabletCountersGroup->FindSubgroup("category", "app"),
            },
            {
                ESourceCounterCategory::SCC_EXECUTOR,
                tabletCountersGroup->FindSubgroup("category", "executor"),
            },
        };

        // Look up source counters for all simple counters
        //
        // WARNING: The code here uses FindNamedCounter() and FindNamedHistogram()
        //          for looking up source counters. These functions return NULL,
        //          if the given counter does not exist. The code in
        //          FindSourceCountersForCounterType() handles this situation as follows.
        //          If the parent group does not exist, then the assumption is that
        //          the corresponding source counters have not been populated yet,
        //          so any errors for missing individual source counters are ignored,
        //          and all target counters are not mapped (until the parent group appears).
        //          However, if all parent groups exist, but the given source counter
        //          does not exist, the code will assume that this is a configuration
        //          error and will skip this particular source counter. This check
        //          is done only once, so only after a restart the code try to look up
        //          this source counter again.
        //
        //          Unfortunately, there is no good way to deal with this gracefully.
        //          Using GetNamedCounter() and GetNamedHistogram() may seem like
        //          a solution (these functions automatically create the counter,
        //          if it does not exist), but it is not. The problem is that
        //          the code in this class does not know what the source counter
        //          should be - for regular counters it is the GAUGE vs RATE indicator
        //          and for histograms it is the set of boundaries. Only the code,
        //          which provides the source counter knows these parameters.
        //          If this class creates the missing source counter/histogram,
        //          it will use different parameters and the corresponding metric
        //          values will be distorted.
        if (!FindSourceCountersForCounterType<
                typename TYdbMetricsMapperImpl::TSimpleCountersOpts,
                decltype(SimpleCounters),
                &NMonitoring::TDynamicCounters::FindNamedCounter
            >(
                this->SimpleCountersOpts(),
                parentCounterGroups,
                SimpleCounters
            )
        ) {
            return false;
        }

        // Look up source counters for all cumulative counters
        if (!FindSourceCountersForCounterType<
                typename TYdbMetricsMapperImpl::TCumulativeCountersOpts,
                decltype(CumulativeCounters),
                &NMonitoring::TDynamicCounters::FindNamedCounter
            >(
                this->CumulativeCountersOpts(),
                parentCounterGroups,
                CumulativeCounters
            )
        ) {
            return false;
        }

        // Look up source counters for all percentile counters
        if (!FindSourceCountersForCounterType<
                typename TYdbMetricsMapperImpl::TPercentileCountersOpts,
                decltype(PercentileCounters),
                &NMonitoring::TDynamicCounters::FindNamedHistogram
            >(
                this->PercentileCountersOpts(),
                parentCounterGroups,
                PercentileCounters
            )
        ) {
            return false;
        }

        // All source counters are looked up successfully, do not do it again
        SourceCountersFound = true;
        return true;
    }

    /**
     * The holder for the target counter and the associated source counters
     * for additive counters (simple and cumulative).
     */
    struct TMappedCounter {
        NMonitoring::TDynamicCounters::TCounterPtr TargetCounter;
        std::vector<NMonitoring::TDynamicCounters::TCounterPtr> SourceCounters;
    };

    /**
     * The holder for the target counter and the associated source counters
     * for histogram counters (percentile).
     */
    struct TMappedHistogram {
        NMonitoring::THistogramPtr TargetCounter;
        std::vector<NMonitoring::THistogramPtr> SourceCounters;
    };

    /**
     * The name of the tablet type, which is used to look up source counters
     * (for the "type" label).
     */
    TString SourceTabletTypeName;

    /**
     * The counter group where the source counters are looked up,
     */
    NMonitoring::TDynamicCounterPtr SourceCounterGroup;

    /**
     * Indicates if the last call to FindSourceCounters() was successful
     * and all source counters were looked up.
     */
    bool SourceCountersFound;

    /**
     * The list of source-target counter pairs used for mapping simple counters.
     *
     * @note The size matches the number of simple counters from TSimpleCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TMappedCounter> SimpleCounters;

    /**
     * The list of source-target counter pairs used for mapping cumulative counters.
     *
     * @note The size matches the number of cumulative counters from TCumulativeCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TMappedCounter> CumulativeCounters;

    /**
     * The list of source-target counter pairs used for mapping percentile counters.
     *
     * @note The size matches the number of percentile counters from TPercentileCountersOpts,
     *       with the order being the same as well.
     */
    std::vector<TMappedHistogram> PercentileCounters;
};

TYdbMetricsMapperPtr CreateYdbMetricsMapperByTabletType(
    TTabletTypes::EType tabletType,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    NMonitoring::TDynamicCounterPtr sourceCounterGroup
) {
    switch (tabletType) {
    case TTabletTypes::DataShard:
        return MakeIntrusive<TYdbMetricsMapperImpl<
            NDataShard::ESimpleDetailedCounters_descriptor,
            NDataShard::ECumulativeDetailedCounters_descriptor,
            NDataShard::EPercentileDetailedCounters_descriptor
        >>(
           targetCounterGroup,
           sourceCounterGroup
        );

    default:
        Y_ABORT("Unsupported tablet type %s", TTabletTypes::TypeToStr(tabletType));
    }
}

} // namespace NKikimr
