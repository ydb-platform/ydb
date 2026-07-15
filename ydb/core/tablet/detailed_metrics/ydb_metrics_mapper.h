#pragma once

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

/**
 * The mapper from tablet/executor metrics to the corresponding YDB metrics
 * (for example, table.datashard.*).
 */
class TYdbMetricsMapper : public TThrRefBase {
public:
    /**
     * Transfer values from the source counters to the corresponding target counters.
     */
    virtual void TransferCounterValues() = 0;
};

using TYdbMetricsMapperPtr = TIntrusivePtr<TYdbMetricsMapper>;

/**
 * Create an instance of the TYdbMetricsMapper for metrics for the given tablet type.
 *
 * @note The target counters will be created in the given target group immediately.
 *       The source counters will be looked up in the source group only when needed.
 *       If source counters are not present when the counters need to be transferred,
 *       no values will be transferred and no errors will be generated. In this case,
 *       the source counters will be looked up again during the next transfer attempt.
 *       The above statement applies only if all counters for the given source
 *       tablet type are missing (no updates received yet). However, if some source counters
 *       are already present and some source counters are not (at least one update
 *       has already been received), then the missing counters will be ignored.
 *
 *       This allows the source counters to be created asynchronously at some point
 *       in the future. Once the source counters are created, the target counters
 *       will be populated with the corresponding values.
 *
 * @param[in] tabletType The tablet type for which to create the TYdbMetricsMapper class
 * @param[in] targetCounterGroup The counter group where the target (mapped) counters are created
 * @param[in] sourceCounterGroup The counter group where the source counters are looked up
 *
 * @return The corresponding instance of the TYdbMetricsMapper class
 */
TYdbMetricsMapperPtr CreateYdbMetricsMapperByTabletType(
    TTabletTypes::EType tabletType,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    NMonitoring::TDynamicCounterPtr sourceCounterGroup
);

} // namespace NKikimr
