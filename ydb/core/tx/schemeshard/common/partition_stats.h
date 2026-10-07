#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <algorithm>
#include <array>

namespace NKikimr::NSchemeShard {

/**
 * The container for the latest time stamps when the CPU usage exceeded
 * specific thresholds: 2%, 5%, 10%, 20%, 30%.
 */
struct TTopCpuUsage {
    /**
     * Describes the boundaries for a CPU usage bucket.
     */
    struct TBucket {
        /**
         * The low boundary for this bucket.
         *
         * @note If the current CPU usage exceeds this value, this bucket is updated.
         */
        const ui32 LowBoundary;

        /**
         * The effective CPU usage value for this bucket.
         *
         * @note If this bucket falls within the given time period,
         *       this value is used as the assumed CPU usage percentage.
         */
        const ui32 EffectiveValue;
    };

    /**
     * The boundaries for all CPU usage buckets tracked by this class.
     *
     * @warning This list must be sorted by the threshold value (in the ascending order).
     */
    static constexpr std::array<TBucket, 5> Buckets = {{
        {2, 5},   // >=  2% -->  5% CPU usage
        {5, 10},  // >=  5% --> 10% CPU usage
        {10, 20}, // >= 10% --> 20% CPU usage
        {20, 30}, // >= 20% --> 30% CPU usage
        {30, 40}, // >= 30% --> 40% CPU usage
    }};

    /**
     * The time when each usage bucket was updated.
     */
    std::array<TInstant, Buckets.size()> BucketUpdateTimes;

    /**
     * Update the CPU usage data using values from another container.
     *
     * @param[in] usage The container to update the usage data from
     */
    void Update(const TTopCpuUsage& usage) {
        // Keep only the latest time for each bucket
        for (ui64 i = 0; i < Buckets.size(); ++i) {
            BucketUpdateTimes[i] = std::max(BucketUpdateTimes[i], usage.BucketUpdateTimes[i]);
        }
    }

    /**
     * Update the historical CPU usage.
     *
     * @param[in] rawCpuUsage The current CPU usage
     * @param[in] now The current time
     */
    void UpdateCpuUsage(ui64 rawCpuUsage, TInstant now) {
        ui32 percent = static_cast<ui32>(rawCpuUsage * 0.000001 * 100);

        // Update all buckets, which have low boundaries below the given CPU usage
        for (ui64 i = 0; i < Buckets.size(); ++i) {
            if (percent < Buckets[i].LowBoundary) {
                return;
            }

            BucketUpdateTimes[i] = now;
        }
    }

    /**
     * Get the peak CPU usage percentage that has been observed since the given time.
     *
     * @note This function does not return the actual peak CPU usage value.
     *       The return value is one of the preset thresholds, which this class
     *       tracks (2%, 5%, 10%, 20%, 30% and 40%).
     *
     * @todo Fix the case when stats were not collected yet
     *
     * @param[in] since The time from which to calculate the peak CPU usage
     *
     * @return The peak CPU usage (as a percentage) since the given time
     */
    ui32 GetLatestMaxCpuUsagePercent(TInstant since) const {
        // Find the highest bucket (from the end of the list),
        // which was updated after the given time
        for (i64 i = Buckets.size() - 1; i >= 0; --i) {
            if (BucketUpdateTimes[i] > since) {
                return Buckets[i].EffectiveValue;
            }
        }

        // No bucket was found, return at least some minimum CPU usage percentage
        return 2;
    }
};

} // namespace NKikimr::NSchemeShard
