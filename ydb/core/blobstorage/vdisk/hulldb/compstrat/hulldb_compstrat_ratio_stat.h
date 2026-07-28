#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>
#include <util/string/printf.h>

namespace NKikimr::NHullComp {

    struct TStorageRatioStat {
        ui32 SstsChecked = 0;
        bool BreakedActualRatio = false;
        bool BreakedTimeout = false;
        bool UsedBatchAlgorithm = false;
        bool NonOverlappingFallback = false;

        void AccountMetrics(
                THullCtx& hullCtx,
                bool optimizationEnabled,
                TDuration elapsed) const
        {
            auto& totals = hullCtx.StorageRatioGroup;
            ++totals.StorageRatioInvocations();
            totals.StorageRatioTotalElapsedMicroseconds() += elapsed.MicroSeconds();

            if (BreakedTimeout) {
                ++totals.StorageRatioTimeouts();
            }
            if (NonOverlappingFallback) {
                ++totals.StorageRatioNonOverlappingFallbacks();
            }
            if (!SstsChecked) {
                ++totals.StorageRatioNoCalculationInvocations();
                return;
            }

            if (optimizationEnabled) {
                ++totals.StorageRatioFullRecalculations();
            }

            if (!optimizationEnabled) {
                ++totals.StorageRatioFeatureDisabledCalculations();
            }

            auto& algorithm = UsedBatchAlgorithm
                ? hullCtx.StorageRatioBatchGroup
                : hullCtx.StorageRatioLegacyGroup;
            ++algorithm.StorageRatioCalculations();
            algorithm.StorageRatioSstsCalculated() += SstsChecked;
            algorithm.StorageRatioElapsedMicroseconds() += elapsed.MicroSeconds();
        }

        TString ToString() const {
            auto bool2str = [] (bool v) { return v ? "true" : "false"; };
            return Sprintf(
                "{SstsChecked# %" PRIu32 " "
                "BreakedActualRatio# %s BreakedTimeout# %s "
                "UsedBatchAlgorithm# %s NonOverlappingFallback# %s}",
                SstsChecked,
                bool2str(BreakedActualRatio),
                bool2str(BreakedTimeout),
                bool2str(UsedBatchAlgorithm),
                bool2str(NonOverlappingFallback));
        }
    };

} // NKikimr::NHullComp
