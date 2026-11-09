#pragma once

#include "flat_executor_misc.h"
#include "flat_tier_predicate.h"

#include <util/generic/hash.h>

namespace NKikimr::NTabletFlatExecutor {

    class ICompactionRouter {
    public:
        virtual ~ICompactionRouter() = default;

        virtual void Prepare(const NTable::TRowScheme&) { }

        // Called once per nonempty key, with its newest reconstructed committed
        // row, or nullptr for a key containing only locks/uncommitted deltas.
        // Returns an index into TCompactCfg::Outputs. Arguments are borrowed.
        virtual ui32 RouteKey(TCellsRef key,
            const NTable::TRowState* row, ui32 sourceTier) = 0;

        virtual TVector<ui32> GetUnrepresentableTierIds() const {
            return {};
        }
    };

    class TDefaultCompactionRouter final : public ICompactionRouter {
    public:
        ui32 RouteKey(TCellsRef, const NTable::TRowState*, ui32) override {
            return 0;
        }
    };

    class TTierCompactionRouter final : public ICompactionRouter {
    public:
        // The policy, evaluation time and physical tier order are fixed for the
        // operation. No policy preserves source placement; allowCooling=false
        // also permits maintenance without scheduling new cooling decisions.
        TTierCompactionRouter(const TCompactCfg& conf,
            std::optional<NTable::TTieringPolicy> policy, TInstant now,
            TVector<ui32> tierOrder, bool allowCooling = true);

        void Prepare(const NTable::TRowScheme& scheme) override;
        ui32 RouteKey(TCellsRef key, const NTable::TRowState* row, ui32 sourceTier) override;
        TVector<ui32> GetUnrepresentableTierIds() const override;

    private:
        const TIntrusiveConstPtr<NTable::TCompactionContext> Context;
        const std::optional<NTable::TTieringPolicy> Policy;
        const TInstant Now;
        const TVector<ui32> TierOrder;
        const bool AllowCooling;
        const bool UseConservativeMasks;
        const bool HasUnknownRemaining;
        THashMap<ui32, ui32> OutputByTier;
        THolder<NTable::TTierPredicate> Predicate;
        THolder<NTable::TCompactionTierCap> Cap;
        bool Prepared = false;
    };

}
