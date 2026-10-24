#pragma once

#include "dsproxy_strategy_base.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>

#include <algorithm>
#include <array>
#include <bit>
#include <tuple>

namespace NKikimr {

class TMirror3dcPutStrategy : public TStrategyBase {
    using TSituation = TBlobState::ESituation;
    using TCounts = std::array<ui32, 3>;

    const TEvBlobStorage::TEvPut::ETactic Tactic;
    const bool EnableRequestMod3x3ForMinLatency;
    const bool Accelerate;

    struct TPlan {
        ui32 Disks = 0;
        ui32 Requests = 0;
        ui32 Pending = 0;
    };

    using TRealmPlans = std::array<std::array<TPlan, 4>, 3>;

    static TStackVec<TCounts, 7> MakeCandidates(EMirror3dcLayout writeLayout) {
        using L = EMirror3dcLayout;
        switch (writeLayout) {
            case L::LAYOUT_111_22:
                return {{1, 1, 1}, {0, 2, 2}, {2, 0, 2}, {2, 2, 0}};
            case L::LAYOUT_11_2:
                return {{0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {0, 0, 2}, {0, 2, 0}, {2, 0, 0}};
            case L::LAYOUT_11:
                return {{0, 1, 1}, {1, 0, 1}, {1, 1, 0}};
            case L::LAYOUT_22:
                return {{0, 2, 2}, {2, 0, 2}, {2, 2, 0}};
            case L::LAYOUT_0:
                return {{0, 0, 0}};
            case L::LAYOUT_1:
                return {{0, 0, 1}, {0, 1, 0}, {1, 0, 0}};
            case L::LAYOUT_2:
                return {{0, 0, 2}, {0, 2, 0}, {2, 0, 0}};
            case L::LAYOUT_111:
                return {{1, 1, 1}};
        }
        return {};
    }

    static ui32 CountRealms(const TCounts& counts) {
        return (counts[0] != 0) + (counts[1] != 0) + (counts[2] != 0);
    }

    static ui32 PrefixMask(const TCounts& counts) {
        ui32 mask = 0;
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 copy = 0; copy < counts[realm]; ++copy) {
                mask |= 1u << (realm + 3 * copy);
            }
        }
        return mask;
    }

    static TCounts CountRealmsDisks(ui32 mask) {
        TCounts counts{};
        for (ui32 realm = 0; realm < 3; ++realm) {
            counts[realm] = std::popcount((mask >> realm) & 0b001001001u);
        }
        return counts;
    }

    TRealmPlans PrepareRealmPlans(ui32 available, ui32 present, ui32 sent) const {
        std::array<ui32, 9> ranks;
        for (ui32 disk = 0; disk < 9; ++disk) {
            const ui32 bit = 1u << disk;
            if (!(available & bit)) {
                ranks[disk] = 3;
            } else if (present & bit) {
                ranks[disk] = 0;
            } else {
                // Keep pending writes normally; acceleration prefers a fresh disk.
                ranks[disk] = bool(sent & bit) == Accelerate ? 2 : 1;
            }
        }
        TRealmPlans realmPlans{};
        for (ui32 realm = 0; realm < 3; ++realm) {
            std::array<ui32, 3> disks = {realm, realm + 3, realm + 6};
            std::sort(disks.begin(), disks.end(), [&](ui32 a, ui32 b) {
                return std::tuple{ranks[a], a} < std::tuple{ranks[b], b};
            });
            // Cache the available part of each prefix: zero through three copies.
            for (ui32 copy = 0; copy < 3; ++copy) {
                auto& plan = realmPlans[realm][copy + 1];
                plan = realmPlans[realm][copy];
                const ui32 bit = 1u << disks[copy];
                if (available & bit) {
                    plan.Disks |= bit;
                    plan.Requests += !(bit & (present | sent));
                    plan.Pending += bool(bit & sent);
                }
            }
        }
        return realmPlans;
    }

    static TPlan MakePlan(const TCounts& target, const TRealmPlans& realmPlans) {
        TPlan plan;
        for (ui32 realm = 0; realm < 3; ++realm) {
            const auto& prefix = realmPlans[realm][target[realm]];
            plan.Disks |= prefix.Disks;
            plan.Requests += prefix.Requests;
            plan.Pending += prefix.Pending;
        }
        return plan;
    }

    TPlan SelectThroughputPlan(const TStackVec<TCounts, 7>& layouts, const TRealmPlans& realmPlans) const {
        TPlan best;
        // skip realms, all requests, pending/new requests, new requests
        std::tuple<ui32, ui32, ui32, ui32> bestScore{4, 10, 10, 10};
        for (const auto& layout : layouts) {
            const auto plan = MakePlan(layout, realmPlans);
            const auto score = std::tuple{
                3 - CountRealms(layout),
                plan.Requests + plan.Pending,
                Accelerate ? plan.Pending : plan.Requests,
                plan.Requests};
            if (score < bestScore) {
                bestScore = score;
                best = plan;
            }
        }
        return best;
    }

    static TPlan SelectLatencyPlan(const TStackVec<TCounts, 7>& layouts, const TRealmPlans& realmPlans,
            const TCounts& capacity) {
        TCounts target{};
        for (const auto& layout : layouts) {
            for (ui32 realm = 0; realm < 3; ++realm) {
                target[realm] = std::max(target[realm], layout[realm]);
            }
        }
        // If the union is itself a minimal quorum, add one available copy per participating realm.
        if (std::find(layouts.begin(), layouts.end(), target) != layouts.end()) {
            for (ui32 realm = 0; realm < 3; ++realm) {
                if (target[realm]) {
                    target[realm] = std::min(target[realm] + 1, capacity[realm]);
                }
            }
        }
        return MakePlan(target, realmPlans);
    }

public:
    TMirror3dcPutStrategy(TEvBlobStorage::TEvPut::ETactic tactic, bool enableRequestMod3x3ForMinLatency,
            bool accelerate = false)
        : Tactic(tactic)
        , EnableRequestMod3x3ForMinLatency(enableRequestMod3x3ForMinLatency)
        , Accelerate(accelerate)
    {}

    EStrategyOutcome Process(TLogContext& logCtx, TBlobState& state, const TBlobStorageGroupInfo& info,
            TBlackboard& blackboard, TGroupDiskRequests& groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, state.Id.Hash());
        if (!checker || state.Disks.size() != 9) {
            return EStrategyOutcome::Error("Invalid mirror-3-dc write state");
        }
        const auto check = [&](ui32 mask) {
            return checker->CheckQuorumForSubgroup(
                TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask));
        };
        ui32 available = 0;
        ui32 present = 0;
        ui32 sent = 0;
        for (ui32 disk = 0; disk < 9; ++disk) {
            const auto physicalRealm = info.GetVDiskId(state.Disks[disk].OrderNumber).FailRealm;
            if (info.GetFailRealmState(physicalRealm) == NKikimrBlobStorage::TGroupInfo::TFailRealm::F) {
                continue;
            }
            const auto situation = state.Disks[disk].DiskParts[disk % 3].Situation;
            const ui32 bit = 1u << disk;
            if (situation != TSituation::Error) {
                available |= bit;
                present |= situation == TSituation::Present ? bit : 0;
                sent |= situation == TSituation::Sent ? bit : 0;
            }
        }
        if (check(present)) {
            return EStrategyOutcome::DONE;
        } else if (!check(available)) {
            return EStrategyOutcome::Error("Mirror-3-dc write quorum is no longer attainable");
        }

        const ui32 slow = MakeSlowSubgroupDiskMask(state, blackboard, true, accelerationParams);
        const ui32 fast = available & (~slow | present);
        if (check(fast)) {
            available = fast;
        }
        ui32 selectedDisks = available;
        if (Tactic != TEvBlobStorage::TEvPut::TacticMinLatency || !EnableRequestMod3x3ForMinLatency) {
            const auto capacity = CountRealmsDisks(available);
            // W is derived from the current realm states when constructing the checker.
            const auto candidates = MakeCandidates(checker->GetWriteLayout());
            TStackVec<TCounts, 7> layouts;
            for (const auto& candidate : candidates) {
                if (candidate[0] > capacity[0] || candidate[1] > capacity[1] || candidate[2] > capacity[2]
                        || !check(PrefixMask(candidate))) {
                    continue;
                }
                layouts.push_back(candidate);
            }
            Y_ABORT_UNLESS(!layouts.empty()); // An attainable W has a minimal placement above.
            const auto realmPlans = PrepareRealmPlans(available, present, sent);
            const auto plan = Tactic == TEvBlobStorage::TEvPut::TacticMinLatency
                ? SelectLatencyPlan(layouts, realmPlans, capacity)
                : SelectThroughputPlan(layouts, realmPlans);
            selectedDisks = plan.Disks;
        }
        TBlobStorageGroupType::TPartPlacement placement;
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (selectedDisks & ~(present | sent) & (1u << disk)) {
                placement.Records.push_back({static_cast<ui8>(disk), static_cast<ui8>(disk % 3)});
            }
        }
        if (!placement.Records.empty()) {
            PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, placement);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }
};

} // namespace NKikimr
