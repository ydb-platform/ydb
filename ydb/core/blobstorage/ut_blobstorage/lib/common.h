#pragma once

#include "env.h"
#include <ydb/core/base/logoblob.h>
#include <util/random/mersenne.h>


inline TBlobStorageGroupType GetErasureTypeByString(const TString& erasure) {
    if (erasure == "none") {
        return TBlobStorageGroupType::ErasureNone;
    } else if (erasure == "block-4-2") {
        return TBlobStorageGroupType::Erasure4Plus2Block;
    } else if (erasure == "mirror-3") {
        return TBlobStorageGroupType::ErasureMirror3;
    } else if (erasure == "mirror-3of4") {
        return TBlobStorageGroupType::ErasureMirror3of4;
    } else if (erasure == "mirror-3-dc") {
        return TBlobStorageGroupType::ErasureMirror3dc;
    }
    UNIT_ASSERT(false);
    return TBlobStorageGroupType::ErasureNone;
}

struct TTestInfo {
        std::unique_ptr<TTestActorSystem> &Runtime;
        TActorId Edge;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
};

inline TTestInfo InitTest(TEnvironmentSetup& env) {
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool();
        env.CommenceReplication();

        auto groups = env.GetGroups();
        auto info = env.GetGroupInfo(groups[0]);

        const TActorId& edge = runtime->AllocateEdgeActor(1);
        return {runtime, edge, info};
}

// calculate mapping orderNumber->pdiskId
inline std::vector<ui32> MakePDiskLayout(const NKikimrBlobStorage::TBaseConfig& base,
        const TBlobStorageGroupInfo::TTopology& topology, ui32 groupId) {
    std::vector<ui32> pdiskLayout;
    for (const auto& vslot : base.GetVSlot()) {
        const auto& vslotId = vslot.GetVSlotId();
        ui32 orderNumber = topology.GetOrderNumber(TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        if (vslot.GetGroupId() == groupId) {
            if (orderNumber >= pdiskLayout.size()) {
                pdiskLayout.resize(orderNumber + 1);
            }
            pdiskLayout[orderNumber] = vslotId.GetPDiskId();
        }
    }
    return pdiskLayout;
}

template <typename T>
class TWeightedRandom {
public:
    TWeightedRandom(ui64 seed = 0)
        : PrefixSum({ 0 })
        , Mt64(new TMersenne<ui64>(seed))
    {}

    TWeightedRandom(const TWeightedRandom&) = default;
    TWeightedRandom(TWeightedRandom&&) = default;
    TWeightedRandom& operator=(const TWeightedRandom&) = default;
    TWeightedRandom& operator=(TWeightedRandom&&) = default;

    void AddValue(T value, ui64 weight) {
        PrefixSum.push_back(weight + PrefixSum.back());
        Values.push_back(value);
    }

    T GetRandom() {
        Y_ABORT_UNLESS(WeightSum() != 0);
        return Get((*Mt64)() % WeightSum());
    }

    T Get(ui64 w) {
        Y_ABORT_UNLESS(PrefixSum.size() > 1);
        auto it = std::upper_bound(PrefixSum.begin(), PrefixSum.end(), w);
        Y_ABORT_UNLESS(it > PrefixSum.begin());
        ui32 idx = it - PrefixSum.begin() - 1;
        return Values[idx];
    }

    ui32 WeightSum() {
        return PrefixSum.back();
    }

private:
    std::vector<T> Values;
    std::vector<ui64> PrefixSum;
    std::shared_ptr<TMersenne<ui64>> Mt64;
};
