#pragma once

#include "env.h"
#include <ydb/core/base/logoblob.h>


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
std::vector<ui32> MakePDiskLayout(const NKikimrBlobStorage::TBaseConfig& base, ui32 groupId) {
    std::vector<ui32> pdiskLayout.resize(groupSize);
    for (const auto& vslot : base.GetVSlot()) {
        const auto& vslotId = vslot.GetVSlotId();
        ui32 orderNumber = topology.GetOrderNumber(TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        if (vslot.GetGroupId() == groupId) {
            if (orderNumber > pdiskLayout.size()) {
                pdiskLayout.resize(orderNumber);
            }
            pdiskLayout[orderNumber] = vslotId.GetPDiskId();
        }
    }
    return pdiskLayout;
}
