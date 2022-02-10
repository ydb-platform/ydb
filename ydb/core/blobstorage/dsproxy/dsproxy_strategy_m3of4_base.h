#pragma once

#include "defs.h"

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

    class TMirror3of4StrategyBase : public IStrategy {
    protected:
        static constexpr ui32 MetadataPartIdx = 2;

        struct TGroups {
            struct TGroupInfo {
                ui32 DiskIdx[3];
                ui32 PartIdx;
                bool Requested = false;
                bool Done = false;
                ui32 NumSlowDisks = 0;
            } Groups[4] = {
                {{0, 4, 5}, 0},
                {{1, 4, 5}, 1},
                {{2, 6, 7}, 0},
                {{3, 6, 7}, 1},
            };
        };

        static constexpr bool DiskPartsAllowed[3][8] = {
            {true, false, true, false, true, true, true, true},
            {false, true, false, true, true, true, true, true},
            {true, true, true, true, true, true, true, true}
        };

        bool CheckFailModel(TBlobState& state, const TBlobStorageGroupInfo& info) {
            TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                for (const auto& part : state.Disks[diskIdx].DiskParts) {
                    if (part.Situation == TBlobState::ESituation::Error) {
                        error |= {&info.GetTopology(), diskIdx};
                        break;
                    }
                }
            }
            return info.GetQuorumChecker().CheckFailModelForSubgroup(error);
        }
    };

} // NKikimr
