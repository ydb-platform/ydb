#include "blobstorage_syncquorum.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/shuffle.h>
#include <util/stream/null.h>

#define STR Cnull

using namespace NKikimr::NSync;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TQuorumTrackerTests) {

        TVector<TVDiskID> GetDisks(TBlobStorageGroupInfo *groupInfo) {
            TVector<TVDiskID> vdisks;
            for (const auto &x : groupInfo->GetVDisks()) {
                auto vd = groupInfo->GetVDiskId(x.OrderNumber);
                vdisks.push_back(vd);
            }
            return vdisks;
        }

        void Check(TQuorumTracker tracker,              // quorum tracker
                   const TVector<TVDiskID> &vdisks,     // array of vdisks
                   const TVector<int> &notYetQuorum,    // array of vdisks pos to apply before achieving quorum
                   int q)                               // last vdisk pos that leads to quorum
        {
            for (const auto &x : notYetQuorum) {
                tracker.Update(vdisks[x]);
                UNIT_ASSERT(!tracker.HasQuorum());
            }
            tracker.Update(vdisks[q]);
            UNIT_ASSERT(tracker.HasQuorum());
        }

        Y_UNIT_TEST(ErasureNoneNeverHasQuorum_4_1) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureNone, 1, 4);

            auto vdisks = GetDisks(&groupInfo);
            Shuffle(vdisks.begin(), vdisks.end());
            auto self = vdisks[0];

            TQuorumTracker tracker(self, groupInfo.PickTopology(), false);
            for (unsigned i = 1; i < vdisks.size(); i++) {
                tracker.Update(vdisks[i]);
                UNIT_ASSERT(!tracker.HasQuorum());
                STR << tracker.ToString() << "\n";
            }
            UNIT_ASSERT(!tracker.HasQuorum());
        }

        Y_UNIT_TEST(Erasure4Plus2BlockIncludingMyFailDomain_8_2) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 2, 8);

            auto vdisks = GetDisks(&groupInfo);
            const TVDiskID& self = vdisks[0];

            TQuorumTracker tracker(self, groupInfo.PickTopology(), true);
            for (unsigned i = 0; i < vdisks.size(); i++) {
                tracker.Update(vdisks[i]);
                if (i + 1 >= 2 * 6) {
                    UNIT_ASSERT(tracker.HasQuorum());
                } else {
                    UNIT_ASSERT(!tracker.HasQuorum());
                }
            }
        }

        Y_UNIT_TEST(Erasure4Plus2BlockNotIncludingMyFailDomain_8_2) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 2, 8);

            auto vdisks = GetDisks(&groupInfo);
            const TVDiskID& self = vdisks[0];

            TQuorumTracker tracker(self, groupInfo.PickTopology(), false);
            for (unsigned i = 0; i < vdisks.size(); i++) {
                tracker.Update(vdisks[i]);
                if (i + 1 >= 2 * (6 + 1)) {
                    UNIT_ASSERT(tracker.HasQuorum());
                } else {
                    UNIT_ASSERT(!tracker.HasQuorum());
                }
            }
        }

        Y_UNIT_TEST(ErasureMirror3IncludingMyFailDomain_4_2) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            auto vdisks = GetDisks(&groupInfo);
            const TVDiskID& self = vdisks[0];

            TQuorumTracker tracker(self, groupInfo.PickTopology(), true);
            TVector<int> notYetQuorum = {0, 1, 2, 3, 4, 7};
            Check(tracker, vdisks, notYetQuorum, 6);
        }

        Y_UNIT_TEST(ErasureMirror3IncludingMyFailDomain_5_2) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 5);
            auto vdisks = GetDisks(&groupInfo);
            const TVDiskID& self = vdisks[0];

            TQuorumTracker tracker(self, groupInfo.PickTopology(), true);
            TVector<int> notYetQuorum = {0, 1, 3, 4, 5, 6, 8, 9};
            Check(tracker, vdisks, notYetQuorum, 2);
        }
    }

} // NKikimr
