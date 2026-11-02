#include <ydb/core/blobstorage/dsproxy/dsproxy_blob_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr {
namespace {

using G = TBlobStorageGroupInfo;
using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using TStates = std::array<R::EState, 3>;
using TCounts = std::array<ui32, 3>;

void CheckCase(const TStates& states, const TCounts& copies, const TCounts& failures,
        const TCounts& historicalCopies, G::EBlobState expected, bool expectedLost = false,
        NKikimrProto::EReplyStatus failureStatus = NKikimrProto::ERROR, const TCounts& transportFailures = {}) {
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        G::TDynamicInfo dynamic(TGroupId::Zero(), 1);
        dynamic.FailRealmStates = {states[permutation[0]], states[permutation[1]], states[permutation[2]]};
        const G info(G::TTopology(TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1),
            std::move(dynamic), "");
        for (ui32 cookie = 0; cookie < 3; ++cookie) {
            const TLogoBlobID id(1, 1, 1, 0, 8, cookie);
            G::TVDiskIds subgroup;
            info.PickSubgroup(id.Hash(), &subgroup, nullptr);
            TIngress historical;
            for (ui32 disk = 0; disk < 9; ++disk) {
                const ui32 realm = permutation[subgroup[disk].FailRealm];
                UNIT_ASSERT(copies[realm] + failures[realm] <= 3);
                if (disk / 3 < historicalCopies[realm]) {
                    const auto ingress = TIngress::CreateIngressWOLocal(&info.GetTopology(),
                        TVDiskIdShort(subgroup[disk]), TLogoBlobID(id, disk % 3 + 1));
                    UNIT_ASSERT(ingress);
                    historical.Merge(*ingress);
                }
            }
            // Range returns full IDs with local ingress bits; also cover explicit part replies.
            for (bool fullId : {false, true}) {
                TBlobStatusTracker tracker(id, &info);
                G::TGroupVDisks failedGroupDisks(&info.GetTopology());
                for (ui32 disk = 0; disk < 9; ++disk) {
                    const ui32 realm = permutation[subgroup[disk].FailRealm];
                    if (disk / 3 < transportFailures[realm]) {
                        failedGroupDisks += G::TGroupVDisks(&info.GetTopology(), subgroup[disk]);
                        continue;
                    }
                    const bool present = disk / 3 < copies[realm];
                    TIngress ingress = historical;
                    const TLogoBlobID partId(id, disk % 3 + 1);
                    if (present) {
                        const auto local = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                            TVDiskIdShort(subgroup[disk]), partId);
                        UNIT_ASSERT(local);
                        ingress.Merge(*local);
                    }
                    NKikimrBlobStorage::TQueryResult reply;
                    LogoBlobIDFromLogoBlobID(fullId ? id : partId, reply.MutableBlobID());
                    reply.SetStatus(present ? NKikimrProto::OK
                        : disk / 3 >= 3 - failures[realm] ? failureStatus : NKikimrProto::NODATA);
                    reply.SetIngress(ingress.Raw());
                    tracker.UpdateFromResponseData(reply, subgroup[disk], &info);
                }
                bool lost = false;
                UNIT_ASSERT_VALUES_EQUAL(ui32(tracker.GetBlobState(&info, &lost, &failedGroupDisks)), ui32(expected));
                UNIT_ASSERT_VALUES_EQUAL(lost, expectedLost);
            }
        }
    } while (std::next_permutation(permutation.begin(), permutation.end()));
}

} // namespace

Y_UNIT_TEST_SUITE(TMirror3dcBlobStatusTrackerTest) {
    Y_UNIT_TEST(WriteLayoutDeterminesFull) {
        CheckCase({R::N0, R::N0, R::N0}, {1, 1, 1}, {}, {}, G::EBS_FULL);
        CheckCase({R::N1, R::F, R::F}, {2, 0, 0}, {}, {}, G::EBS_FULL);
        CheckCase({R::N1, R::R, R::F}, {2, 2, 0}, {}, {}, G::EBS_FULL);
        CheckCase({R::N2, R::N2, R::R}, {2, 0, 2}, {}, {}, G::EBS_RECOVERABLE_DOUBTED);
    }

    Y_UNIT_TEST(EnsuredLayoutDeterminesAbsence) {
        CheckCase({R::N1, R::F, R::F}, {}, {}, {}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckCase({R::N1, R::F, R::F}, {}, {1, 3, 3}, {}, G::EBS_DISINTEGRATED);
        CheckCase({R::N2, R::F, R::F}, {}, {1, 3, 3}, {}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(RecoveryFailuresFollowEnsuredAlternatives) {
        for (auto status : {NKikimrProto::ERROR, NKikimrProto::NOT_YET, NKikimrProto::VDISK_ERROR_STATE}) {
            CheckCase({R::N2, R::N2, R::R}, {}, {1, 1, 1}, {},
                G::EBS_DISINTEGRATED, false, status);
            CheckCase({R::N2, R::N2, R::R}, {}, {2, 0, 2}, {},
                G::EBS_UNRECOVERABLE_FRAGMENTARY, false, status);
            CheckCase({R::N2, R::N2, R::R}, {}, {2, 2, 3}, {},
                G::EBS_DISINTEGRATED, false, status);
        }
    }

    Y_UNIT_TEST(FailedRealmCopiesDoNotCount) {
        CheckCase({R::N1, R::F, R::F}, {0, 3, 3}, {}, {}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(TransportErrorsAndNotYetCombineForAbsence) {
        CheckCase({R::N2, R::F, R::F}, {}, {1, 0, 0}, {}, G::EBS_UNRECOVERABLE_FRAGMENTARY,
            false, NKikimrProto::NOT_YET);
        CheckCase({R::N2, R::F, R::F}, {}, {1, 0, 0}, {}, G::EBS_DISINTEGRATED,
            false, NKikimrProto::NOT_YET, {1, 0, 0});
    }

    Y_UNIT_TEST(IngressUsesCurrentWriteLayout) {
        CheckCase({R::N1, R::F, R::F}, {1, 0, 0}, {}, {}, G::EBS_RECOVERABLE_DOUBTED);
        CheckCase({R::N1, R::F, R::F}, {1, 0, 0}, {}, {2, 0, 0}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckCase({R::N1, R::F, R::F}, {}, {}, {2, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY, true);
        CheckCase({R::N2, R::N2, R::R}, {2, 0, 2}, {}, {2, 2, 0}, G::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(InvalidRealmStatesDoNotFallBackToLegacy) {
        CheckCase({R::F, R::F, R::F}, {3, 3, 3}, {}, {}, G::EBS_DISINTEGRATED);
    }
}

} // namespace NKikimr
