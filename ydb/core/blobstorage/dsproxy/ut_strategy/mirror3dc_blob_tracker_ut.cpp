#include <ydb/core/blobstorage/dsproxy/dsproxy_blob_tracker.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using TDataCenterState = NKikimrBlobStorage::TDataCenterState;

namespace NKikimr {
namespace {

using TCounts = std::array<ui32, 3>;

void CheckCase(const TMirror3dcGroupState& states, const TCounts& copies, const TCounts& failures,
        const TCounts& historicalCopies, TBlobStorageGroupInfo::EBlobState expected, bool expectedLost = false,
        NKikimrProto::EReplyStatus failureStatus = NKikimrProto::ERROR, const TCounts& transportFailures = {}) {
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
        dynamic.Mirror3dcState = {states.State, {states.FailRealmStates[permutation[0]], states.FailRealmStates[permutation[1]], states.FailRealmStates[permutation[2]]}};
        const TBlobStorageGroupInfo info(TBlobStorageGroupInfo::TTopology(TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1),
            std::move(dynamic), "");
        for (ui32 cookie = 0; cookie < 3; ++cookie) {
            const TLogoBlobID id(1, 1, 1, 0, 8, cookie);
            TBlobStorageGroupInfo::TVDiskIds subgroup;
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
                TBlobStorageGroupInfo::TGroupVDisks failedGroupDisks(&info.GetTopology());
                for (ui32 disk = 0; disk < 9; ++disk) {
                    const ui32 realm = permutation[subgroup[disk].FailRealm];
                    if (disk / 3 < transportFailures[realm]) {
                        failedGroupDisks += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), subgroup[disk]);
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
        CheckCase({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {1, 1, 1}, {}, {}, TBlobStorageGroupInfo::EBS_FULL);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, {}, TBlobStorageGroupInfo::EBS_FULL);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {}, {}, TBlobStorageGroupInfo::EBS_FULL);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 0, 2}, {}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED);
    }

    Y_UNIT_TEST(EnsuredLayoutDeterminesAbsence) {
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {}, {}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 3, 3}, {}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 3, 3}, {}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(RecoveryFailuresFollowEnsuredAlternatives) {
        for (auto status : {NKikimrProto::ERROR, NKikimrProto::NOT_YET, NKikimrProto::VDISK_ERROR_STATE}) {
            CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 1}, {},
                TBlobStorageGroupInfo::EBS_DISINTEGRATED, false, status);
            CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 0, 2}, {},
                TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, false, status);
            CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 2, 3}, {},
                TBlobStorageGroupInfo::EBS_DISINTEGRATED, false, status);
        }
    }

    Y_UNIT_TEST(FailedRealmCopiesDoNotCount) {
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {0, 3, 3}, {}, {}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(TransportErrorsAndNotYetCombineForAbsence) {
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY,
            false, NKikimrProto::NOT_YET);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_DISINTEGRATED,
            false, NKikimrProto::NOT_YET, {1, 0, 0});
    }

    Y_UNIT_TEST(IngressUsesCurrentWriteLayout) {
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, {2, 0, 0}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {}, {2, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, true);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 0, 2}, {}, {2, 2, 0}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(InvalidGroupConfigurationDoesNotFallBackToLegacy) {
        CheckCase({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}},
            {3, 3, 3}, {}, {}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckCase({static_cast<EMirror3dcGroupState>(-1), {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}},
            {3, 3, 3}, {}, {}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
    }
}

} // namespace NKikimr
