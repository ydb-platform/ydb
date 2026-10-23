#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_quorum_tracker.h>

using namespace NKikimr;

void RunCheckFailModel(TBlobStorageGroupType::EErasureSpecies erasure) {
    TBlobStorageGroupType gtype(erasure);
    const bool m3dc = erasure == TBlobStorageGroupType::ErasureMirror3dc;
    const ui32 numVDisksPerFailDomain = m3dc ? 1 : 2;
    const ui32 numFailDomainsPerFailRealm = m3dc ? 4 : gtype.BlobSubgroupSize() + 2;
    const ui32 numFailRealms = m3dc ? 3 : 1;
    TBlobStorageGroupInfo info(erasure, numVDisksPerFailDomain, numFailDomainsPerFailRealm, numFailRealms);

    const ui32 numDisks = info.GetTotalVDisksNum();
    for (ui64 failedMask = 0; failedMask != (ui64)1 << numDisks; ++failedMask) {
        TGroupQuorumTracker tracker(&info);

        NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
        for (const auto& vdisk : info.GetVDisks()) {
            const TVDiskID& vdiskId = info.GetVDiskId(vdisk.OrderNumber);
            NKikimrProto::EReplyStatus diskStatus = (failedMask >> vdisk.OrderNumber) & 1
                ? NKikimrProto::ERROR : NKikimrProto::OK;
            status = tracker.ProcessReply(vdiskId, diskStatus);
        }

        NKikimrProto::EReplyStatus expectedStatus = info.GetQuorumChecker().CheckFailModelForGroup(
                TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&info.GetTopology(), failedMask))
            ? NKikimrProto::OK
            : NKikimrProto::ERROR;

        UNIT_ASSERT_VALUES_EQUAL(status, expectedStatus);
    }
}

Y_UNIT_TEST_SUITE(TDsProxyQuorumTracker) {

    Y_UNIT_TEST(SingleDcQuorum) {
        for (ui32 realm = 0; realm < 3; ++realm) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 3);
            info.EnableSingleDcMode = true;
            info.SurvivingDc = realm;
            for (ui32 failed = 0; failed < 8; ++failed) {
                TGroupQuorumTracker tracker(&info);
                for (const auto& disk : info.GetVDisks()) {
                    const auto id = info.GetVDiskId(disk.OrderNumber);
                    if (id.FailRealm != realm) {
                        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(id, NKikimrProto::OK), NKikimrProto::UNKNOWN);
                    }
                }
                ui32 local = 0;
                ui32 errors = 0;
                for (const auto& disk : info.GetVDisks()) {
                    const auto id = info.GetVDiskId(disk.OrderNumber);
                    if (id.FailRealm == realm) {
                        const bool error = failed & (1u << local++);
                        errors += error;
                        tracker.ProcessReply(id, error ? NKikimrProto::ERROR : NKikimrProto::OK);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateStatus(),
                    errors >= 2 ? NKikimrProto::ERROR : NKikimrProto::OK);
            }
        }
    }

    Y_UNIT_TEST(SingleDcQuorumCooldown) {
        for (ui32 realm = 0; realm < 3; ++realm) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 3);
            info.EnableSingleDcMode = true;
            info.SurvivingDc = realm;
            std::vector<TVDiskID> local;
            for (const auto& disk : info.GetVDisks()) {
                const auto id = info.GetVDiskId(disk.OrderNumber);
                if (id.FailRealm == realm) {
                    local.push_back(id);
                }
            }
            TGroupQuorumTracker tracker(&info);
            std::vector<TVDiskID> queryStatus, resend;
            const auto now = TInstant::Seconds(100);
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(local[0], NKikimrProto::OK,
                now, 1, queryStatus, resend), NKikimrProto::UNKNOWN);
            const auto later = now + VDiskCooldownTimeoutOnProxy + TDuration::Seconds(1);
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(local[1], NKikimrProto::OK,
                later, 1, queryStatus, resend), NKikimrProto::UNKNOWN);
            UNIT_ASSERT_VALUES_EQUAL(queryStatus.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(queryStatus.front(), local[0]);
            UNIT_ASSERT(resend.empty());
            // A restarted disk cannot confirm the old incarnation's write.
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(local[0], NKikimrProto::OK,
                later, 2, queryStatus, resend), NKikimrProto::UNKNOWN);
            UNIT_ASSERT_VALUES_EQUAL(resend.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resend.front(), local[0]);
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(local[0], NKikimrProto::OK,
                later, 2, queryStatus, resend), NKikimrProto::OK);
        }
    }

#define UNIT_TEST_FOR_ERASURE(ERASURE) \
    Y_UNIT_TEST(CheckFailModel##ERASURE) { \
        RunCheckFailModel(TBlobStorageGroupType::ERASURE); \
    }

    UNIT_TEST_FOR_ERASURE(ErasureNone)
    UNIT_TEST_FOR_ERASURE(Erasure4Plus2Block)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3dc)

}
