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

namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using G = TBlobStorageGroupInfo;

G MakeMirror3dcInfo(const std::array<R::EState, 3>& realms) {
    G::TDynamicInfo dynamic(TGroupId::Zero(), 1);
    dynamic.FailRealmStates.assign(realms.begin(), realms.end());
    return G(G::TTopology(TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1), std::move(dynamic), "");
}

} // namespace

Y_UNIT_TEST_SUITE(TDsProxyQuorumTracker) {
    Y_UNIT_TEST(NormalMatchesLegacyAfterEveryReply) {
        const auto info = MakeMirror3dcInfo({R::N0, R::N0, R::N0});
        const auto& legacy = info.GetQuorumChecker();
        for (ui32 mask = 0; mask < (1u << 12); ++mask) {
            TGroupQuorumTracker tracker(&info);
            G::TGroupVDisks failed(&info.GetTopology());
            G::TGroupVDisks successful(&info.GetTopology());
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateStatus(), NKikimrProto::UNKNOWN);
            for (const auto& disk : info.GetVDisks()) {
                const auto vd = info.GetVDiskId(disk.OrderNumber);
                const bool error = mask & (1u << disk.OrderNumber);
                (error ? failed : successful) += G::TGroupVDisks(&info.GetTopology(), vd);
                const auto expected = !legacy.CheckFailModelForGroup(failed) ? NKikimrProto::ERROR
                    : legacy.CheckQuorumForGroup(successful) ? NKikimrProto::OK : NKikimrProto::UNKNOWN;
                UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(vd, error ? NKikimrProto::ERROR : NKikimrProto::OK),
                    expected);
            }
        }
    }

    Y_UNIT_TEST(RecoveryErrorsCanHideTripleButCannotReplaceNormalRealmInPair) {
        const auto info = MakeMirror3dcInfo({R::N2, R::N2, R::R});
        for (const auto failures : {std::array{1u, 1u, 1u}, std::array{2u, 0u, 2u}}) {
            TGroupQuorumTracker tracker(&info);
            for (const auto& disk : info.GetVDisks()) {
                const auto vd = info.GetVDiskId(disk.OrderNumber);
                tracker.ProcessReply(vd, vd.FailDomain < failures[vd.FailRealm]
                    ? NKikimrProto::ERROR : NKikimrProto::OK);
            }
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateStatus(),
                failures[1] ? NKikimrProto::ERROR : NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(EnsuredOneRequiresEveryNormalDomain) {
        const auto info = MakeMirror3dcInfo({R::N1, R::R, R::F});
        TGroupQuorumTracker tracker(&info);
        for (ui32 disk = 4; disk < 12; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(disk), NKikimrProto::ERROR),
                NKikimrProto::UNKNOWN);
        }
        for (ui32 disk = 0; disk < 4; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(disk), NKikimrProto::OK),
                disk == 3 ? NKikimrProto::OK : NKikimrProto::UNKNOWN);
        }
        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(0), NKikimrProto::ERROR), NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(0), NKikimrProto::OK), NKikimrProto::OK);
    }

    Y_UNIT_TEST(EnsuredTwoAllowsOneMissingNormalDomain) {
        const auto info = MakeMirror3dcInfo({R::N2, R::R, R::F});
        TGroupQuorumTracker tracker(&info);
        for (ui32 disk = 0; disk < 3; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(disk), NKikimrProto::OK),
                disk == 2 ? NKikimrProto::OK : NKikimrProto::UNKNOWN);
        }
        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(3), NKikimrProto::ERROR), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(2), NKikimrProto::ERROR), NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(EnsuredTwoRealmsRequiresIntersection) {
        for (const auto state : {R::N1, R::N2}) {
            const auto info = MakeMirror3dcInfo({state, state, R::F});
            TGroupQuorumTracker tracker(&info);
            const ui32 required = state == R::N1 ? 4 : 3;
            for (ui32 disk = 0; disk < required; ++disk) {
                UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(disk), NKikimrProto::OK),
                    disk + 1 == required ? NKikimrProto::OK : NKikimrProto::UNKNOWN);
            }
        }
    }

    Y_UNIT_TEST(InvalidStateFailsImmediately) {
        const auto info = MakeMirror3dcInfo({R::N0, R::R, R::R});
        TGroupQuorumTracker tracker(&info);
        UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateStatus(), NKikimrProto::ERROR);
        for (ui32 disk = 0; disk < 12; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReply(info.GetVDiskId(disk), NKikimrProto::OK),
                NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(CooldownRechecksNormalRealmQuorum) {
        const auto info = MakeMirror3dcInfo({R::N2, R::R, R::F});
        TGroupQuorumTracker tracker(&info);
        const auto now = TInstant::Seconds(1);
        std::vector<TVDiskID> queryStatus;
        std::vector<TVDiskID> resend;
        for (ui32 disk = 0; disk < 3; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(info.GetVDiskId(disk), NKikimrProto::OK,
                now, 1, queryStatus, resend), disk == 2 ? NKikimrProto::OK : NKikimrProto::UNKNOWN);
        }
        const auto expired = now + VDiskCooldownTimeoutOnProxy;
        UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(info.GetVDiskId(4), NKikimrProto::ERROR,
            expired, 1, queryStatus, resend), NKikimrProto::UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(queryStatus.size(), 3);
        UNIT_ASSERT(resend.empty());
        for (ui32 disk = 0; disk < 3; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.ProcessReplyWithCooldown(info.GetVDiskId(disk), NKikimrProto::OK,
                expired, 1, queryStatus, resend), disk == 2 ? NKikimrProto::OK : NKikimrProto::UNKNOWN);
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
