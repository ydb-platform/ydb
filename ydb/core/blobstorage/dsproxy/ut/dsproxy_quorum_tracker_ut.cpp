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

#define UNIT_TEST_FOR_ERASURE(ERASURE) \
    Y_UNIT_TEST(CheckFailModel##ERASURE) { \
        RunCheckFailModel(TBlobStorageGroupType::ERASURE); \
    }

    UNIT_TEST_FOR_ERASURE(ErasureNone)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3)
    UNIT_TEST_FOR_ERASURE(Erasure3Plus1Block)
    UNIT_TEST_FOR_ERASURE(Erasure3Plus1Stripe)
    UNIT_TEST_FOR_ERASURE(Erasure4Plus2Block)
    UNIT_TEST_FOR_ERASURE(Erasure3Plus2Block)
    UNIT_TEST_FOR_ERASURE(Erasure4Plus2Stripe)
    UNIT_TEST_FOR_ERASURE(Erasure3Plus2Stripe)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3Plus2)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3dc)

}
