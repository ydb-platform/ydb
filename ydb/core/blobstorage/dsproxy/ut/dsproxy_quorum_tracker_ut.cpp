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
    auto check = [&](ui64 failedMask) {
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
    };
    if (erasure == TBlobStorageGroupType::Erasure8Plus2Block) {
        // Whole-domain losses exercise both VDisks of each ordinary failure domain.
        check(0);
        for (ui32 first = 0; first < numFailDomainsPerFailRealm; ++first) {
            const ui64 a = ui64{3} << (2 * first);
            check(a);
            for (ui32 second = 0; second < first; ++second) {
                check(a | (ui64{3} << (2 * second)));
            }
        }
        check(0x3f);
        check(ui64{0x3f} << (numDisks - 6));
        ui64 seed = 0x82422026;
        for (ui32 i = 0; i < 1024; ++i) {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            check(seed & ((ui64{1} << numDisks) - 1));
        }
    } else {
        for (ui64 mask = 0; mask != (ui64{1} << numDisks); ++mask) {
            check(mask);
        }
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
    Y_UNIT_TEST(CheckFailModelBlock82) { RunCheckFailModel(TBlobStorageGroupType::Erasure8Plus2Block); }
    UNIT_TEST_FOR_ERASURE(Erasure3Plus2Block)
    UNIT_TEST_FOR_ERASURE(Erasure4Plus2Stripe)
    UNIT_TEST_FOR_ERASURE(Erasure3Plus2Stripe)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3Plus2)
    UNIT_TEST_FOR_ERASURE(ErasureMirror3dc)

}
