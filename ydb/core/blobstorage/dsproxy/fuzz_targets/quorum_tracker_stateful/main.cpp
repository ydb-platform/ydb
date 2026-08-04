#include <ydb/core/blobstorage/dsproxy/dsproxy_quorum_tracker.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

namespace {

using namespace NKikimr;

constexpr ui32 MaxOps = 256;

TBlobStorageGroupType::EErasureSpecies PickErasure(FuzzedDataProvider& provider) {
    switch (provider.ConsumeIntegralInRange<ui8>(0, 2)) {
        case 0:
            return TBlobStorageGroupType::ErasureNone;
        case 1:
            return TBlobStorageGroupType::Erasure4Plus2Block;
        default:
            return TBlobStorageGroupType::ErasureMirror3dc;
    }
}

TBlobStorageGroupInfo MakeInfo(TBlobStorageGroupType::EErasureSpecies erasure) {
    TBlobStorageGroupType gtype(erasure);
    const bool m3dc = erasure == TBlobStorageGroupType::ErasureMirror3dc;
    const ui32 numVDisksPerFailDomain = m3dc ? 1 : 2;
    const ui32 numFailDomainsPerFailRealm = m3dc ? 4 : gtype.BlobSubgroupSize() + 2;
    const ui32 numFailRealms = m3dc ? 3 : 1;
    return TBlobStorageGroupInfo(erasure, numVDisksPerFailDomain, numFailDomainsPerFailRealm, numFailRealms);
}

NKikimrProto::EReplyStatus PickStatus(FuzzedDataProvider& provider) {
    switch (provider.ConsumeIntegralInRange<ui8>(0, 3)) {
        case 0:
            return NKikimrProto::OK;
        case 1:
            return NKikimrProto::ERROR;
        case 2:
            return NKikimrProto::VDISK_ERROR_STATE;
        default:
            return NKikimrProto::OUT_OF_SPACE;
    }
}

void CheckStatus(const TBlobStorageGroupInfo& info, NKikimrProto::EReplyStatus status) {
    Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::ERROR || status == NKikimrProto::UNKNOWN);
    Y_ABORT_UNLESS(status != NKikimrProto::OK || info.GetQuorumChecker().CheckFailModelForGroup(
        TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology())));
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    const TBlobStorageGroupInfo info = MakeInfo(PickErasure(provider));
    TGroupQuorumTracker tracker(&info);

    TInstant now = TInstant::MilliSeconds(provider.ConsumeIntegralInRange<ui64>(0, 1000));
    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 opIndex = 0; opIndex < ops && provider.remaining_bytes(); ++opIndex) {
        const ui32 diskIndex = provider.ConsumeIntegralInRange<ui32>(0, info.GetTotalVDisksNum() - 1);
        const TVDiskID& vdiskId = info.GetVDiskId(diskIndex);
        NKikimrProto::EReplyStatus status = PickStatus(provider);

        if (provider.ConsumeBool()) {
            std::vector<TVDiskID> queryStatus;
            std::vector<TVDiskID> resend;
            now += TDuration::MilliSeconds(provider.ConsumeIntegralInRange<ui64>(0, 20000));
            const ui64 incarnation = provider.ConsumeIntegral<ui64>();
            status = tracker.ProcessReplyWithCooldown(vdiskId, status, now, incarnation, queryStatus, resend);
            for (const auto& id : queryStatus) {
                Y_ABORT_UNLESS(info.GetOrderNumber(id) < info.GetTotalVDisksNum());
            }
            for (const auto& id : resend) {
                Y_ABORT_UNLESS(id == vdiskId);
            }
            Y_ABORT_UNLESS(resend.size() <= 1);
        } else {
            status = tracker.ProcessReply(vdiskId, status);
        }

        CheckStatus(info, status);
        Y_ABORT_UNLESS(tracker.ToString());
    }

    return 0;
}
