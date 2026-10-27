#include "dsproxy_vdisk_mock_ut.h"

namespace NKikimr {
namespace {

void CheckRangePages(bool reverse) {
    TIntrusivePtr<TBlobStorageGroupInfo> info = new TBlobStorageGroupInfo(TErasureType::ErasureMirror3of4);
    TGroupMock group(0, info);
    const TLogoBlobID ids[] = {
        TLogoBlobID(1, 1, 1, 0, 100, 0),
        TLogoBlobID(1, 1, 2, 0, 100, 0),
        TLogoBlobID(1, 1, 3, 0, 100, 0),
    };
    auto& disk = group.GetVDiskInSubgroup(ids[0], 0);
    for (const auto& id : ids) {
        UNIT_ASSERT_VALUES_EQUAL(disk.Put(TLogoBlobID(id, 3), TString()), NKikimrProto::OK);
    }
    // Multiple parts of one blob consume one index record, not two page slots.
    UNIT_ASSERT_VALUES_EQUAL(disk.Put(TLogoBlobID(ids[0], 1), TString(50, 'x')), NKikimrProto::OK);

    const ui64 cookie = 123;
    const auto check = [&](const TLogoBlobID& from, const TLogoBlobID& to, TMaybe<ui32> limit,
            const TVector<ui32>& expected, bool overflow) {
        auto query = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(
            disk.GetVDiskId(), TInstant::Max(), NKikimrBlobStorage::Discover,
            TEvBlobStorage::TEvVGet::EFlags::ShowInternals, Nothing(), from, to, 0, &cookie);
        if (limit) {
            query->Record.MutableRangeQuery()->SetMaxResults(*limit);
        }
        TEvBlobStorage::TEvVGetResult reply;
        disk.OnVGet(*query, reply);
        UNIT_ASSERT_VALUES_EQUAL(reply.Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(reply.Record.GetIsRangeOverflow(), overflow);
        UNIT_ASSERT_VALUES_EQUAL(reply.Record.ResultSize(), expected.size());
        for (ui32 i = 0; i < expected.size(); ++i) {
            const auto& item = reply.Record.GetResult(i);
            const ui32 index = expected[i];
            UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(item.GetBlobID()), ids[index]);
            UNIT_ASSERT_VALUES_EQUAL(item.GetCookie(), cookie);
            UNIT_ASSERT(item.HasIngress());
            UNIT_ASSERT_VALUES_EQUAL(item.PartsSize(), index == 0 ? 2 : 1);
            UNIT_ASSERT_VALUES_EQUAL(item.GetParts(item.PartsSize() - 1), 3);
            if (index == 0) {
                UNIT_ASSERT_VALUES_EQUAL(item.GetParts(0), 1);
            }
        }
    };

    const auto from = ids[reverse ? 2 : 0];
    const auto to = ids[reverse ? 0 : 2];
    const TVector<ui32> all = reverse ? TVector<ui32>{2, 1, 0} : TVector<ui32>{0, 1, 2};
    check(from, to, Nothing(), all, false);
    check(from, to, 4, all, false);
    check(from, to, 3, all, true); // Exact limit still sets overflow, like VDisk.
    check(from, to, 2, {all[0], all[1]}, true);
    check(from, to, 0, {}, true); // Explicit protobuf zero differs from an absent limit.
    check(to, to, 1, {all[2]}, true); // Inclusive endpoint, including nonzero part IDs.
    check(TLogoBlobID(ids[0], 1), TLogoBlobID(ids[0], 3), 1, {0}, true);

    // Continue after the two-record page, then probe beyond the last full page.
    check(to, to, 2, {all[2]}, false);
    const TLogoBlobID outside(1, 1, reverse ? 0 : 4, 0, 100, 0);
    check(outside, outside, 2, {}, false);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDSProxyVDiskMock) {
    Y_UNIT_TEST(ForwardRangePages) {
        CheckRangePages(false);
    }

    Y_UNIT_TEST(ReverseRangePages) {
        CheckRangePages(true);
    }
}

} // namespace NKikimr
