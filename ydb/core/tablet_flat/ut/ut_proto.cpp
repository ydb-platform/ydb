#include <ydb/core/tablet_flat/flat_sausage_solid.h>
#include <ydb/core/tablet_flat/flat_store_solid.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(NProto /* executor binary units */) {
    Y_UNIT_TEST(LargeGlobId)
    {
        NKikimrExecutorFlat::TPageCollection pageCollection;

        NPageCollection::TLargeGlobId largeGlobId(13, TLogoBlobID{ 1, 3, 7, 11, 10, 0 }, 77);

        UNIT_ASSERT_VALUES_EQUAL(largeGlobId.BlobCount(), 8u);

        ui32 num = 0;
        for (const auto& logo : largeGlobId.Blobs()) {
            ++num;
            LogoBlobIDFromLogoBlobID(logo, pageCollection.AddMetaId());

            UNIT_ASSERT_C(num <= 8 && logo.Cookie() + 1 == num, "num=" << num << " logo=" << logo);
            UNIT_ASSERT_C(logo.BlobSize() == (num == 8 ? 7 : 10), "num=" << num << " logo=" << logo);
        }
        UNIT_ASSERT_VALUES_EQUAL(num, 8u);

        auto lookup = [](const TLogoBlobID&) { return 13; };

        auto other = TLargeGlobIdProto::Get(pageCollection.GetMetaId(), lookup);

        UNIT_ASSERT(other == largeGlobId);
    }
}

}
}
