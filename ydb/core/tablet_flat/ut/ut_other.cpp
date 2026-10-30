#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/tablet_flat/flat_executor_misc.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_writer_conf.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/flat_sausage_chop.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(NOther) {
    Y_UNIT_TEST(CompactOutputDefaultMaterialization)
    {
        const NTable::TScheme::TFamily leaderFamily;
        const TCompactCfg::TOutputConf conf;

        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeOuterEdge, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeExtEdge, leaderFamily.Large);
    }

    Y_UNIT_TEST(CompactOutputLeaderFamilyMaterialization)
    {
        NTable::TScheme::TFamily leaderFamily;
        leaderFamily.Small = 64;
        leaderFamily.Large = 512;

        TCompactCfg::TOutputConf conf(leaderFamily);
        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeOuterEdge, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeExtEdge, 512);

        conf.Layout.SmallEdge = Max<ui32>();
        conf.Layout.LargeEdge = 1024;
        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeOuterEdge, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(conf.MaterializeExtEdge, 512);

        conf.MaterializeOuterEdge = 0;
        conf.MaterializeExtEdge = Max<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(conf.Layout.SmallEdge, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(conf.Layout.LargeEdge, 1024);
    }

    Y_UNIT_TEST(Blocks)
    {
        NWriter::TConf conf;

        conf.Groups[0].MaxBlobSize = 128;       /* Page collection blob size */
        conf.Slots = { { 1, 11 }, { 3, 13 }, { 5, 17 } };
        conf.Groups[0].Channel = 3;     /* Put data to channel 3 grp 13 */
        conf.BlobsChannels = {5};    /* Put blobs to channel 5 grp 17 */
        conf.ChannelsShares = NUtil::TChannelsShares({{5, 1.0f}});

        const TLogoBlobID mask(1, 3, 7, 0, 0, 0);

        TAutoPtr<NWriter::TBundle> bundle = new NWriter::TBundle(mask, conf);

        if (auto *out = static_cast<NTable::IPageWriter*>(bundle.Get())) {
            for (auto seq: xrange(7))
                out->Write(TSharedData::Copy(TString(196, 'a' + seq)), NTable::EPage::Undef, 0);

            for (auto seq: xrange(3))
                out->WriteLarge(TString(200, 'b' + seq), seq); /* external blobs */

            out->Finish({ });
        }

        auto globs = bundle->GetBlobsToSave();

        auto results = bundle->Results();
        UNIT_ASSERT(results);
        UNIT_ASSERT_VALUES_EQUAL(results[0].PageCollections.at(0).PageCollection->Total(), 7);
        UNIT_ASSERT_VALUES_EQUAL(results[0].PageCollections.at(0).RegularPages.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(results[0].PageCollections.at(0).StickyPages.size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(globs.size(), 18 /* 11 page collections + 4 meta + 3 external */);

        /*_ Ensure that writer places blobs to the correct channel and grp */

        for (auto &one: globs) {
            UNIT_ASSERT(NPageCollection::TGroupBlobsByCookie::IsInPlane(one.GId.Logo, mask));
            UNIT_ASSERT(
                (one.GId.Group == 13 && one.GId.Logo.Channel() == 3)
                ||(one.GId.Group == 17 && one.GId.Logo.Channel() == 5));
        }

        /*_ Ensure external blob references are accounted correctly */
        UNIT_ASSERT_VALUES_EQUAL(results[0].Growth.size(), 1);
        UNIT_ASSERT(results[0].Growth[0] == NTable::TScreen::THole(0, 3));
    }
}

}
}
