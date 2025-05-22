#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/generic/xrange.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NTable {

namespace {
    /* Sample rows set for old pages compatability tests, should be
        updated with assosiated raw page collections (see data/ directory).
     */

    const NTest::TMass MassZ(new NTest::TModelStd(false), 128);
}

Y_UNIT_TEST_SUITE(NPage) {

    Y_UNIT_TEST(Encoded)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({ 0 });

        const TRow foo = *TSchemedCookRow(*lay).Col(555_u32, "foo");
        const TRow bar = *TSchemedCookRow(*lay).Col(777_u32, "bar");

        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.Group(0).ForceCompression = true; /* required for this UT only */

        TCheckIter wrap(TPartCook(lay, conf).Add(foo).Finish(), { });

        wrap.To(1).Has(foo).To(2).NoKey(bar);

        auto &part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());

        for (auto page: xrange(part.Store->PageCollectionPagesCount(0))) {
            auto *raw = part.Store->GetPage(0, page);
            auto got = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);

            if (got.Type != NPage::EPage::DataPage) {
                /* Have to check for compression only rows page */
            } else if (got.Codec == NPage::ECodec::Plain) {
                UNIT_FAIL("Test has failed to cook compressed pages");
            }
        }
    }

    Y_UNIT_TEST(ABI_002)
    {
        const auto raw = NResource::Find("abi/002_full_part.pages");

        TStringInput input(raw);

        {
            using namespace NTest;

            const TLogoBlobID label(1,2,3);

            auto part = NTest::TLoader(TStore::Restore(input), { }).Load(label);

            TPartEggs eggs{ nullptr, MassZ.Model->Scheme, { std::move(part) } };

            TWreck<TCheckIter, TPartEggs>(MassZ, 666).Do(EWreck::Cached, eggs);
        }
    }

    Y_UNIT_TEST(GroupIdEncoding) {
        NPage::TGroupId main;
        UNIT_ASSERT_VALUES_EQUAL(main.Raw(), 0u);
        NPage::TGroupId alt(1);
        UNIT_ASSERT_VALUES_EQUAL(alt.Raw(), 1u);
        NPage::TGroupId mainHist(0, true);
        UNIT_ASSERT_VALUES_EQUAL(mainHist.Raw(), 0x80000000u);
        NPage::TGroupId altHist(1, true);
        UNIT_ASSERT_VALUES_EQUAL(altHist.Raw(), 0x80000001u);
        UNIT_ASSERT(main == main);
        UNIT_ASSERT(main < alt);
        UNIT_ASSERT(main < mainHist);
        UNIT_ASSERT(alt < mainHist);
        UNIT_ASSERT(alt < altHist);
        UNIT_ASSERT(mainHist < altHist);
    }

}

}
}
