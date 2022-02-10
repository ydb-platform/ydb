#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_mem_iter.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_iterator.h>
#include <ydb/core/tablet_flat/flat_part_slice.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_pretty.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_iter.h>

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {

namespace {
    static NPage::TConf Conf(ui32 page = NPage::TConf().Groups.at(0).PageSize) noexcept
    {
        NPage::TConf conf;

        conf.Group(0).PageSize = page;
        conf.LargeEdge = 29;  /* neet to cover external blob usage */

        return conf;
    }

    const NTest::TMass Mass0(new NTest::TModelStd(false), 6666);

    static void VerifySingleLevelNonTrivial(const TAutoPtr<TSubset>& subset)
    {
        /* parts form a single run and have non-trivial pages */
        TLevels levels(subset->Scheme->Keys);

        for (const auto &partView : subset->Flatten) {
            levels.Add(partView.Part, partView.Slices);

            auto *partStore = partView.As<NTest::TPartStore>();

            if (partStore->Store->PageCollectionArray(0).size() * 3 > partStore->Stat.Rows) {
                UNIT_ASSERT_C(false, "Part has too few rows per page"
                    << ", rows " << partStore->Stat.Rows
                    << ", pages " << partStore->Store->PageCollectionArray(0).size());
            }
        }

        UNIT_ASSERT_C(levels.size() == 1, "Levels size " << levels.size());
    }
}

using TCheckIt = NTest::TChecker<NTest::TWrapIter, TSubset>;
using TCheckReverseIt = NTest::TChecker<NTest::TWrapReverseIter, TSubset>;

Y_UNIT_TEST_SUITE(TIterator) {
    using namespace NTest;

    Y_UNIT_TEST(Basics)
    {
        TCheckIt wrap(*TMake(Mass0).Mixed(0, 1, TMixerOne{ }), { });

        { /*_ ESeek::Exact have to give at most one row */
            wrap.To(1).Seek(Mass0.Saved[7], ESeek::Exact).Is(Mass0.Saved[7]);
            wrap.To(2).Next().Is(EReady::Gone);
        }

        { /*_ Seek w/o pages have to yield EReady::Page forever */
            auto *env = new TNoEnv{ false, ELargeObjNeed::Has };

            wrap.Displace<IPages>(env);

            wrap.To(6).Seek(Mass0.Saved[7], ESeek::Lower).Is(EReady::Page);
            env->Pages = true; /* load all pages to cache */
            wrap.To(7).Next().Is(EReady::Page).Next().Is(EReady::Page);
        }
    }

    Y_UNIT_TEST(External)
    {
        TCheckIt wrap(*TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ }), { });

        UNIT_ASSERT((*wrap).Flatten.size() == 1);

        auto *part = (*wrap).Flatten.at(0).Part.Get();

        UNIT_ASSERT(part && part->Large && part->Blobs);

        /* Work on the second row with external blobs, should check how
            iterator behaves on absense of blobs in cache on Next().
         */

        const auto frame = ui32(0) - part->Large->Relation(0).Refer;
        const auto on = part->Large->Relation(frame).Row;

        auto *env = new TNoEnv{ true, ELargeObjNeed::Has };

        wrap.Displace<IPages>(env);
        wrap.To(1).Seek(Mass0.Saved[on-1], ESeek::Lower).Is(Mass0.Saved[on-1]);
        env->Lobs = ELargeObjNeed::Yes; /* no any blobs in cache */
        wrap.To(2).Next().Is(EReady::Page).To(3).Next().Is(EReady::Page);
        env->Lobs = ELargeObjNeed::Has; /* load all blobs to cache */
        wrap.To(8).Next().Is(Mass0.Saved[on]);
    }

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ });

        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
    }

    Y_UNIT_TEST(SingleReverse)
    {
        auto subset = TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ });

        TWreck<TCheckReverseIt, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, TMixerRnd(4));

        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Evicted, *subset);
        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Forward, *subset);
    }

    Y_UNIT_TEST(MixedReverse)
    {
        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, TMixerRnd(4));

        TWreck<TCheckReverseIt, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckReverseIt, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Evicted, *subset);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());

        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, mixer);

        VerifySingleLevelNonTrivial(subset);

        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Evicted, *subset);
        TWreck<TCheckIt, TSubset>(Mass0, 666).Do(EWreck::Forward, *subset);
    }

    Y_UNIT_TEST(SerialReverse)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());

        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, mixer);

        VerifySingleLevelNonTrivial(subset);

        TWreck<TCheckReverseIt, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckReverseIt, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Evicted, *subset);
    }

}

}
}
