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

    struct TFirstTimeFailEnv : public NTest::TTestEnv {
        TFirstTimeFailEnv() = default;

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override {
            if (lob == ELargeObj::Extern) {
                if (SeenExtern.insert(ref).second) {
                    return { true, nullptr };
                }
            }

            return NTest::TTestEnv::Locate(part, ref, lob);
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
            if (Seen[groupId].insert(pageId).second) {
                return nullptr;
            }

            return NTest::TTestEnv::TryGetPage(part, pageId, groupId);
        }

        THashSet<ui64> SeenExtern;
        THashMap<TGroupId, THashSet<TPageId>> Seen;
    };

    Y_UNIT_TEST(GetKey)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0, NScheme::NTypeIds::String)
            .Col(0, 1, NScheme::NTypeIds::String)
            .Key({ 0 });

        // Note: we use compressed pages because they are volatile and freed
        // when advancing to the next page. We also force values to external
        // blobs so page faults are triggered after each advance to the next
        // page, which causes key reference to be invalidated unless key page
        // is pinned in memory.
        auto conf = NPage::TConf();
        conf.Group(0).PageSize = 256;
        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.LargeEdge = 24;

        auto cook = TPartCook(lay, conf);
        for (int i = 1; i <= 1000; ++i) {
            TString key = Sprintf("not_inline_and_long_key_%04d", i);
            TString value = Sprintf("not_inline_and_long_value_%04d", i);
            cook.AddN(key, value);
        }

        auto cooked = cook.Finish();

        TSubset subset(TEpoch::Zero(), cooked.Scheme);
        for (const auto& part : cooked.Parts) {
            subset.Flatten.push_back({ part, nullptr, part->Slices });
        }

        TCheckIt wrap(subset, { new TFirstTimeFailEnv });

        // Must page fault the first couple of times
        wrap.To(0).Seek({}, ESeek::Lower).Is(EReady::Page);
        do {
            wrap.Seek({}, ESeek::Lower);
        } while (wrap.GetReady() == EReady::Page);
        wrap.To(1).IsOpN(ERowOp::Upsert,
            "not_inline_and_long_key_0001",
            "not_inline_and_long_value_0001");

        TString lastSeen = TString(wrap->GetKey().Cells().at(0).AsBuf());
        UNIT_ASSERT_VALUES_EQUAL(lastSeen, "not_inline_and_long_key_0001");
        int nextExpected = 2;
        size_t pageFaults = 0;
        for (;;) {
            wrap.Next();

            if (wrap.GetReady() == EReady::Gone) {
                UNIT_ASSERT(wrap->GetKey().Cells().empty());
                break;
            }

            TString currentKey = TString(wrap->GetKey().Cells().at(0).AsBuf());
            if (wrap.GetReady() == EReady::Page) {
                UNIT_ASSERT_VALUES_EQUAL(currentKey, lastSeen);
                ++pageFaults;
                continue;
            }

            wrap.To(nextExpected).IsOpN(ERowOp::Upsert,
                Sprintf("not_inline_and_long_key_%04d", nextExpected),
                Sprintf("not_inline_and_long_value_%04d", nextExpected));
            UNIT_ASSERT_VALUES_EQUAL(currentKey, Sprintf("not_inline_and_long_key_%04d", nextExpected));
            lastSeen = currentKey;
            ++nextExpected;
        }

        UNIT_ASSERT_VALUES_EQUAL(nextExpected, 1001);
        UNIT_ASSERT_C(pageFaults >= 3, "Not enough page faults: " << pageFaults);
    }

}

}
}
