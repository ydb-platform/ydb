#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_comp.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_iter.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

using namespace NTest;

using TCheckIter = TChecker<TWrapIter, TSubset>;
using TCheckReverseIter = TChecker<TWrapReverseIter, TSubset>;

namespace {
    NPage::TConf PageConf(size_t groups) noexcept
    {
        NPage::TConf conf{ false, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).IndexMin = 1024; /* Should cover index buffer grow code */
            conf.Group(group).BTreeIndexNodeTargetSize = 128; /* Should cover up/down moves */
        }
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */

        return conf;
    }

    const TMass& Mass0()
    {
        static const TMass mass(new TModelStd(true), 24000, /* seed = */ 1);
        return mass;
    }

    const TMass& Mass1()
    {
        static const TMass mass(new TModelStd(true), 24000, /* seed = */ 2);
        return mass;
    }

    const TMass& Mass2()
    {
        static const TMass mass(new TModelStd(true), 24000, /* seed = */ 3);
        return mass;
    }

    TSubset MakeSubset(TIntrusiveConstPtr<TRowScheme> scheme, TVector<const TPartEggs*> eggs)
    {
        TVector<TPartView> partView;

        for (auto &one: eggs) {
            for (const auto &part : one->Parts) {
                Y_ABORT_UNLESS(part->Slices, "Missing part slices");
                partView.push_back({ part, nullptr, part->Slices });
            }
        }

        return TSubset(TEpoch::Zero(), std::move(scheme), std::move(partView));
    }

    const TSubset& Subset()
    {
        static const auto& mass0 = Mass0();
        static const auto& mass1 = Mass1();
        static const auto& mass2 = Mass2();

        static const auto conf = PageConf(4);

        static const auto eggs0_n = TPartCook::Make(mass0, conf, TLogoBlobID(1, 2, 11, 1, 0, 0), TEpoch::FromIndex(1), TRowVersion(1, 10));
        static const auto eggs0_e = TPartCook::Make(mass0, conf, TLogoBlobID(1, 2, 12, 1, 0, 0), TEpoch::FromIndex(2), TRowVersion(2, 20), ERowOp::Erase);
        static const auto eggs1_n = TPartCook::Make(mass1, conf, TLogoBlobID(1, 2, 13, 1, 0, 0), TEpoch::FromIndex(3), TRowVersion(3, 30));
        static const auto eggs1_e = TPartCook::Make(mass1, conf, TLogoBlobID(1, 2, 14, 1, 0, 0), TEpoch::FromIndex(4), TRowVersion(4, 40), ERowOp::Erase);
        static const auto eggs2_n = TPartCook::Make(mass2, conf, TLogoBlobID(1, 2, 15, 1, 0, 0), TEpoch::FromIndex(5), TRowVersion(5, 50));

        static const auto eggs = TCompaction(new TForwardEnv(512, 1024), conf)
            .Do(eggs0_n.Scheme, { &eggs0_n, &eggs0_e, &eggs1_n, &eggs1_e, &eggs2_n });

        static const auto subset = MakeSubset(eggs.Scheme, { &eggs });

        return subset;
    }
}

Y_UNIT_TEST_SUITE(TVersions) {

    Y_UNIT_TEST(WreckHead)
    {
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Cached, Subset());
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Evicted, Subset());
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Forward, Subset());
    }

    Y_UNIT_TEST(WreckHeadReverse)
    {
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass2(), 666).Do(EWreck::Cached, Subset());
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass2(), 666).Do(EWreck::Evicted, Subset());
    }

    Y_UNIT_TEST(Wreck2)
    {
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Cached, Subset(), TRowVersion(5, 50));
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(5, 50));
        TWreck<TCheckIter, TSubset>(Mass2(), 666).Do(EWreck::Forward, Subset(), TRowVersion(5, 50));
    }

    Y_UNIT_TEST(Wreck2Reverse)
    {
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass2(), 666).Do(EWreck::Cached, Subset(), TRowVersion(5, 50));
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass2(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(5, 50));
    }

    Y_UNIT_TEST(Wreck1)
    {
        TWreck<TCheckIter, TSubset>(Mass1(), 666).Do(EWreck::Cached, Subset(), TRowVersion(3, 30));
        TWreck<TCheckIter, TSubset>(Mass1(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(3, 30));
        TWreck<TCheckIter, TSubset>(Mass1(), 666).Do(EWreck::Forward, Subset(), TRowVersion(3, 30));
    }

    Y_UNIT_TEST(Wreck1Reverse)
    {
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass1(), 666).Do(EWreck::Cached, Subset(), TRowVersion(3, 30));
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass1(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(3, 30));
    }

    Y_UNIT_TEST(Wreck0)
    {
        TWreck<TCheckIter, TSubset>(Mass0(), 666).Do(EWreck::Cached, Subset(), TRowVersion(1, 10));
        TWreck<TCheckIter, TSubset>(Mass0(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(1, 10));
        TWreck<TCheckIter, TSubset>(Mass0(), 666).Do(EWreck::Forward, Subset(), TRowVersion(1, 10));
    }

    Y_UNIT_TEST(Wreck0Reverse)
    {
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0(), 666).Do(EWreck::Cached, Subset(), TRowVersion(1, 10));
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0(), 666).Do(EWreck::Evicted, Subset(), TRowVersion(1, 10));
    }

}

}
}
