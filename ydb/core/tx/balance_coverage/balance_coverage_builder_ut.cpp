#include "balance_coverage_builder.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace NTxDataShard {

Y_UNIT_TEST_SUITE(TBalanceCoverageBuilderTest) {
    Y_UNIT_TEST(TestZeroTracks) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks;
        bool res = builder.AddResult(tracks);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestEmpty) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks;
        auto track = tracks.MutableTrack()->Add();
        track->AddHop()->SetShard(1);
        bool res = builder.AddResult(tracks);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestOneSplit) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks;
        auto track = tracks.MutableTrack()->Add();
        track->AddHop()->SetShard(1);
        track->AddHop()->SetShard(2);
        bool res = builder.AddResult(tracks);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestSimpleSplit) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1 = tracks1.MutableTrack()->Add();
        track1->AddHop()->SetShard(1);
        track1->AddHop()->SetShard(2);
        track1->MutableHop(track1->HopSize() - 1)->AddSibling(3);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks2;
        auto track2 = tracks2.MutableTrack()->Add();
        track2->AddHop()->SetShard(1);
        track2->AddHop()->SetShard(3);
        track2->MutableHop(track2->HopSize() - 1)->AddSibling(2);
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestComplexSplit) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1 = tracks1.MutableTrack()->Add();
        track1->AddHop()->SetShard(1);
        track1->AddHop()->SetShard(2);
        track1->MutableHop(track1->HopSize() - 1)->AddSibling(3);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks2;
        auto track2 = tracks2.MutableTrack()->Add();
        track2->AddHop()->SetShard(1);
        track2->AddHop()->SetShard(3);
        track2->AddHop()->SetShard(4);
        track2->MutableHop(track2->HopSize() - 1)->AddSibling(5);
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks3;
        auto track3 = tracks3.MutableTrack()->Add();
        track3->AddHop()->SetShard(1);
        track3->AddHop()->SetShard(3);
        track3->AddHop()->SetShard(5);
        track3->MutableHop(track3->HopSize() - 1)->AddSibling(4);
        res = builder.AddResult(tracks3);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestComplexSplitWithDuplicates) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1 = tracks1.MutableTrack()->Add();
        track1->AddHop()->SetShard(1);
        track1->AddHop()->SetShard(2);
        track1->MutableHop(track1->HopSize() - 1)->AddSibling(3);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        res = builder.AddResult(tracks1);
        UNIT_ASSERT(!res);
        NKikimrTx::TBalanceTrackList tracks2;
        auto track2 = tracks2.MutableTrack()->Add();
        track2->AddHop()->SetShard(1);
        track2->AddHop()->SetShard(3);
        track2->AddHop()->SetShard(4);
        track2->MutableHop(track2->HopSize() - 1)->AddSibling(5);
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(!res);
        NKikimrTx::TBalanceTrackList tracks3;
        auto track3 = tracks3.MutableTrack()->Add();
        track3->AddHop()->SetShard(1);
        track3->AddHop()->SetShard(3);
        track3->AddHop()->SetShard(5);
        track3->MutableHop(track3->HopSize() - 1)->AddSibling(4);
        res = builder.AddResult(tracks3);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
        res = builder.AddResult(tracks3);
        UNIT_ASSERT(!res);
    }

    Y_UNIT_TEST(TestSplitWithPartialMergeOne) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1 = tracks1.MutableTrack()->Add();
        track1->AddHop()->SetShard(1);
        track1->AddHop()->SetShard(2);
        track1->MutableHop(track1->HopSize() - 1)->AddSibling(3);
        track1->MutableHop(track1->HopSize() - 1)->AddSibling(3);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks2;
        auto track2a = tracks2.MutableTrack()->Add();
        track2a->AddHop()->SetShard(1);
        track2a->AddHop()->SetShard(3);
        track2a->AddHop()->SetShard(5);
        auto track2b = tracks2.MutableTrack()->Add();
        track2b->AddHop()->SetShard(1);
        track2b->AddHop()->SetShard(4);
        track2b->AddHop()->SetShard(5);
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestSplitWithPartialMergeAll) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1a = tracks1.MutableTrack()->Add();
        track1a->AddHop()->SetShard(1);
        track1a->AddHop()->SetShard(2);
        track1a->MutableHop(track1a->HopSize() - 1)->AddSibling(3);
        track1a->MutableHop(track1a->HopSize() - 1)->AddSibling(4);
        track1a->MutableHop(track1a->HopSize() - 1)->AddSibling(5);
        track1a->AddHop()->SetShard(6);
        auto track1b = tracks1.MutableTrack()->Add();
        track1b->AddHop()->SetShard(1);
        track1b->AddHop()->SetShard(3);
        track1b->MutableHop(track1b->HopSize() - 1)->AddSibling(2);
        track1b->MutableHop(track1b->HopSize() - 1)->AddSibling(4);
        track1b->MutableHop(track1b->HopSize() - 1)->AddSibling(5);
        track1b->AddHop()->SetShard(6);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks2;
        auto track2a = tracks2.MutableTrack()->Add();
        track2a->AddHop()->SetShard(1);
        track2a->AddHop()->SetShard(4);
        track2a->MutableHop(track2a->HopSize() - 1)->AddSibling(2);
        track2a->MutableHop(track2a->HopSize() - 1)->AddSibling(3);
        track2a->MutableHop(track2a->HopSize() - 1)->AddSibling(5);
        track2a->AddHop()->SetShard(7);
        auto track2b = tracks2.MutableTrack()->Add();
        track2b->AddHop()->SetShard(1);
        track2b->AddHop()->SetShard(5);
        track2b->MutableHop(track2b->HopSize() - 1)->AddSibling(2);
        track2b->MutableHop(track2b->HopSize() - 1)->AddSibling(3);
        track2b->MutableHop(track2b->HopSize() - 1)->AddSibling(4);
        track2b->AddHop()->SetShard(7);
        res = builder.AddResult(tracks2);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }

    Y_UNIT_TEST(TestSplitWithMergeBack) {
        TBalanceCoverageBuilder builder;
        UNIT_ASSERT(!builder.IsComplete());
        NKikimrTx::TBalanceTrackList tracks1;
        auto track1a = tracks1.MutableTrack()->Add();
        track1a->AddHop()->SetShard(1);
        track1a->AddHop()->SetShard(2);
        track1a->MutableHop(track1a->HopSize() - 1)->AddSibling(3);
        track1a->AddHop()->SetShard(4);
        auto track1b = tracks1.MutableTrack()->Add();
        track1b->AddHop()->SetShard(1);
        track1b->AddHop()->SetShard(3);
        track1b->MutableHop(track1b->HopSize() - 1)->AddSibling(2);
        track1b->AddHop()->SetShard(4);
        bool res = builder.AddResult(tracks1);
        UNIT_ASSERT(res);
        UNIT_ASSERT(builder.IsComplete());
    }
}

}

}
