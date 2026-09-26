#include "blobstorage_pdisk_compaction_arbiter.h"

#include <library/cpp/testing/unittest/registar.h>

#include <functional>

namespace NKikimr::NPDisk {

Y_UNIT_TEST_SUITE(TCompactionArbiterTest) {

    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    using EBidder = TEvCompactionBidder::EKind;
    using EArbiter = TEvCompactionArbiter::EKind;

    struct TSpace : TCompactionArbiter::ISpace {
        TColor::E Color = TColor::GREEN;
        ui32 Free = 1000; // chunks any owner may still take

        TColor::E GetColor() const override {
            return Color;
        }

        bool Fits(TOwner /*owner*/, ui32 chunks) const override {
            return chunks < Free;
        }
    };

    struct TEnv {
        TSpace Space;
        TCompactionArbiter Arbiter{TColor::YELLOW};
        TCompactionArbiter::TOutbox Out;

        static TActorId Actor(TOwner owner, ui32 bidderId) {
            return TActorId(1, 0, owner, bidderId);
        }

        void Send(EBidder kind, TOwner owner, ui32 bidderId, TOwnerRound round = 1, std::function<void(TEvCompactionBidder&)> fill = {}) {
            TEvCompactionBidder ev(kind, owner, round, bidderId);
            if (fill) {
                fill(ev);
            }
            Arbiter.Handle(ev, Actor(owner, bidderId), Space, Out);
        }

        void Register(TOwner owner, ui32 bidderId, TOwnerRound round = 1) {
            Send(EBidder::Register, owner, bidderId, round);
        }

        void Bid(TOwner owner, ui32 bidderId, ui64 roundId, ui32 need, ui32 free) {
            Send(EBidder::Bid, owner, bidderId, 1, [&](TEvCompactionBidder& ev) {
                ev.RoundId = roundId;
                ev.HasCandidate = true;
                ev.NeedChunks = need;
                ev.FreeChunks = free;
            });
        }

        void BidNothing(TOwner owner, ui32 bidderId, ui64 roundId) {
            Send(EBidder::Bid, owner, bidderId, 1, [&](TEvCompactionBidder& ev) {
                ev.RoundId = roundId;
            });
        }

        void SetColor(TColor::E color) {
            Space.Color = color;
            Arbiter.OnSpaceChanged(Space, Out);
        }

        // messages of this kind sent to this bidder since the last call, and forget all the others
        ui32 Take(EArbiter kind, TOwner owner, ui32 bidderId) {
            ui32 n = 0;
            for (const auto& msg : Out) {
                n += msg.Recipient == Actor(owner, bidderId) && msg.Event->Kind == kind;
            }
            return n;
        }

        void Clear() {
            Out.clear();
        }

        bool Leased(TOwner owner, ui32 bidderId) const {
            const auto holder = Arbiter.GetLeaseHolder();
            return holder && holder->Owner == owner && holder->BidderId == bidderId;
        }
    };

    Y_UNIT_TEST(NoPressureNoRounds) {
        TEnv env;
        env.Register(1, 0);
        env.Register(2, 0);
        UNIT_ASSERT(!env.Arbiter.IsPressure());
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::CallForBids, 1, 0), 0);
        env.Send(EBidder::Dirty, 1, 0);
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
    }

    Y_UNIT_TEST(PressureHasHysteresis) {
        TEnv env;
        env.Register(1, 0);
        env.SetColor(TColor::LIGHT_YELLOW);
        UNIT_ASSERT(!env.Arbiter.IsPressure());
        env.SetColor(TColor::YELLOW);
        UNIT_ASSERT(env.Arbiter.IsPressure());
        // it stays on one colour below where it came on
        env.SetColor(TColor::LIGHT_YELLOW);
        UNIT_ASSERT(env.Arbiter.IsPressure());
        env.Clear();
        env.SetColor(TColor::CYAN);
        UNIT_ASSERT(!env.Arbiter.IsPressure());
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::Pressure, 1, 0), 1);
    }

    Y_UNIT_TEST(RoundWaitsForEveryBidderAndLeasesTheBestNet) {
        TEnv env;
        env.Register(1, 0);
        env.Register(1, 1);
        env.Register(2, 0);
        env.Clear();
        env.SetColor(TColor::ORANGE);
        const ui64 round = env.Arbiter.GetRoundId();
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::CallForBids, 1, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::CallForBids, 1, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::CallForBids, 2, 0), 1);

        env.Bid(1, 0, round, 10, 12); // net 2
        env.Bid(2, 0, round, 5, 20); // net 15
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
        env.Clear();
        env.Bid(1, 1, round, 1, 9); // net 8: a Barriers bid of the first VDisk
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(env.Leased(2, 0));
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::Lease, 2, 0), 1);
    }

    Y_UNIT_TEST(OnlyOneLeaseAndReleaseStartsTheNextRound) {
        TEnv env;
        env.Register(1, 0);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        ui64 round = env.Arbiter.GetRoundId();
        env.Bid(1, 0, round, 1, 10);
        env.Bid(2, 0, round, 1, 5);
        UNIT_ASSERT(env.Leased(1, 0));

        // nobody is asked while the lease is held, however much work shows up
        env.Clear();
        env.Send(EBidder::Dirty, 2, 0);
        env.SetColor(TColor::RED);
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(env.Leased(1, 0));
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::Lease, 2, 0), 0);

        env.Send(EBidder::Release, 1, 0);
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(env.Arbiter.GetRoundId() > round);
        round = env.Arbiter.GetRoundId();
        env.BidNothing(1, 0, round);
        env.Bid(2, 0, round, 1, 5);
        UNIT_ASSERT(env.Leased(2, 0));
    }

    Y_UNIT_TEST(CandidateThatDoesNotFitWaitsForSpace) {
        TEnv env;
        env.Space.Free = 10;
        env.Register(1, 0);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        const ui64 round = env.Arbiter.GetRoundId();
        env.Bid(1, 0, round, 50, 100); // the best, but does not fit
        env.Bid(2, 0, round, 5, 6);
        UNIT_ASSERT(env.Leased(2, 0)); // the smaller one is not held up behind it
        env.Send(EBidder::Release, 2, 0);

        const ui64 round2 = env.Arbiter.GetRoundId();
        env.Bid(1, 0, round2, 50, 100);
        env.BidNothing(2, 0, round2);
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());

        // space freed elsewhere: the bid of the last round fits now
        env.Space.Free = 100;
        env.Arbiter.OnSpaceChanged(env.Space, env.Out);
        UNIT_ASSERT(env.Leased(1, 0));
    }

    Y_UNIT_TEST(DirtyOpensARoundWhenIdle) {
        TEnv env;
        env.Register(1, 0);
        env.SetColor(TColor::ORANGE);
        env.BidNothing(1, 0, env.Arbiter.GetRoundId());
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
        const ui64 round = env.Arbiter.GetRoundId();
        env.Send(EBidder::Dirty, 1, 0);
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(env.Arbiter.GetRoundId() > round);
    }

    Y_UNIT_TEST(DirtyDuringARoundIsNotLost) {
        TEnv env;
        env.Register(1, 0);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        const ui64 round = env.Arbiter.GetRoundId();
        env.BidNothing(1, 0, round);
        env.Send(EBidder::Dirty, 1, 0); // it has work now, but it already answered this round
        env.BidNothing(2, 0, round);
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(env.Arbiter.GetRoundId() > round);
    }

    Y_UNIT_TEST(BidderRegisteringMidRoundIsWaitedFor) {
        TEnv env;
        env.Register(1, 0);
        env.SetColor(TColor::ORANGE);
        const ui64 round = env.Arbiter.GetRoundId();
        env.Clear();
        env.Register(2, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::Pressure, 2, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Take(EArbiter::CallForBids, 2, 0), 1);
        env.Bid(1, 0, round, 1, 5);
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        env.Bid(2, 0, round, 1, 50);
        UNIT_ASSERT(env.Leased(2, 0));
    }

    Y_UNIT_TEST(DroppedBiddersDoNotHoldAnything) {
        TEnv env;
        env.Register(1, 0);
        env.Register(1, 1);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        ui64 round = env.Arbiter.GetRoundId();
        env.Bid(2, 0, round, 1, 5);
        // the owner re-inits: the round no longer waits for its bidders
        env.Arbiter.DropOwner(1, env.Space, env.Out);
        UNIT_ASSERT_VALUES_EQUAL(env.Arbiter.GetBidderCount(), 1);
        UNIT_ASSERT(env.Leased(2, 0));

        // the lease holder is gone (a message to it came back): the next round starts
        env.Register(3, 0);
        env.Arbiter.DropActor(TEnv::Actor(2, 0), env.Space, env.Out);
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        round = env.Arbiter.GetRoundId();
        env.Bid(3, 0, round, 1, 2);
        UNIT_ASSERT(env.Leased(3, 0));
    }

    Y_UNIT_TEST(StaleMessagesAreIgnored) {
        TEnv env;
        env.Register(1, 0);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        const ui64 round = env.Arbiter.GetRoundId();
        env.Bid(1, 0, round - 1, 1, 100); // an answer to an older round
        env.Send(EBidder::Bid, 2, 0, /*round=*/7, [&](TEvCompactionBidder& ev) { // an older incarnation of the owner
            ev.RoundId = round;
            ev.HasCandidate = true;
            ev.FreeChunks = 100;
        });
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        env.Send(EBidder::Release, 1, 0); // it holds no lease
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        env.BidNothing(1, 0, round);
        env.BidNothing(2, 0, round);
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
    }

    Y_UNIT_TEST(PressureOffEndsTheRoundButNotTheLease) {
        TEnv env;
        env.Register(1, 0);
        env.Register(2, 0);
        env.SetColor(TColor::ORANGE);
        ui64 round = env.Arbiter.GetRoundId();
        env.Bid(1, 0, round, 1, 5);
        env.Bid(2, 0, round, 1, 3);
        UNIT_ASSERT(env.Leased(1, 0));
        env.SetColor(TColor::GREEN);
        UNIT_ASSERT(!env.Arbiter.IsPressure());
        UNIT_ASSERT(env.Leased(1, 0)); // its compaction is running
        env.Send(EBidder::Release, 1, 0);
        UNIT_ASSERT(!env.Arbiter.GetLeaseHolder());
        UNIT_ASSERT(!env.Arbiter.IsRoundOpen());

        // on again: a round, and a bid from before it does not count
        env.SetColor(TColor::ORANGE);
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
        env.Bid(2, 0, round, 1, 3);
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
    }

    Y_UNIT_TEST(AdmissionColorGreenMeansAlways) {
        TEnv env;
        env.Register(1, 0);
        env.Arbiter.SetAdmissionColor(TColor::GREEN, env.Space, env.Out);
        UNIT_ASSERT(env.Arbiter.IsPressure());
        UNIT_ASSERT(env.Arbiter.IsRoundOpen());
    }
}

} // NKikimr::NPDisk
