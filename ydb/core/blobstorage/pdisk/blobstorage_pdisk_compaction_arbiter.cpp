#include "blobstorage_pdisk_compaction_arbiter.h"

namespace NKikimr::NPDisk {

    namespace {

        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        // Pressure goes off one colour below the one it came on at, so that a pool hovering around the boundary does
        // not flip it back and forth with every chunk.
        TColor::E OffColor(TColor::E admissionColor) {
            static constexpr TColor::E ladder[] = {
                TColor::GREEN,
                TColor::CYAN,
                TColor::LIGHT_YELLOW,
                TColor::YELLOW,
                TColor::LIGHT_ORANGE,
                TColor::PRE_ORANGE,
                TColor::ORANGE,
                TColor::RED,
                TColor::BLACK,
            };
            TColor::E res = TColor::GREEN;
            for (TColor::E color : ladder) {
                if (color < admissionColor) {
                    res = color;
                }
            }
            return res;
        }

    } // anonymous

    TCompactionArbiter::TCompactionArbiter(TColor::E admissionColor)
        : AdmissionColor(admissionColor)
    {}

    void TCompactionArbiter::SetAdmissionColor(TColor::E color, const ISpace& space, TOutbox& out) {
        AdmissionColor = color;
        OnSpaceChanged(space, out);
    }

    void TCompactionArbiter::Handle(const TEvCompactionBidder& ev, const TActorId& sender, const ISpace& space,
            TOutbox& out) {
        using EKind = TEvCompactionBidder::EKind;

        if (ev.Kind == EKind::Register) {
            const TBidderKey key{ev.Owner, ev.BidderId};
            TBidder& bidder = Bidders[key];
            // A new registration under a key is a new incarnation of the bidder: whatever the old one held is gone.
            if (LeaseHolder == key) {
                LeaseHolder.reset();
            }
            const bool awaited = bidder.Awaited;
            bidder = {};
            bidder.ActorId = sender;
            bidder.OwnerRound = ev.OwnerRound;
            Send(out, sender, TEvCompactionArbiter::EKind::Pressure, Pressure, 0);
            if (RoundOpen) {
                // it joins the round in progress, which waits for its answer too
                bidder.Awaited = true;
                Awaiting += !awaited;
                Send(out, sender, TEvCompactionArbiter::EKind::CallForBids, false, RoundId);
            } else {
                Awaiting -= awaited;
                if (Pressure && !LeaseHolder) {
                    StartRound(space, out);
                }
            }
            return;
        }

        TBidder *bidder = Find(ev, sender);
        if (!bidder) {
            return; // not registered, or an earlier incarnation
        }
        const TBidderKey key{ev.Owner, ev.BidderId};

        switch (ev.Kind) {
            case EKind::Register:
                Y_ABORT();

            case EKind::Bid:
                if (!RoundOpen || ev.RoundId != RoundId || !bidder->Awaited) {
                    return; // an answer to a round that is over
                }
                bidder->Awaited = false;
                bidder->HasCandidate = ev.HasCandidate;
                bidder->NeedChunks = ev.NeedChunks;
                bidder->FreeChunks = ev.FreeChunks;
                Y_ABORT_UNLESS(Awaiting);
                if (!--Awaiting) {
                    CloseRound(space, out);
                }
                return;

            case EKind::Dirty:
                if (!Pressure) {
                    return; // it runs its compactions on its own
                }
                if (RoundOpen || LeaseHolder) {
                    DirtyPending = true; // looked into once the round is over or the lease is released
                } else {
                    StartRound(space, out);
                }
                return;

            case EKind::Release:
                if (LeaseHolder != key) {
                    return;
                }
                LeaseHolder.reset();
                StartRound(space, out);
                return;
        }
    }

    void TCompactionArbiter::OnSpaceChanged(const ISpace& space, TOutbox& out) {
        UpdatePressure(space, out);
        // more room may let a candidate of the last round fit now
        if (Pressure && !RoundOpen && !LeaseHolder) {
            TryGrant(space, out);
        }
    }

    void TCompactionArbiter::DropOwner(TOwner owner, const ISpace& space, TOutbox& out) {
        bool leaseDropped = false;
        for (auto it = Bidders.lower_bound(TBidderKey{owner, 0}); it != Bidders.end() && it->first.Owner == owner; ) {
            Erase(it++, &leaseDropped);
        }
        AfterDrop(leaseDropped, space, out);
    }

    void TCompactionArbiter::DropActor(const TActorId& actorId, const ISpace& space, TOutbox& out) {
        bool leaseDropped = false;
        for (auto it = Bidders.begin(); it != Bidders.end(); ) {
            if (it->second.ActorId == actorId) {
                Erase(it++, &leaseDropped);
            } else {
                ++it;
            }
        }
        AfterDrop(leaseDropped, space, out);
    }

    TString TCompactionArbiter::ToString() const {
        TStringStream str;
        str << "{AdmissionColor# " << TColor::E_Name(AdmissionColor)
            << " Pressure# " << Pressure
            << " RoundId# " << RoundId
            << " RoundOpen# " << RoundOpen
            << " Awaiting# " << Awaiting
            << " DirtyPending# " << DirtyPending
            << " LeaseHolder# ";
        if (LeaseHolder) {
            str << "{Owner# " << ui32(LeaseHolder->Owner) << " BidderId# " << LeaseHolder->BidderId << "}";
        } else {
            str << "none";
        }
        str << " Bidders# [";
        for (const auto& [key, bidder] : Bidders) {
            str << " {Owner# " << ui32(key.Owner) << " BidderId# " << key.BidderId
                << " Awaited# " << bidder.Awaited
                << " HasCandidate# " << bidder.HasCandidate
                << " NeedChunks# " << bidder.NeedChunks
                << " FreeChunks# " << bidder.FreeChunks << "}";
        }
        str << " ]}";
        return str.Str();
    }

    void TCompactionArbiter::UpdatePressure(const ISpace& space, TOutbox& out) {
        const TColor::E color = space.GetColor();
        const bool pressure = color >= (Pressure ? OffColor(AdmissionColor) : AdmissionColor);
        if (pressure == Pressure) {
            return;
        }
        Pressure = pressure;
        for (const auto& [key, bidder] : Bidders) {
            Send(out, bidder.ActorId, TEvCompactionArbiter::EKind::Pressure, Pressure, 0);
        }
        if (Pressure) {
            StartRound(space, out);
        } else {
            // Off the bidders go on their own. A lease already given stays with its compaction until released.
            RoundOpen = false;
            Awaiting = 0;
            DirtyPending = false;
            for (auto& [key, bidder] : Bidders) {
                bidder.Awaited = false;
                bidder.HasCandidate = false;
            }
        }
    }

    void TCompactionArbiter::StartRound(const ISpace& space, TOutbox& out) {
        if (!Pressure || RoundOpen || LeaseHolder) {
            return;
        }
        ++RoundId;
        RoundOpen = true;
        DirtyPending = false;
        Awaiting = 0;
        for (auto& [key, bidder] : Bidders) {
            bidder.Awaited = true;
            bidder.HasCandidate = false;
            ++Awaiting;
            Send(out, bidder.ActorId, TEvCompactionArbiter::EKind::CallForBids, false, RoundId);
        }
        if (!Awaiting) {
            CloseRound(space, out);
        }
    }

    void TCompactionArbiter::CloseRound(const ISpace& space, TOutbox& out) {
        Y_ABORT_UNLESS(RoundOpen && !Awaiting);
        RoundOpen = false;
        if (!TryGrant(space, out) && DirtyPending) {
            StartRound(space, out);
        }
    }

    bool TCompactionArbiter::TryGrant(const ISpace& space, TOutbox& out) {
        if (!Pressure || RoundOpen || LeaseHolder) {
            return false;
        }

        // The candidate that gives back the most; of those, the one that takes the least.
        auto better = [](const TBidder& x, const TBidder& y) {
            const i64 xNet = i64(x.FreeChunks) - i64(x.NeedChunks);
            const i64 yNet = i64(y.FreeChunks) - i64(y.NeedChunks);
            return xNet != yNet ? xNet > yNet : x.NeedChunks < y.NeedChunks;
        };

        std::map<TBidderKey, TBidder>::iterator best = Bidders.end();
        for (auto it = Bidders.begin(); it != Bidders.end(); ++it) {
            const TBidder& bidder = it->second;
            if (bidder.HasCandidate && (best == Bidders.end() || better(bidder, best->second))
                    && space.Fits(it->first.Owner, bidder.NeedChunks)) {
                best = it;
            }
        }
        if (best == Bidders.end()) {
            return false;
        }

        LeaseHolder = best->first;
        best->second.HasCandidate = false;
        Send(out, best->second.ActorId, TEvCompactionArbiter::EKind::Lease, false, RoundId);
        return true;
    }

    void TCompactionArbiter::AfterDrop(bool leaseDropped, const ISpace& space, TOutbox& out) {
        if (RoundOpen && !Awaiting) {
            CloseRound(space, out);
        } else if (leaseDropped) {
            StartRound(space, out);
        }
    }

    void TCompactionArbiter::Erase(std::map<TBidderKey, TBidder>::iterator it, bool *leaseDropped) {
        if (it->second.Awaited) {
            Y_ABORT_UNLESS(Awaiting);
            --Awaiting;
        }
        if (LeaseHolder == it->first) {
            LeaseHolder.reset();
            *leaseDropped = true;
        }
        Bidders.erase(it);
    }

    TCompactionArbiter::TBidder *TCompactionArbiter::Find(const TEvCompactionBidder& ev, const TActorId& sender) {
        const auto it = Bidders.find(TBidderKey{ev.Owner, ev.BidderId});
        if (it == Bidders.end() || it->second.ActorId != sender || it->second.OwnerRound != ev.OwnerRound) {
            return nullptr;
        }
        return &it->second;
    }

    void TCompactionArbiter::Send(TOutbox& out, const TActorId& recipient, TEvCompactionArbiter::EKind kind,
            bool pressure, ui64 roundId) {
        out.push_back({recipient, std::make_unique<TEvCompactionArbiter>(kind, pressure, roundId)});
    }

} // NKikimr::NPDisk
