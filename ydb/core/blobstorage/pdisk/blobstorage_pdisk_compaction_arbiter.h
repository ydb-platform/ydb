#pragma once

#include "defs.h"
#include "blobstorage_pdisk.h"

#include <compare>
#include <map>
#include <optional>
#include <vector>

namespace NKikimr::NPDisk {

    ////////////////////////////////////////////////////////////////////////////
    // TCompactionArbiter
    //
    // Decides which level compaction may run on a PDisk whose shared chunk pool is short of space
    // (EnableVDiskPlannedCompaction). Parallel compactions there can each take part of what is left, so that none of
    // them finishes; one at a time, and the one that gives back the most, is what gets the disk out of it.
    //
    // While the pool is at or past the admission colour ("pressure"), the arbiter asks every registered bidder for its
    // best candidate (a round), waits until all of them have answered, and leases the disk to the candidate that frees
    // the most chunks net among those that fit. Nothing else is leased until that one is released. The arbiter keeps
    // no timers: a bidder always answers, and one that cannot is dropped by an event -- a message to it comes back
    // undelivered, or its owner re-inits or goes away.
    //
    // A pure state machine: TPDisk feeds it what it hears and what happens to the space, and sends what it returns.
    ////////////////////////////////////////////////////////////////////////////
    class TCompactionArbiter {
    public:
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        struct TOutgoing {
            TActorId Recipient;
            std::unique_ptr<TEvCompactionArbiter> Event;
        };
        using TOutbox = std::vector<TOutgoing>;

        // What the arbiter needs to know about the space.
        struct ISpace {
            virtual ~ISpace() = default;
            // colour of the shared chunk pool
            virtual TColor::E GetColor() const = 0;
            // whether a housekeeping reservation of `chunks` for `owner` would succeed right now
            virtual bool Fits(TOwner owner, ui32 chunks) const = 0;
        };

        struct TBidderKey {
            TOwner Owner;
            ui32 BidderId;

            auto operator<=>(const TBidderKey&) const = default;
        };

    public:
        explicit TCompactionArbiter(TColor::E admissionColor);

        void SetAdmissionColor(TColor::E color, const ISpace& space, TOutbox& out);

        // A message from a bidder. The caller has checked that its owner and round are the current ones.
        void Handle(const TEvCompactionBidder& ev, const TActorId& sender, const ISpace& space, TOutbox& out);

        // The free space or the colour may have changed.
        void OnSpaceChanged(const ISpace& space, TOutbox& out);

        // The owner re-inits or goes away: all its bidders and whatever they hold are gone.
        void DropOwner(TOwner owner, const ISpace& space, TOutbox& out);

        // A message to this actor came back undelivered: it is gone.
        void DropActor(const TActorId& actorId, const ISpace& space, TOutbox& out);

        bool IsPressure() const { return Pressure; }
        bool IsRoundOpen() const { return RoundOpen; }
        ui64 GetRoundId() const { return RoundId; }
        std::optional<TBidderKey> GetLeaseHolder() const { return LeaseHolder; }
        size_t GetBidderCount() const { return Bidders.size(); }
        TColor::E GetAdmissionColor() const { return AdmissionColor; }
        TString ToString() const;

    private:
        struct TBidder {
            TActorId ActorId;
            TOwnerRound OwnerRound = 0;
            bool Awaited = false; // owes a bid for the open round
            bool HasCandidate = false; // its bid from the last round it answered, while not yet leased
            ui32 NeedChunks = 0;
            ui32 FreeChunks = 0;
        };

        void UpdatePressure(const ISpace& space, TOutbox& out);
        void StartRound(const ISpace& space, TOutbox& out);
        void CloseRound(const ISpace& space, TOutbox& out);
        bool TryGrant(const ISpace& space, TOutbox& out);
        void AfterDrop(bool leaseDropped, const ISpace& space, TOutbox& out);
        void Erase(std::map<TBidderKey, TBidder>::iterator it, bool *leaseDropped);
        TBidder *Find(const TEvCompactionBidder& ev, const TActorId& sender);
        static void Send(TOutbox& out, const TActorId& recipient, TEvCompactionArbiter::EKind kind, bool pressure,
            ui64 roundId);

    private:
        TColor::E AdmissionColor;
        std::map<TBidderKey, TBidder> Bidders;
        bool Pressure = false;
        ui64 RoundId = 0;
        bool RoundOpen = false;
        ui32 Awaiting = 0; // bidders that owe a bid for the open round
        bool DirtyPending = false; // some bidder said it may have work while a round was open or a lease was held
        std::optional<TBidderKey> LeaseHolder;
    };

} // NKikimr::NPDisk
