#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_output_estimate.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <deque>

namespace NKikimr {

    class THull;

    ////////////////////////////////////////////////////////////////////////////
    // TFreshAdmissionGate -- admits writes to Fresh only against chunks already
    // reserved for compacting it.
    //
    // Each write is charged what compacting it into an SST will take, before it
    // gets a log record. If the Fresh segments it lands in already hold enough
    // reserved chunks for everything in them, everything in flight and this write,
    // it goes ahead at once. Otherwise the missing chunks are asked of PDisk, bounded
    // by the color at which the write itself would be refused, and the write waits
    // for the answer: it goes ahead on success and is refused OUT_OF_SPACE if PDisk
    // declines. Writes also wait while a Fresh segment is due to rotate, so that
    // rotation happens with nothing in flight (see TFreshData).
    //
    // A write that would push a Fresh segment past one SST rotates that segment out
    // first, if it can -- that is, unless the previous one is still compacting --
    // so every segment compacts into exactly one SST instead of leaving a second,
    // nearly empty one behind.
    //
    // Waiting writes queue in arrival order, and everything arriving behind them
    // queues too. One reservation is in flight at a time.
    //
    // Only client writes come here: TEvVPut and TEvVMultiPut (for a huge blob, its
    // index record), TEvVBlock and TEvVCollectGarbage. What serves replication and
    // recovery -- local sync data, Anubis/Osiris, recovered huge blobs, detected
    // phantoms -- is still put into Fresh ungated. Those records are charged to the
    // segment like any other, so they use up its reservation, and a segment they
    // take past it has its compaction reserve the missing chunks as housekeeping
    // (BSHC50). Fresh compaction needs no chunk beyond the reserved ones only as long
    // as these writers stay within what rounding up to whole chunks leaves spare.
    ////////////////////////////////////////////////////////////////////////////
    class TFreshAdmissionGate {
    public:
        enum class EDecision {
            Admitted, // charged as in flight; each log record carries its own part of the admission
            Wait,     // park the event; it is handled again once it can be decided
            Refused,  // answer OUT_OF_SPACE
        };

        // Handles a parked event again, exactly as if it had just arrived.
        using TRedispatch = std::function<void(std::unique_ptr<IEventHandle>, const TActorContext&)>;

        TFreshAdmissionGate(TIntrusivePtr<TVDiskContext> vctx, TPDiskCtxPtr pdiskCtx, std::shared_ptr<THull> hull,
            TRedispatch redispatch);

        // An arriving event has to go behind the ones already waiting.
        bool MustQueue() const {
            return !Parked.empty() && !Draining;
        }

        // `housekeeping` marks writes that serve reclaiming space, such as garbage collection: their chunks are
        // not held back by the static group reserve (see TEvChunkReserve::ForHousekeeping).
        EDecision Decide(const TFreshAdmission& admission, ESpaceColor refuseAtColor, bool housekeeping,
            const TActorContext& ctx);

        template <typename TEvPtr>
        void Park(TEvPtr& ev) {
            Park(std::unique_ptr<IEventHandle>(ev.Release()));
        }
        void Park(std::unique_ptr<IEventHandle> ev);

        // The admitted records have been replayed into Fresh, or never will be.
        void Land(const TFreshAdmission& admission, const TActorContext& ctx);

        // Tries the waiting events again, e.g. after landing let a pending rotation happen.
        void Kick(const TActorContext& ctx);

        // Status other than OK and OUT_OF_SPACE is the caller's to handle, and so are the color and headroom
        // PDisk reports: they already include the chunks just reserved, and later writes are judged by them.
        void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx);

        void RenderHtml(IOutputStream& str) const;

    private:
        struct TReservation {
            TFreshShortfall Split;
            ESpaceColor RefuseAtColor;
            bool Housekeeping;
        };

        const TIntrusivePtr<TVDiskContext> VCtx;
        const TPDiskCtxPtr PDiskCtx;
        const std::shared_ptr<THull> Hull;
        const TRedispatch Redispatch;

        std::deque<std::unique_ptr<IEventHandle>> Parked;
        std::optional<TReservation> InFlight;
        // The loosest bound PDisk has declined, for ordinary and for housekeeping reservations, until it grants
        // one at that bound or a stricter one. A write no looser is refused without asking again. Forgotten once
        // nothing waits, so a write arriving later always asks.
        std::optional<ESpaceColor> RefusedAtColor[2];
        bool Draining = false;
        bool StopDraining = false;

        void Drain(const TActorContext& ctx);
    };

} // NKikimr
