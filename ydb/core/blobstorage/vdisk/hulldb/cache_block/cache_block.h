#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/defs.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////
    // TBlocksCache -- fast cache for VDisk blocks; we build it once at some point
    // after/during restart and later keep it actual
    ///////////////////////////////////////////////////////////////////////////////
    class TBlocksCache {
    public:
        enum class EStatus : ui8 {
            OK = 0,
            BLOCKED_PERS = 1,
            BLOCKED_INFLIGH = 2
        };

        struct TBlockRes {
            EStatus Status;
            // when Status == EStatus::BLOCKED_INFLIGH the field Lsn shows after confirmation of
            // which Lsn we can report BLOCKED to the user
            ui64 Lsn;

            void Output(IOutputStream &str) const;
            TString ToString() const;
        };

        struct TBlockedGen {
            ui32 Generation = 0;
            ui64 IssuerGuid = 0;

            TBlockedGen() = default;
            TBlockedGen(const TBlockedGen&) = default;

            TBlockedGen(ui32 generation, ui64 issuerGuid)
                : Generation(generation)
                , IssuerGuid(issuerGuid)
            {}

            TBlockedGen &operator=(const TBlockedGen &other) = default;

            bool IsBlocked(const TBlockedGen& gen) const {
                return gen.Generation < Generation || (gen.Generation == Generation && (!IssuerGuid || gen.IssuerGuid != IssuerGuid));
            }

            friend bool operator ==(const TBlockedGen& x, const TBlockedGen& y) {
                return x.Generation == y.Generation &&
                    x.IssuerGuid == y.IssuerGuid;
            }
        };

        TBlocksCache() = default;
        ~TBlocksCache() = default;
        TBlocksCache(const TBlocksCache &) = delete;
        TBlocksCache &operator=(const TBlocksCache&) = delete;
        TBlocksCache(TBlocksCache &&) = default;
        TBlocksCache &operator=(TBlocksCache &&) = default;

        TBlockRes IsBlocked(ui64 tabletId, TBlockedGen gen, ui32 *actualGen = nullptr) const;
        bool IsBlockedLegacy(ui64 tabletId, TBlockedGen gen, ui32 *actualGen = nullptr) const;
        bool HasRecord(ui64 tabletId) const;
        bool Find(ui64 tabletId, ui32 *outGen) const;
        bool IsInFlight() const { return !InFlightBlocks.empty() || !InFlightBlocksQueue.empty(); }

        void UpdateLegacy(ui64 tabletId, TBlockedGen gen) { UpdatePersistent(tabletId, gen); }
        // for log replay
        void UpdatePersistent(ui64 tabletId, TBlockedGen gen);
        void UpdateInFlight(ui64 tabletId, TBlockedGen gen, ui64 lsn);
        void CommitInFlight(ui64 tabletId, TBlockedGen gen, ui64 lsn);
        void Build(const THullDs *hullDs);

    private:
        using TTabletId = ui64;

        // a block in flight for a given tablet
        struct TBlockInFlight {
            ui64 Lsn;
            TBlockedGen BlockedGen;
        };

        // a queue of blocks in flight for a given tablet
        using TInFlightQueue = TDeque<TBlockInFlight>;

        // per tablet state of in flight blocks
        struct TInFlightState {
            // max blocked gen among blocks in flight
            TBlockedGen MaxBlockedGen;
            // lsn for this block
            ui64 LsnForMaxBlockedGen;
            // a queue of blocks in flight
            TInFlightQueue InFlightQueue;
        };

        // an element of a queue of blocks in flight for _all_ tablets
        struct TLsnToTabletId {
            ui64 Lsn;
            TTabletId TabletId;
        };

        // blocks that are persistently stored on disk
        using TPersistentBlocks = THashMap<TTabletId, TBlockedGen>;
        // blocks that passed all checks and are being written to recovery log right now
        using TInFlightBlocks = THashMap<TTabletId, TInFlightState>;
        // index for TInFlightBlocks by TLsn
        using TInFlightBlocksQueue = TDeque<TLsnToTabletId>;

        TPersistentBlocks PersistentBlocks;
        TInFlightBlocks InFlightBlocks;
        TInFlightBlocksQueue InFlightBlocksQueue;
        bool Initialized = false;

        TBlockRes IsBlockedByInFlight(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const;
        TBlockRes IsBlockedByPersistent(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const;
    };

} // NKikimr
