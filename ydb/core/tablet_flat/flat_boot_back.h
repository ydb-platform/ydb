#pragma once

#include "defs.h"
#include "flat_dbase_naked.h"
#include "flat_dbase_scheme.h"
#include "flat_boot_switch.h"
#include "flat_boot_oven.h"
#include "logic_snap_waste.h"
#include "logic_redo_queue.h"
#include <ydb/core/base/tablet.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    using TDependency = NKikimr::TEvTablet::TDependencyGraph;

    struct TLogEntry {
        TLogEntry() = default;

        TLogEntry(NTable::TTxStamp stamp, TString body)
            : Stamp(stamp)
            , Body(std::move(body))
        {

        }

        TLogEntry(NTable::TTxStamp stamp, NPageCollection::TLargeGlobId largeGlobId)
            : Stamp(stamp)
            , LargeGlobId(largeGlobId)
        {

        }

        explicit operator bool() const noexcept
        {
            return bool(Body);
        }

        const NTable::TTxStamp Stamp;
        const NPageCollection::TLargeGlobId LargeGlobId;
        TString Body;
    };

    struct TBody {
        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Body{ " << NFmt::Do(LargeGlobId)
                << " has " << Body.size() << "b }";
        }

        explicit operator bool() const noexcept
        {
            return bool(Body);
        }

        const NPageCollection::TLargeGlobId LargeGlobId;
        TString Body;
    };

    struct TBack {
        TBack(bool follower, ui64 tablet, ui32 generation)
            : Follower(follower)
            , Tablet(tablet)
            , Generation(generation)
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << (Follower ? "Follower" : "Leader")
                << "{" << Tablet << ":" << Generation << ":-}";
        }

        void SetTableEdge(const NKikimrExecutorFlat::TLogMemSnap &edge)
        {
            const ui64 stamp = TTxStamp(edge.GetGeneration(), edge.GetStep());
            const NTable::TEpoch epoch(edge.HasHead() ? edge.GetHead() : 0);

            Y_ABORT_UNLESS(stamp != Max<ui64>(), "Undefined TxStamp of snapshot");

            auto &last = Edges[edge.GetTable()];

            last.TxStamp = Max(last.TxStamp, stamp);
            last.Head = Max(last.Head, epoch);
        }

        const bool Follower = false;
        const ui64 Tablet = Max<ui64>();
        const ui32 Generation = Max<ui32>();

        ui64 Serial = 0;

        TAutoPtr<NBoot::TSteppedCookieAllocatorFactory> SteppedCookieAllocatorFactory;
        TIntrusivePtr<NSnap::TWaste> Waste;
        TAutoPtr<NTable::TScheme> Scheme;
        TAutoPtr<NTable::TDatabaseImpl> DatabaseImpl;
        TAutoPtr<NRedo::TQueue> Redo;
        NPageCollection::TLargeGlobId Snap;
        TDeque<TLogEntry> RedoLog;
        TDeque<TBody> GCELog;
        TDeque<TBody> LoansLog;
        TDeque<TBody> AlterLog;
        TDeque<TBody> TurnsLog;
        TDeque<TSwitch> Switches;
        THashMap<ui32, NTable::TSnapEdge> Edges;
        THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> PageCaches;
        THashMap<TLogoBlobID, TSharedData> TxStatusCaches;
    };

}
}
}
