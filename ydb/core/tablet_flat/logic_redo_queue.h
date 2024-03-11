#pragma once

#include "util_fmt_flat.h"
#include "flat_util_misc.h"
#include "logic_redo_eggs.h"
#include "logic_redo_entry.h"
#include "logic_redo_table.h"
#include "flat_exec_commit.h"
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <ydb/core/util/queue_inplace.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NRedo {

    struct TQueue {
        using TStamp = NTable::TTxStamp;
        using TAffects = TArrayRef<const ui32>;

        TQueue(THashMap<ui32, NTable::TSnapEdge> edges)
            : Log(new TLog)
            , Edges(std::move(edges))
        {

        }

        void Push(TStamp stamp, TAffects affects, const NPageCollection::TLargeGlobId &largeGlobId)
        {
            Push(TEntry::Create(stamp, affects, largeGlobId));
        }

        void Push(TStamp stamp, TAffects affects, TString embedded)
        {
            NSan::CheckMemIsInitialized(embedded.data(), embedded.size());
            Push(TEntry::Create(stamp, affects, std::move(embedded)));
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "LRedo{" << Overhead.size() << "t" << ", " << Items
                << " (" << Memory << " mem" << ", " << LargeGlobIdsBytes << " raw)b }";
        }

        void Push(TEntry *entry) noexcept
        {
            if (bool(entry->Embedded) == bool(entry->LargeGlobId)) {
                Y_Fail(NFmt::Do(*entry) << " has incorrect payload");
            }

            Log->Push(entry);

            Items++;
            Memory += entry->BytesMem();
            LargeGlobIdsBytes += entry->BytesLargeGlobId();

            for (ui32 table : entry->Tables()) {
                const auto *edge = Edges.FindPtr(table);

                if (edge && edge->TxStamp >= entry->Stamp) {
                    Y_Fail(
                        "Entry " << NFmt::Do(*entry) << " queued below table"
                        << table << " edge " << NFmt::TStamp(edge->TxStamp));
                } else if (auto *over = Overhead[table].Push(table, entry)) {
                    Changes.PushBack(over);
                }
            }

            if (entry->References == 0)
                Y_Fail(NFmt::Do(*entry) << " has no effects on data");
        }

        void Cut(ui32 table, NTable::TSnapEdge edge, TGCBlobDelta &gc) noexcept
        {
            Y_ABORT_UNLESS(edge.TxStamp != Max<ui64>(), "Undefined TxStamp of edge");

            auto &cur = Edges[table];

            cur.TxStamp = Max(cur.TxStamp, edge.TxStamp);
            cur.Head = Max(cur.Head, edge.Head);

            if (auto *over = Overhead[table].Cut(cur.TxStamp, gc, LargeGlobIdsBytes))
                Changes.PushBack(over);
        }

        void Flush(NKikimrExecutorFlat::TLogSnapshot &snap)
        {
            for (auto &it : Overhead)
                it.second.Clear();

            auto was = std::exchange(Log, new TLog);

            Items = 0;
            Memory = 0;
            LargeGlobIdsBytes = 0;

            auto logos = snap.MutableNonSnapLogBodies();

            while (TAutoPtr<TEntry> entry = was->Pop()) {
                if (entry->FilterTables(Edges)) {
                    for (const auto& blobId : entry->LargeGlobId.Blobs()) {
                        LogoBlobIDFromLogoBlobID(blobId, logos->Add());
                    }

                    if (entry->Embedded) {
                        auto *body = snap.AddEmbeddedLogBodies();

                        body->SetGeneration(entry->Stamp.Gen());
                        body->SetStep(entry->Stamp.Step());
                        body->SetBody(entry->Embedded);
                    }

                    entry->References = 0;

                    Push(entry.Release());
                } else {
                    Y_ABORT_UNLESS(entry->References == 0);
                }
            }
        }

        TArrayRef<const TUsage> GrabUsage() noexcept
        {
            Usage.clear();

            while (auto *one = Changes ? Changes.PopFront() : nullptr) {
                Usage.push_back({ one->Table, one->Trace.size(), one->Bytes });
            }

            return Usage;
        }

        using TLog = TOneOneQueueInplace<NRedo::TEntry *, 4096>;

        TAutoPtr<TLog, TLog::TPtrCleanInplaceMallocDestructor> Log;
        THashMap<ui32, NTable::TSnapEdge> Edges;
        THashMap<ui32, TOverhead> Overhead;
        TIntrusiveList<TOverhead> Changes;
        TVector<TUsage> Usage;

        ui64 Items = 0;
        ui64 Memory = 0;    /* Bytes consumed memory by records */
        ui64 LargeGlobIdsBytes = 0;    /* Bytes acquired in redo TLargeGlobId-s  */
    };

}
}
}
