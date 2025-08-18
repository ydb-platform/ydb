#pragma once

#include "flat_util_misc.h"
#include "logic_redo_eggs.h"
#include "logic_redo_entry.h"
#include "logic_redo_table.h"
#include "flat_exec_commit.h"
#include "util_fmt_abort.h"
#include "util_fmt_flat.h"
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
        using TLog = TQueueInplace<std::unique_ptr<TEntry>, 4096>;

        TQueue(THashMap<ui32, NTable::TSnapEdge> edges)
            : Edges(std::move(edges))
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

        void Describe(IOutputStream &out) const
        {
            out
                << "LRedo{" << Overhead.size() << "t" << ", " << Items
                << " (" << Memory << " mem" << ", " << LargeGlobIdsBytes << " raw)b }";
        }

        void Push(std::unique_ptr<TEntry>&& entryPtr)
        {
            TEntry* entry = entryPtr.get();
            if (bool(entry->Embedded) == bool(entry->LargeGlobId)) {
                Y_TABLET_ERROR(NFmt::Do(*entry) << " has incorrect payload");
            }

            Log.Push(std::move(entryPtr));

            Items++;
            Memory += entry->BytesMem();
            LargeGlobIdsBytes += entry->BytesLargeGlobId();

            for (ui32 table : entry->Tables()) {
                const auto *edge = Edges.FindPtr(table);

                if (edge && edge->TxStamp >= entry->Stamp) {
                    Y_TABLET_ERROR(
                        "Entry " << NFmt::Do(*entry) << " queued below table"
                        << table << " edge " << NFmt::TStamp(edge->TxStamp));
                } else if (auto *over = Overhead[table].Push(table, entry)) {
                    Changes.PushBack(over);
                }
            }

            if (entry->References == 0) {
                Y_TABLET_ERROR(NFmt::Do(*entry) << " has no effects on data");
            }
        }

        void Cut(ui32 table, NTable::TSnapEdge edge, TGCBlobDelta &gc)
        {
            Y_ENSURE(edge.TxStamp != Max<ui64>(), "Undefined TxStamp of edge");

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

            TLog was = std::exchange(Log, TLog{});

            Items = 0;
            Memory = 0;
            LargeGlobIdsBytes = 0;

            auto logos = snap.MutableNonSnapLogBodies();

            while (auto entry = was.PopDefault()) {
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

                    Push(std::move(entry));
                } else {
                    Y_ENSURE(entry->References == 0);
                }
            }
        }

        TArrayRef<const TUsage> GrabUsage()
        {
            Usage.clear();

            while (auto *one = Changes ? Changes.PopFront() : nullptr) {
                Usage.push_back({ one->Table, one->Trace.size(), one->Bytes });
            }

            return Usage;
        }

        TLog Log;
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
