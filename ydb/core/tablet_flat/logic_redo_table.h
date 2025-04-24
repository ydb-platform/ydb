#pragma once

#include "logic_redo_entry.h"
#include "flat_exec_commit.h"
#include "util_fmt_abort.h"
#include <util/generic/deque.h>
#include <util/generic/intrlist.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NRedo {

    struct TOverhead : public TIntrusiveListItem<TOverhead> {
        using TTxStamp = NTable::TTxStamp;

        void Clear() noexcept
        {
            Trace.clear();
            Bytes = 0;
        }

        TOverhead* Push(ui32 table, TEntry* entry)
        {
            if (Trace && Trace.back() == entry) {
                Y_TABLET_ERROR(NFmt::Do(*entry) << " is dublicated in table trace");
            } else if (Trace && entry->Stamp < Trace.back()->Stamp) {
                Y_TABLET_ERROR(NFmt::Do(*entry) << " is in the past of table trace");
            }

            Trace.emplace_back(entry);
            Bytes += entry->BytesData();
            Table = table;
            entry->References += 1;

            return this;
        }

        TOverhead* Cut(TTxStamp stamp, TGCBlobDelta &gc, ui64 &largeGlobIds)
        {
            if (!Trace || stamp < Trace.front()->Stamp) {
                return nullptr;
            }

            const auto end = std::upper_bound(Trace.begin(), Trace.end(), stamp,
                [](TTxStamp stamp, const TEntry *entry) {
                    return stamp < entry->Stamp;
                });

            Y_ENSURE(end != Trace.begin());

            for (auto it = Trace.begin(); it != end; ++it) {
                NUtil::SubSafe(Bytes, (*it)->BytesData());

                if (NUtil::SubSafe((*it)->References, ui32(1)) == 0) {
                    (*it)->LargeGlobId.MaterializeTo(gc.Deleted);
                    NUtil::SubSafe(largeGlobIds, (*it)->BytesLargeGlobId());
                }
            }
            Trace.erase(Trace.begin(), end);
            return this;
        }

        ui32 Table = Max<ui32>();
        TDeque<TEntry*> Trace;
        ui64 Bytes = 0;     /* Bytes of redo logs touched by this table */
    };

}
}
}
