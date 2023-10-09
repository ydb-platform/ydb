#pragma once

#include "defs.h"
#include "tablet_flat_executor.h"
#include "flat_executor_misc.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TTableSnapshotContext::TImpl {
    public:
        using TSnapEdge = NTable::TSnapEdge;

        enum class EReady {
            Zero    = 0,
            Wait    = 1,
            Done    = 2,
        };

        struct TState {
            EReady Ready = EReady::Zero;
            TSnapEdge Edge;
        };

        struct TFlush {
            ui32 Step = Max<ui32>();
            TVector<TIntrusivePtr<TBarrier>> Barriers;
            THashMap<TLogoBlobID, TSet<ui64>> Bundles;
            TMap<ui32, ui32> Moved;
        };

        void Prepare(ui32 table, TSnapEdge edge)
        {
            auto &state = Get(table, EReady::Zero);

            state.Ready = EReady::Wait;
            state.Edge = edge;

            Pending++;
        }

        bool Complete(ui32 table, TIntrusivePtr<TBarrier> barrier)
        {
            Get(table, EReady::Wait).Ready = EReady::Done;
            Holds.Barriers.push_back(barrier);

            Y_ABORT_UNLESS(Pending, "Snapshot pending counter is out of sync");

            return --Pending == 0;
        }

        void Borrowed(ui32 step, ui32 table, const TLogoBlobID &bundle, ui64 loaner)
        {
            Get(table, EReady::Done);
            Holds.Step = Min(Holds.Step, step);
            Holds.Bundles[bundle].insert(loaner);
        }

        void Moved(ui32 src, ui32 dst)
        {
            Get(src, EReady::Done);
            Y_ABORT_UNLESS(!Holds.Moved.contains(src), "Table moved multiple times");
            Holds.Moved[src] = dst;
        }

        void Clear()
        {
            Holds.Step = Max<ui32>();
            Holds.Bundles.clear();
            Holds.Moved.clear();
        }

        TSnapEdge Edge(ui32 table)
        {
            return Get(table, EReady::Done).Edge;
        }

        TFlush Release()
        {
            return std::move(Holds);
        }

    private:
        TState& Get(ui32 table, EReady ready)
        {
            auto &state = Tables[table];
            Y_ABORT_UNLESS(state.Ready == ready, "Table snapshot is not in state");
            return state;
        }

    private:
        ui32 Pending = 0;
        THashMap<ui32, TState> Tables;
        TFlush Holds;
    };

}
}
