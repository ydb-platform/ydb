#pragma once

#include "flat_part_index_iter.h"
#include "flat_table_part.h"
#include "flat_page_index.h"
#include "flat_part_slice.h"

namespace NKikimr {
namespace NTable {

    class TForward {
        using TIndex = NPage::TIndex;
        using TGroupId = NPage::TGroupId;
        using TPageId = NPage::TPageId;
        using TIter = TIndex::TIter;

    public:
        TForward(const TPart* part, IPages* env, TGroupId groupId, ui32 trace, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : Trace(Max(ui32(1), trace))
            , Index(part, env, groupId)
        {
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }
        }

        bool HasMore() noexcept
        {
            return Head != End;
        }

        std::optional<TPageId> Clean(TPageId until) noexcept
        {
            EnsureStarted();

            Y_ABORT_UNLESS(GetPageOf(Tail) <= until, "Part lookups goes below of its trace pages");

            const auto edgeId = Max(GetPageOf(Edge), until);

            while (Edge != End && Edge->GetPageId() < until) {
              ++Edge;
            }

            Y_ABORT_UNLESS(GetPageOf(Edge) == edgeId, "Part lookup page is out of its index");

            if (Tail == Head) {
                Tail = Head = Edge;
            } else if (Edge - Tail >= ssize_t(Trace)) {
                return (Tail++)->GetPageId();
            }

            return { };
        }

        std::optional<TPageId> More(TPageId until) noexcept
        {
            EnsureStarted();

            if (Head != End && (Head <= Edge || GetPageOf(Head) < until)) {
                return (Head++)->GetPageId();
            }

            return { };
        }

    private:
        void EnsureStarted()
        {
            if (Started) {
                return;
            }

            auto index = Index.TryLoadRaw();

            // temporary solution: its too hard to handle index page faults in Clean/More methods
            Y_ABORT_UNLESS(index, "Index should have been loaded before using TForward");

            Tail = (*index)->Begin();
            Head = (*index)->Begin();
            Edge = (*index)->Begin();
            End = (*index)->End();

            if (BeginRowId > 0) {
                // Find the first page we would allow to load
                Tail = Head = Edge = index->LookupRow(BeginRowId);
            }
            if (EndRowId < index->GetEndRowId()) {
                // Find the first page we would refuse to load
                End = index->LookupRow(EndRowId);
                if (End && End->GetRowId() < EndRowId) {
                    ++End;
                }
            }

            Started = true;
        }

        inline TPageId GetPageOf(const TIter &it) const
        {
            return it == End ? Max<TPageId>() : it->GetPageId();
        }

    public:
        const ui32  Trace = 0;

    private:
        TPartIndexIt Index;
        bool Started = false;
        TIter Tail, Head, Edge, End;
        TRowId BeginRowId, EndRowId;
    };
}
}
