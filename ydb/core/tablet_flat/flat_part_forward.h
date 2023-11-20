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
        struct TResult {
            TResult(TPageId pageId) noexcept : PageId(pageId) { }

            explicit operator bool() const noexcept
            {
                return PageId != Max<TPageId>();
            }

            operator TPageId() const noexcept
            {
                return PageId;
            }

            TPageId PageId = Max<TPageId>();
        };

        explicit TForward(const TPart* part, IPages* env, TGroupId groupId, ui32 trace, const TIntrusiveConstPtr<TSlices>& bounds = nullptr)
            : Trace(Max(ui32(1), trace))
            , Index(part, env, groupId)
        {
            if (bounds && !bounds->empty()) {
                BeginBoundRowId = bounds->front().BeginRowId();
                EndBoundRowId = bounds->back().EndRowId();
            } else {
                BeginBoundRowId = 0;
                EndBoundRowId = Max<TRowId>();
            }
        }

        inline TPageId On(bool head = false) noexcept
        {
            EnsureStarted();
            return PageOf(head ? Head : Edge);
        }

        TResult Clean(TPageId until) noexcept
        {
            EnsureStarted();

            if (PageOf(Tail) > until) {
                Y_ABORT("Part lookups goes below of its trace pages");
            } else {
                const auto edgeId = Max(PageOf(Edge), until);

                while (Edge != End && Edge->GetPageId() < until) ++Edge;

                if (PageOf(Edge) != edgeId)
                    Y_ABORT("Part lookup page is out of its index");

                if (Tail == Head) Tail = Head = Edge;
            }

            if (Tail != Head && Edge - Tail >= ssize_t(Trace)) {
                return (Tail++)->GetPageId();
            }

            return Max<TPageId>();
        }

        TResult More(TPageId until) noexcept
        {
            EnsureStarted();

            if (Head != End && ((Edge - Head >= 0) || PageOf(Head) < until)) {
                return (Head++)->GetPageId();
            }

            return Max<TPageId>();
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

            if (BeginBoundRowId > 0) {
                // Find the first page we would allow to load
                Tail = Head = Edge = index->LookupRow(BeginBoundRowId);
            }
            if (EndBoundRowId < index->GetEndRowId()) {
                // Find the first page we would refuse to load
                End = index->LookupRow(EndBoundRowId);
                if (End && End->GetRowId() < EndBoundRowId) {
                    ++End;
                }
            }

            Started = true;
        }

        inline TPageId PageOf(const TIter &it) const
        {
            return it == End ? Max<TPageId>() : it->GetPageId();
        }

    public:
        const ui32  Trace = 0;

    private:
        TPartIndexIt Index;
        bool Started = false;
        TIter Tail, Head, Edge, End;
        TRowId BeginBoundRowId, EndBoundRowId;
    };
}
}
