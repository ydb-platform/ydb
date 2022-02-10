#pragma once

#include "flat_table_part.h"
#include "flat_page_index.h"
#include "flat_part_slice.h"

namespace NKikimr {
namespace NTable {

    class TForward {
        using TIndex = NPage::TIndex;
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

        explicit TForward(const TIndex& index, ui32 trace, const TIntrusiveConstPtr<TSlices>& bounds = nullptr)
            : Trace(Max(ui32(1), trace))
            , Tail(index->Begin())
            , Head(index->Begin())
            , Edge(index->Begin())
            , End(index->End())
        {
            if (bounds && !bounds->empty()) {
                if (auto rowId = bounds->front().BeginRowId(); rowId > 0) {
                    // Find the first page we would allow to load
                    Tail = Head = Edge = index.LookupRow(rowId);
                }
                if (auto rowId = bounds->back().EndRowId(); rowId < index.GetEndRowId()) {
                    // Find the first page we would refuse to load
                    End = index.LookupRow(rowId);
                    if (End && End->GetRowId() < rowId) {
                        ++End;
                    }
                }
            }
        }

        inline TPageId On(bool head = false) const noexcept
        {
            return PageOf(head ? Head : Edge);
        }

        inline size_t Span() const noexcept
        {
            return Head - Tail;
        }

        TResult Clean(TPageId until) noexcept
        {
            if (PageOf(Tail) > until) {
                Y_FAIL("Part lookups goes below of its trace pages");
            } else {
                const auto edgeId = Max(PageOf(Edge), until);

                while (Edge != End && Edge->GetPageId() < until) ++Edge;

                if (PageOf(Edge) != edgeId)
                    Y_FAIL("Part lookup page is out of its index");

                if (Tail == Head) Tail = Head = Edge;
            }

            if (Tail != Head && Edge - Tail >= ssize_t(Trace)) {
                return (Tail++)->GetPageId();
            }

            return Max<TPageId>();
        }

        TResult More(TPageId until) noexcept
        {
            if (Head != End && ((Edge - Head >= 0) || PageOf(Head) < until)) {
                return (Head++)->GetPageId();
            }

            return Max<TPageId>();
        }

    private:
        inline TPageId PageOf(const TIter &it) const
        {
            return it == End ? Max<TPageId>() : it->GetPageId();
        }

    public:
        const ui32  Trace = 0;

    private:
        TIter Tail, Head, Edge, End;
    };
}
}
