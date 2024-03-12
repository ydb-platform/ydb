#pragma once

#include "flat_part_iface.h"
#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_page.h"
#include "flat_part_index_iter_iface.h"
#include "flat_table_part.h"
#include "flat_part_slice.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    template<size_t Capacity>
    class TLoadedPagesCircularBuffer {
    public:
        const TSharedData* Get(TPageId pageId) const
        {
            if (pageId < FirstUnseenPageId) {
                for (const auto& page : LoadedPages) {
                    if (page.PageId == pageId) {
                        return &page.Data;
                    }
                }

                Y_ABORT("Failed to locate page within forward trace");
            }

            // next pages may be requested, ignore them
            return nullptr;
        }

        // returns released data size
        ui64 Emplace(TPage &page)
        {
            Y_ABORT_UNLESS(page, "Cannot push invalid page to trace cache");

            Offset = (Offset + 1) % Capacity;

            const ui64 releasedDataSize = LoadedPages[Offset].Data.size();

            LoadedPages[Offset].Data = page.Release();
            LoadedPages[Offset].PageId = page.PageId;
            FirstUnseenPageId = Max(FirstUnseenPageId, page.PageId + 1);

            return releasedDataSize;
        }

    private:
        std::array<NPageCollection::TLoadedPage, Capacity> LoadedPages;
        ui32 Offset = 0;
        TPageId FirstUnseenPageId = 0;
    };

    class TCache : public IPageLoadingLogic {
        enum EIndexState {
            DoStart,
            Valid,
            DoNext,
            DoBinarySearch,
            Exhausted
        };
        struct TBinarySearchState {
            TPageId PageId;
            TRowId BeginRowId, EndRowId;
        };

    public:
        using TGroupId = NPage::TGroupId;

        TCache() = delete;

        TCache(const TPart* part, IPages* env, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : Index(CreateIndexIter(part, env, groupId))
        { 
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Index->GetEndRowId();
            }
        }

        ~TCache()
        {
            for (auto &it: Pages) {
                it.Release();
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            Y_ABORT_UNLESS(pageId != Max<TPageId>(), "Requested page is invalid");

            if (auto *page = Trace.Get(pageId)) {
                return { page, false, true };
            }

            DropPagesBefore(pageId);
            Shrink();

            bool grow = OnHold + OnFetch <= lower;

            if (Offset == Pages.size()) { // isn't processed yet
                if (!SyncIndex(pageId)) {
                    return {nullptr, false, true};
                }
                AddToQueue(head, pageId);
            }

            grow &= IndexState != Exhausted;

            return {Pages.at(Offset).Touch(pageId, Stat), grow, true};
        }

        // IndexState: {DoNext, Valid} -> {DoNext, Valid, Exhausted}
        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            if (IndexState == DoNext) {
                if (!IndexDoNext()) {
                    return;
                }
            }

            Y_DEBUG_ABORT_UNLESS(IndexState == Valid, "Index state is invalid");

            // Note: not an effective implementation, each Index->Next() page fault stops Forward
            // and it continues only with a new Handle call that return Grow = true
            // some index forward loading mechanism is needed here

            while (IndexState == Valid && OnHold + OnFetch < upper) {
                AddToQueue(head, Index->GetPageId());
                if (IndexState = DoNext; !IndexDoNext()) {
                    return;
                }
            }
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            auto it = Pages.begin();

            for (auto &one: loaded) {
                if (it == Pages.end() || it->PageId > one.PageId) {
                    it = std::lower_bound(Pages.begin(), it, one.PageId);
                } else if (it->PageId < one.PageId) {
                    it = std::lower_bound(++it, Pages.end(), one.PageId);
                }

                if (it == Pages.end() || it->PageId != one.PageId) {
                    Y_ABORT("Got page that hasn't been requested for load");
                } if (one.Data.size() > OnFetch) {
                    Y_ABORT("Forward cache ahead counters is out of sync");
                }

                Stat.Saved += one.Data.size();
                OnFetch -= one.Data.size();
                OnHold += it->Settle(one); // settle of a dropped page returns 0 and releases its data

                ++it;
            }

            Shrink();
        }

    private:
        void DropPagesBefore(TPageId pageId) noexcept
        {
            while (Offset < Pages.size()) {
                auto &page = Pages.at(Offset);

                if (page.PageId >= pageId) {
                    break;
                }

                if (page.Size == 0) {
                    Y_ABORT("Dropping page that has not been touched");
                } else if (page.Usage == EUsage::Keep && page) {
                    OnHold -= Trace.Emplace(page);
                } else {
                    OnHold -= page.Release().size();
                    *(page.Ready() ? &Stat.After : &Stat.Before) += page.Size;
                }

                // keep pending pages but increment offset
                Offset++;
            }
        }

        void Shrink() noexcept
        {
            for (; Offset && Pages[0].Ready(); Offset--) {
                Pages.pop_front();
            }
        }

        void AddToQueue(IPageLoadingQueue *head, TPageId pageId) noexcept
        {
            auto size = head->AddToQueue(pageId, EPage::DataPage);

            Stat.Fetch += size;
            OnFetch += size;

            Y_ABORT_UNLESS(!Pages || Pages.back().PageId < pageId);
            Pages.emplace_back(pageId, size, 0, Max<TPageId>());
            Pages.back().Fetch = EFetch::Wait;
        }

        // IndexState: {DoStart, Valid, DoNext, DoBinarySearch} -> {DoStart, DoNext, DoBinarySearch} (returns false) | {Valid, DoNext} (returns true)
        bool SyncIndex(TPageId pageId) noexcept
        {
            if (IndexState == DoStart) {
                if (!IndexDoStart()) {
                    return false;
                }
            }

            if (IndexState == Valid && Index->GetPageId() < pageId) {
                IndexState = DoNext;
            }

            if (IndexState == DoNext) {
                if (!IndexDoNext()) {
                    return false;
                }

                Y_ABORT_UNLESS(IndexState == Valid, "Requested page is outside of slices");
                Y_ABORT_UNLESS(Index->GetPageId() <= pageId, "Index is out of sync");

                if (Index->GetPageId() < pageId) {
                    IndexState = DoBinarySearch;
                    BinarySearchState = {pageId, Index->GetNextRowId(), EndRowId};
                }
            }

            if (IndexState == DoBinarySearch) {
                if (BinarySearchState.PageId != pageId) {
                    Y_ABORT_UNLESS(BinarySearchState.PageId < pageId);
                    BinarySearchState = {pageId, BinarySearchState.BeginRowId, EndRowId};
                }

                if (!IndexDoBinarySearch()) {
                    return false;
                }
            }

            Y_ABORT_UNLESS(IndexState == Valid);
            Y_ABORT_UNLESS(Index->GetPageId() == pageId, "Index is out of sync");
            
            IndexState = DoNext; // point to the next page
            IndexDoNext(); // ignore result
            
            return true;
        }

        // IndexState: DoStart -> DoStart (returns false) | Valid (returns true)
        bool IndexDoStart() noexcept
        {
            Y_ABORT_UNLESS(IndexState == DoStart);

            if (EndRowId == Max<TRowId>()) { // may happen with flat group indexes
                if (auto ready = Index->SeekLast(); ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready == EReady::Page, "Slices are invalid");
                    return false;
                }
                EndRowId = Index->GetRowId() + 1; // not real but appropriate for a binary search 
            }

            if (auto ready = Index->Seek(BeginRowId); ready != EReady::Data) {
                Y_ABORT_UNLESS(ready == EReady::Page, "Slices are invalid");
                return false;
            }

            IndexState = Valid;
            return true;
        }

        // IndexState: DoNext -> DoNext (returns false) | Valid (returns true) | Exhausted (returns true)
        bool IndexDoNext() noexcept
        {
            Y_ABORT_UNLESS(IndexState == DoNext);

            auto ready = Index->Next();

            if (ready == EReady::Page) {
                return false;
            }

            if (ready == EReady::Data && Index->GetRowId() < EndRowId) {
                IndexState = Valid;
                return true;
            }

            IndexState = Exhausted;
            return true;
        }

        // IndexState: DoBinarySearch -> DoBinarySearch (returns false) | Valid (returns true)
        bool IndexDoBinarySearch() noexcept
        {
            Y_ABORT_UNLESS(IndexState == DoBinarySearch);

            while (BinarySearchState.BeginRowId < BinarySearchState.EndRowId) {
                auto middleRowId = (BinarySearchState.BeginRowId + BinarySearchState.EndRowId) / 2;
                if (auto ready = Index->Seek(middleRowId); ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready == EReady::Page, "Slices are invalid");
                    return false;
                }
                if (Index->GetPageId() == BinarySearchState.PageId) {
                    IndexState = Valid;
                    return true;
                } else if (Index->GetPageId() > BinarySearchState.PageId) {
                    BinarySearchState.EndRowId = middleRowId;
                } else { // Index->GetPageId() < BinarySearchState.PageId
                    BinarySearchState.BeginRowId = middleRowId + 1;
                }
            }

            Y_ABORT("Index is out of sync");
        }

    private:
        THolder<IIndexIter> Index; /* Points on next to load page */
        EIndexState IndexState = DoStart;
        TBinarySearchState BinarySearchState;
        TRowId BeginRowId, EndRowId;
        TLoadedPagesCircularBuffer<TPart::Trace> Trace;

        /*_ Forward cache line state */
        ui64 OnHold = 0;
        ui64 OnFetch = 0;
        ui32 Offset = 0;
        TDeque<TPage> Pages;
    };
}
}
}
