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

            if (!ContinueNext) {
                grow &= Index->IsValid() && Index->GetRowId() < EndRowId;
            }

            return {Pages.at(Offset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Y_ABORT_UNLESS(Started, "Couldn't be called before Handle returns grow");

            if (ContinueNext) {
                if (auto ready = Index->Next(); ready == EReady::Page) {
                    return;
                }
                ContinueNext = false;
            }

            // Note: not an effective implementation, each Index->Next() page fault stops Forward
            // and it continues only with a new Handle call that return Grow = true
            // some index forward loading mechanism is needed here

            while (OnHold + OnFetch < upper && Index->IsValid() && Index->GetRowId() < EndRowId) {
                AddToQueue(head, Index->GetPageId());
                if (auto ready = Index->Next(); ready == EReady::Page) {
                    ContinueNext = true;
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

        bool SyncIndex(TPageId pageId) noexcept
        {
            if (!Started) {
                if (auto ready = Index->Seek(BeginRowId); ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready == EReady::Page, "Slices are invalid");
                    return false;
                }
                Y_ABORT_UNLESS(Index->GetPageId() <= pageId, "Requested page is outside of slices");

                // TODO: when pageId is somewhere in the middle of a part, spends lots of time doing Index->Next here
                // should add Index->Seek(TPageId) method or seek TLead.Key instead
                Started = true;
            }

            while (ContinueNext || Index->IsValid() && Index->GetPageId() < pageId) {
                if (auto ready = Index->Next(); ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready == EReady::Page, "Requested page doesn't belong to the part");
                    ContinueNext = true;
                    return false;
                }
                ContinueNext = false;
                Y_DEBUG_ABORT_UNLESS(Index->GetRowId() < EndRowId, "Requested page is outside of slices");
            }

            Y_ABORT_UNLESS(Index->IsValid(), "Requested page doesn't belong to the part");
            Y_ABORT_UNLESS(Index->GetPageId() == pageId, "Requested page doesn't belong to the part or index is out of sync");
            
            if (auto ready = Index->Next(); ready == EReady::Page) {
                ContinueNext = true;
            }
            
            return true;
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

    private:
        THolder<IIndexIter> Index; /* Points on next to load page */
        bool Started = false;
        bool ContinueNext = false;
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
