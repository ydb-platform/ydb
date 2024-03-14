#pragma once

#include "flat_part_iface.h"
#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_page.h"
#include "flat_part_index_iter.h"
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

        TCache(const TPart* part, IPages* env, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& bounds = nullptr)
            : Index(MakeHolder<TPartIndexIt>(part, env, groupId)) // TODO: use CreateIndexIter(part, env, groupId)
        { 
            if (bounds && !bounds->empty()) {
                BeginRowId = bounds->front().BeginRowId();
                EndRowId = bounds->back().EndRowId();
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
                SyncIndex(pageId);
                AddToQueue(head, pageId);
            }

            grow &= Index->IsValid() && Index->GetRowId() < EndRowId;

            return {Pages.at(Offset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Y_ABORT_UNLESS(Started, "Couldn't be called before Handle returns grow");

            while (OnHold + OnFetch < upper && Index->IsValid() && Index->GetRowId() < EndRowId) {
                AddToQueue(head, Index->GetPageId());
                Y_ABORT_UNLESS(Index->Next() != EReady::Page);
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

        void SyncIndex(TPageId pageId) noexcept
        {
            if (!Started) {
                Y_ABORT_UNLESS(Index->Seek(BeginRowId) == EReady::Data);
                Y_ABORT_UNLESS(Index->GetPageId() <= pageId, "Requested page is out of slice bounds");
                Started = true;
            }

            while (Index->IsValid() && Index->GetPageId() < pageId) {
                Y_ABORT_UNLESS(Index->Next() == EReady::Data);
                Y_ABORT_UNLESS(Index->GetRowId() < EndRowId, "Requested page is out of slice bounds");
            }

            Y_ABORT_UNLESS(Index->GetPageId() == pageId, "Requested page doesn't belong to the part");
            Y_ABORT_UNLESS(Index->Next() != EReady::Page);
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
