#pragma once

#include "flat_part_iface.h"
#include "flat_part_forward.h"
#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_page.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    template<size_t Capacity>
    class TLoadedPagesCircularBuffer {
    public:
        const TSharedData* Get(TPageId pageId) const
        {
            if (pageId <= LastLoadedPageId) {
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

            const ui64 was = LoadedPages[Offset].Data.size();

            LoadedPages[Offset].Data = page.Release();
            LoadedPages[Offset].PageId = page.PageId;
            LastLoadedPageId = Max(LastLoadedPageId, page.PageId);

            return was;
        }

    private:
        std::array<NPageCollection::TLoadedPage, Capacity> LoadedPages;
        ui32 Offset = 0;
        TPageId LastLoadedPageId = 0;
    };

    class TCache : public IPageLoadingLogic {
    public:
        using TGroupId = NPage::TGroupId;

        TCache() = delete;

        TCache(const TPart* part, IPages* env, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& bounds = nullptr)
            : Index(part, env, groupId, 1, bounds)
        { }

        ~TCache()
        {
            for (auto &it: Pages) {
                it.Release();
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            Y_ABORT_UNLESS(pageId != Max<TPageId>(), "Invalid requested pageId");

            if (auto *page = Trace.Get(pageId))
                return { page, false, true };

            Rewind(pageId).Shrink(); /* points Offset to pageId */

            bool more = Grow && (OnHold + OnFetch <= lower);

            return { Preload(head, 0).Touch(pageId, Stat), more, true };
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Preload(head, upper);
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
                OnHold += it->Settle(one);

                ++it;
            }

            Shrink();
        }

    private:
        TPage& Preload(IPageLoadingQueue *head, ui64 upper) noexcept
        {
            auto until = [this, upper]() {
                return OnHold + OnFetch < upper ? Max<TPageId>() : 0;
            };

            while (auto more = Index.More(until())) {
                auto size = head->AddToQueue(*more, EPage::DataPage);

                Stat.Fetch += size;
                OnFetch += size;

                Pages.emplace_back(*more, size, 0, Max<TPageId>());
                Pages.back().Fetch = EFetch::Wait;
            }

            Grow = Grow && Index.HasMore();

            return Pages.at(Offset);
        }

        TCache& Rewind(TPageId pageId) noexcept
        {
            while (auto drop = Index.Clean(pageId)) {
                auto &page = Pages.at(Offset);

                if (!Pages || page.PageId != *drop) {
                    Y_ABORT("Dropping page that is not exist in cache");
                } else if (page.Size == 0) {
                    Y_ABORT("Dropping page that has not been touched");
                } else if (page.Usage == EUsage::Keep) {
                    OnHold -= Trace.Emplace(page);
                } else if (auto size = page.Release().size()) {
                    OnHold -= size;

                    *(page.Ready() ? &Stat.After : &Stat.Before) += size;
                }

                Offset++;
            }

            return *this;
        }

        TCache& Shrink() noexcept
        {
            for (; Offset && Pages[0].Ready(); Offset--) {
                Pages.pop_front();
            }

            return *this;
        }

    private:
        bool Grow = true;       /* Have some pages for Forward(...) */
        TForward Index;
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
