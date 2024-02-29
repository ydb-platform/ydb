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

        TCache(const TPart* part, IPages* env, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& bounds = nullptr)
            : Index(CreateIndexIter(part, env, groupId))
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
            Y_ABORT_UNLESS(pageId != Max<TPageId>(), "Invalid requested pageId");

            if (auto *page = Trace.Get(pageId)) {
                return { page, false, true };
            }

            DropPagesBefore(pageId);
            Shrink();

            bool more = Grow && (OnHold + OnFetch <= lower);

            if (!Started) {
                Y_ABORT_UNLESS(Index->Seek(BeginRowId) == EReady::Data);
                Y_ABORT_UNLESS(Index->GetPageId() <= pageId);
                Started = true;
            }

            while (Index->IsValid() && Index->GetPageId() < pageId) {
                Y_ABORT_UNLESS(Index->Next() == EReady::Data);
                Y_ABORT_UNLESS(Index->GetRowId() < EndRowId);
            }

            if (Offset == Pages.size()) {
                Y_ABORT_UNLESS(Index->GetPageId() == pageId);
                Request(head, pageId);
                Y_ABORT_UNLESS(Index->Next() == EReady::Data);
            }

            return {Pages.at(Offset).Touch(pageId, Stat), more, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            while (OnHold + OnFetch < upper && Index->IsValid() && Index->GetRowId() < EndRowId) {
                Request(head, Index->GetPageId());
                Y_ABORT_UNLESS(Index->Next() != EReady::Page);
            }

            Grow &= Index->IsValid() && Index->GetRowId() < EndRowId;
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

        void Request(IPageLoadingQueue *head, TPageId pageId) {
            auto size = head->AddToQueue(pageId, EPage::DataPage);

            Stat.Fetch += size;
            OnFetch += size;

            Y_ABORT_UNLESS(!Pages || Pages.back().PageId < pageId);
            Pages.emplace_back(pageId, size, 0, Max<TPageId>());
            Pages.back().Fetch = EFetch::Wait;
        }

    private:
        bool Grow = true;       /* Have some pages for Forward(...) */
        THolder<IIndexIter> Index;
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
