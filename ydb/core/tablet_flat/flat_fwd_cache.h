#pragma once

#include "flat_part_iface.h"
#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_page.h"
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

    class TFlatIndexCache : public IPageLoadingLogic {
    public:
        using TGroupId = NPage::TGroupId;

        TFlatIndexCache(const TPart* part, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : IndexPage(part->IndexPages.GetFlat(groupId), part->GetPageSize(part->IndexPages.GetFlat(groupId)), 0, Max<TPageId>())
        { 
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }
        }

        ~TFlatIndexCache()
        {
            IndexPage.Release();
            for (auto &it: Pages) {
                it.Release();
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            if (pageId == IndexPage.PageId) {
                if (IndexPage.Fetch == EFetch::None) {
                    Stat.Fetch += head->AddToQueue(pageId, EPage::Index);
                    IndexPage.Fetch = EFetch::Wait;
                }
                return {IndexPage.Touch(pageId, Stat), false, true};
            }

            if (auto *page = Trace.Get(pageId)) {
                return {page, false, true};
            }

            DropPagesBefore(pageId);
            Shrink();

            bool grow = OnHold + OnFetch <= lower;

            if (PagesOffset == Pages.size()) { // isn't processed yet
                Advance(pageId);
                Request(head, pageId);
            }

            grow &= Iter && Iter->GetRowId() < EndRowId;

            return {Pages.at(PagesOffset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Y_DEBUG_ABORT_UNLESS(Iter && Iter->GetRowId() < EndRowId);

            while (Iter && Iter->GetRowId() < EndRowId && OnHold + OnFetch < upper) {
                Request(head, Iter->GetPageId());
                Iter++;
            }
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            auto it = Pages.begin();

            for (auto &one: loaded) {
                if (one.PageId == IndexPage.PageId) {
                    Index.emplace(one.Data);
                    Iter = Index->LookupRow(BeginRowId);
                    Stat.Saved += IndexPage.Settle(one);
                    continue;
                }

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
            while (PagesOffset < Pages.size()) {
                auto &page = Pages.at(PagesOffset);

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
                PagesOffset++;
            }
        }

        void Shrink() noexcept
        {
            for (; PagesOffset && Pages[0].Ready(); PagesOffset--) {
                Pages.pop_front();
            }
        }

        void Advance(TPageId pageId) noexcept
        {
            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() <= pageId);
            while (Iter && Iter->GetPageId() < pageId) {
                Iter++;
            }

            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() == pageId);
            Iter++;
        }

        void Request(IPageLoadingQueue *head, TPageId pageId) noexcept
        {
            auto size = head->AddToQueue(pageId, EPage::DataPage);

            Stat.Fetch += size;
            OnFetch += size;

            Y_ABORT_UNLESS(!Pages || Pages.back().PageId < pageId);
            Pages.emplace_back(pageId, size, 0, Max<TPageId>());
            Pages.back().Fetch = EFetch::Wait;
        }

    private:
        TRowId BeginRowId, EndRowId;
        
        TPage IndexPage;
        std::optional<NPage::TIndex> Index;
        NPage::TIndex::TIter Iter;

        TLoadedPagesCircularBuffer<TPart::Trace> Trace;

        /* Forward cache line state */
        ui64 OnHold = 0, OnFetch = 0;
        TDeque<TPage> Pages;
        ui32 PagesOffset = 0;
    };

    class TBTreeIndexCache : public IPageLoadingLogic {
    public:
        using TGroupId = NPage::TGroupId;

        TBTreeIndexCache(const TPart* part, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : IndexPage(part->IndexPages.GetFlat(groupId), part->GetPageSize(part->IndexPages.GetFlat(groupId)), 0, Max<TPageId>())
        { 
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }
        }

        ~TBTreeIndexCache()
        {
            IndexPage.Release();
            for (auto &it: Pages) {
                it.Release();
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            if (pageId == IndexPage.PageId) {
                if (IndexPage.Fetch == EFetch::None) {
                    Stat.Fetch += head->AddToQueue(pageId, EPage::Index);
                    IndexPage.Fetch = EFetch::Wait;
                }
                return {IndexPage.Touch(pageId, Stat), false, true};
            }

            if (auto *page = Trace.Get(pageId)) {
                return {page, false, true};
            }

            DropPagesBefore(pageId);
            Shrink();

            bool grow = OnHold + OnFetch <= lower;

            if (PagesOffset == Pages.size()) { // isn't processed yet
                Advance(pageId);
                Request(head, pageId);
            }

            grow &= Iter && Iter->GetRowId() < EndRowId;

            return {Pages.at(PagesOffset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Y_DEBUG_ABORT_UNLESS(Iter && Iter->GetRowId() < EndRowId);

            while (Iter && Iter->GetRowId() < EndRowId && OnHold + OnFetch < upper) {
                Request(head, Iter->GetPageId());
                Iter++;
            }
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            auto it = Pages.begin();

            for (auto &one: loaded) {
                if (one.PageId == IndexPage.PageId) {
                    Index.emplace(one.Data);
                    Iter = Index->LookupRow(BeginRowId);
                    Stat.Saved += IndexPage.Settle(one);
                    continue;
                }

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
            while (PagesOffset < Pages.size()) {
                auto &page = Pages.at(PagesOffset);

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
                PagesOffset++;
            }
        }

        void Shrink() noexcept
        {
            for (; PagesOffset && Pages[0].Ready(); PagesOffset--) {
                Pages.pop_front();
            }
        }

        void Advance(TPageId pageId) noexcept
        {
            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() <= pageId);
            while (Iter && Iter->GetPageId() < pageId) {
                Iter++;
            }

            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() == pageId);
            Iter++;
        }

        void Request(IPageLoadingQueue *head, TPageId pageId) noexcept
        {
            auto size = head->AddToQueue(pageId, EPage::DataPage);

            Stat.Fetch += size;
            OnFetch += size;

            Y_ABORT_UNLESS(!Pages || Pages.back().PageId < pageId);
            Pages.emplace_back(pageId, size, 0, Max<TPageId>());
            Pages.back().Fetch = EFetch::Wait;
        }

    private:
        TRowId BeginRowId, EndRowId;
        
        TPage IndexPage;
        std::optional<NPage::TIndex> Index;
        NPage::TIndex::TIter Iter;

        TLoadedPagesCircularBuffer<TPart::Trace> Trace;

        /* Forward cache line state */
        ui64 OnHold = 0, OnFetch = 0;
        TDeque<TPage> Pages;
        ui32 PagesOffset = 0;
    };

    inline THolder<IPageLoadingLogic> CreateCache(const TPart* part, NPage::TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr) {
        if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
            return MakeHolder<TBTreeIndexCache>(part, groupId, slices);
        } else {
            return MakeHolder<TFlatIndexCache>(part, groupId, slices);
        }
    }
}
}
}
