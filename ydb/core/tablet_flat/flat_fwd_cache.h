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
            DataSize = DataSize - releasedDataSize + page.Size;

            LoadedPages[Offset].Data = page.Release();
            LoadedPages[Offset].PageId = page.PageId;
            FirstUnseenPageId = Max(FirstUnseenPageId, page.PageId + 1);

            return releasedDataSize;
        }

        ui64 GetDataSize() {
            return DataSize;
        }

    private:
        std::array<NPageCollection::TLoadedPage, Capacity> LoadedPages;
        ui32 Offset = 0;
        ui64 DataSize = 0;
        TPageId FirstUnseenPageId = 0;
    };

    class TIndexPageLocator {
        using TGroupId = NPage::TGroupId;

        struct TIndexPageLocation {
            TGroupId GroupId;
            ui32 Level;
        };

    public:
        void Add(TPageId pageId, TGroupId groupId, ui32 level) {
            Y_ABORT_UNLESS(Map.emplace(pageId, TIndexPageLocation{groupId, level}).second, "All index pages should be unique");
        }

        ui32 GetLevel(TPageId pageId) const {
            auto ptr = Map.FindPtr(pageId);
            Y_ABORT_UNLESS(ptr, "Unknown page");
            return ptr->Level;
        }

        TGroupId GetGroup(TPageId pageId) {
            auto ptr = Map.FindPtr(pageId);
            Y_ABORT_UNLESS(ptr, "Unknown page");
            return ptr->GroupId;
        }

        const TMap<TPageId, TIndexPageLocation>& GetMap() {
            return Map;
        }

    private:
        TMap<TPageId, TIndexPageLocation> Map;
    };

    class TFlatIndexCache : public IPageLoadingLogic {
    public:
        using TGroupId = NPage::TGroupId;

        TFlatIndexCache(const TPart* part, TIndexPageLocator& indexPageLocator, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : Part(part)
            , GroupId(groupId)
            , IndexPage(Part->IndexPages.GetFlat(groupId), Part->GetPageSize(Part->IndexPages.GetFlat(groupId), {}), 0, Max<TPageId>())
        { 
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }

            indexPageLocator.Add(IndexPage.PageId, GroupId, 0);
        }

        ~TFlatIndexCache()
        {
            IndexPage.Release();
            for (auto &it : Pages) {
                it.Release();
            }
        }

        TResult Get(IPageLoadingQueue *head, TPageId pageId, EPage type, ui64 lower) noexcept override
        {
            if (type == EPage::FlatIndex) {
                Y_ABORT_UNLESS(pageId == IndexPage.PageId);

                // Note: doesn't affect read ahead limits, only stats
                if (IndexPage.Fetch == EFetch::None) {
                    Stat.Fetch += head->AddToQueue(pageId, EPage::FlatIndex);
                    IndexPage.Fetch = EFetch::Wait;
                }
                return {IndexPage.Touch(pageId, Stat), false, true};
            }

            Y_ABORT_UNLESS(type == EPage::DataPage);

            if (auto *page = Trace.Get(pageId)) {
                return {page, false, true};
            }

            DropPagesBefore(pageId);
            ShrinkPages();

            bool grow = OnHold + OnFetch <= lower;

            if (PagesBeginOffset == Pages.size()) { // isn't processed yet
                AdvanceNextPage(pageId);
                RequestNextPage(head);
            }

            grow &= Iter && Iter->GetRowId() < EndRowId;

            return {Pages.at(PagesBeginOffset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            while (Iter && Iter->GetRowId() < EndRowId && OnHold + OnFetch < upper) {
                RequestNextPage(head);
            }
        }

        void Fill(NPageCollection::TLoadedPage& page, EPage type) noexcept override
        {
            Stat.Saved += page.Data.size();
            
            if (type == EPage::FlatIndex) {
                // Note: doesn't affect read ahead limits, only stats
                Y_ABORT_UNLESS(page.PageId == IndexPage.PageId);
                Index.emplace(page.Data);
                Iter = Index->LookupRow(BeginRowId);
                IndexPage.Settle(page);
                return;
            }

            Y_ABORT_UNLESS(type == EPage::DataPage);

            auto it = std::lower_bound(Pages.begin(), Pages.end(), page.PageId);
            Y_ABORT_UNLESS(it != Pages.end() && it->PageId == page.PageId, "Got page that hasn't been requested for load");
            
            Y_ABORT_UNLESS(page.Data.size() <= OnFetch, "Forward cache ahead counters is out of sync");
            OnFetch -= page.Data.size();
            OnHold += it->Settle(page); // settle of a dropped page returns 0 and releases its data

            ShrinkPages();
        }

    private:
        void DropPagesBefore(TPageId pageId) noexcept
        {
            while (PagesBeginOffset < Pages.size()) {
                auto &page = Pages.at(PagesBeginOffset);

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
                PagesBeginOffset++;
            }
        }

        void ShrinkPages() noexcept
        {
            while (PagesBeginOffset && Pages.front().Ready()) {
                Pages.pop_front();
                PagesBeginOffset--;
            }
        }

        void AdvanceNextPage(TPageId pageId) noexcept
        {
            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() <= pageId);
            while (Iter && Iter->GetPageId() < pageId) {
                Iter++;
            }

            Y_ABORT_UNLESS(Iter);
            Y_ABORT_UNLESS(Iter->GetPageId() == pageId);
        }

        void RequestNextPage(IPageLoadingQueue *head) noexcept
        {
            Y_ABORT_UNLESS(Iter);

            auto size = head->AddToQueue(Iter->GetPageId(), EPage::DataPage);

            Stat.Fetch += size;
            OnFetch += size;

            Y_ABORT_UNLESS(!Pages || Pages.back().PageId < Iter->GetPageId());
            Pages.emplace_back(Iter->GetPageId(), size, 0, Max<TPageId>());
            Pages.back().Fetch = EFetch::Wait;

            Iter++;
        }

    private:
        const TPart* Part;
        const TGroupId GroupId;
        TRowId BeginRowId, EndRowId;
        
        TPage IndexPage;
        std::optional<NPage::TFlatIndex> Index;
        NPage::TFlatIndex::TIter Iter;

        TLoadedPagesCircularBuffer<TPart::Trace> Trace;

        /* Forward cache line state */
        ui64 OnHold = 0, OnFetch = 0;
        TDeque<TPage> Pages;
        ui32 PagesBeginOffset = 0;
    };

    class TBTreeIndexCache : public IPageLoadingLogic {
        struct TNodeState {
            TPageId PageId;
            ui64 EndDataSize;

            bool operator < (const TNodeState& another) const {
                return PageId < another.PageId;
            }
        };

        struct TPageEx : public TPage {
            ui64 EndDataSize;
        };

        struct TLevel {
            TLoadedPagesCircularBuffer<TPart::Trace> Trace;
            TDeque<TPageEx> Pages;
            ui32 PagesBeginOffset = 0, PagesPendingOffset = 0;
            TDeque<TNodeState> Queue;
        };

    public:
        using TGroupId = NPage::TGroupId;

        TBTreeIndexCache(const TPart* part, TIndexPageLocator& indexPageLocator, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : Part(part)
            , GroupId(groupId)
            , IndexPageLocator(indexPageLocator)
        {
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }

            auto& meta = Part->IndexPages.GetBTree(groupId);
            Levels.resize(meta.LevelCount + 1);
            Levels[0].Queue.push_back({meta.GetPageId(), meta.GetDataSize()});
            if (meta.LevelCount) {
                IndexPageLocator.Add(meta.GetPageId(), GroupId, 0);
            }
        }

        ~TBTreeIndexCache()
        {
            for (auto &level : Levels) {
                for (auto &it : level.Pages) {
                    it.Release();
                }
            }
        }

        TResult Get(IPageLoadingQueue *head, TPageId pageId, EPage type, ui64 lower) noexcept override
        {
            auto levelId = GetLevel(pageId, type);
            auto& level = Levels[levelId];

            if (auto *page = level.Trace.Get(pageId)) {
                return {page, false, true};
            }

            DropPagesBefore(level, pageId);
            ShrinkPages(level);

            bool grow = GetDataSize(level) <= lower;

            if (level.PagesBeginOffset == level.Pages.size()) { // isn't processed yet
                AdvanceNextPage(level, pageId);
                RequestNextPage(level, head);
            }

            grow &= !level.Queue.empty();

            return {level.Pages.at(level.PagesBeginOffset).Touch(pageId, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            for (auto& level : Levels) {
                if (level.Pages.empty()) {
                    // level hasn't been used yet
                    continue;
                }
                while (!level.Queue.empty() && GetDataSize(level) < upper) {
                    RequestNextPage(level, head);
                }
            }
        }

        void Fill(NPageCollection::TLoadedPage& page, EPage type) noexcept override
        {
            Stat.Saved += page.Data.size();
              
            auto levelId = GetLevel(page.PageId, type);
            auto& level = Levels[levelId];

            auto it = level.Pages.begin() + level.PagesPendingOffset;
            Y_ABORT_UNLESS(it != level.Pages.end(), "No pending pages");
            Y_ABORT_UNLESS(it->PageId <= page.PageId, "Got page that hasn't been requested for load");
            if (it->PageId < page.PageId) {
                it = std::lower_bound(it, level.Pages.end(), page.PageId);
            }
            Y_ABORT_UNLESS(it != level.Pages.end() && it->PageId == page.PageId, "Got page that hasn't been requested for load");

            if (levelId + 2 < Levels.size()) { // next level is index
                NPage::TBtreeIndexNode node(page.Data);
                for (auto pos : xrange(node.GetChildrenCount())) {
                    IndexPageLocator.Add(node.GetShortChild(pos).GetPageId(), GroupId, levelId + 1);
                }
            }
            
            it->Settle(page); // settle of a dropped page releases its data

            AdvancePending(levelId);
            ShrinkPages(level);
        }

    private:
        ui32 GetLevel(TPageId pageId, EPage type) {
            switch (type) {
                case EPage::BTreeIndex:
                    return IndexPageLocator.GetLevel(pageId);
                case EPage::DataPage:
                    return Levels.size() - 1;
                default:
                    Y_ABORT("Unknown page type");
            }
        }

        void DropPagesBefore(TLevel& level, TPageId pageId) noexcept
        {
            while (level.PagesBeginOffset < level.Pages.size()) {
                auto &page = level.Pages.at(level.PagesBeginOffset);

                if (page.PageId >= pageId) {
                    break;
                }

                if (page.Size == 0) {
                    Y_ABORT("Dropping page that has not been touched");
                } else if (page.Usage == EUsage::Keep && page) {
                    level.Trace.Emplace(page);
                    // Note: keep dropped pages in IndexPageLocator for simplicity
                } else {
                    page.Release();
                    *(page.Ready() ? &Stat.After : &Stat.Before) += page.Size;
                }

                // keep pending pages but increment offset
                level.PagesBeginOffset++;
            }
        }

        void AdvancePending(ui32 levelId) noexcept
        {
            auto& level = Levels[levelId];

            while (level.PagesPendingOffset < level.Pages.size()) {
                auto &page = level.Pages.at(level.PagesPendingOffset);

                if (!page.Ready()) {
                    // queue should be sorted, at first wait pending pages
                    break;
                }

                if (levelId + 1 < Levels.size() && page) {
                    NPage::TBtreeIndexNode node(page.Data);
                    for (auto pos : xrange(node.GetChildrenCount())) {
                        auto& child = node.GetShortChild(pos);
                        if (child.GetRowCount() <= BeginRowId) {
                            continue;
                        }
                        Y_ABORT_UNLESS(!Levels[levelId + 1].Queue || Levels[levelId + 1].Queue.back().PageId < child.GetPageId());
                        Levels[levelId + 1].Queue.push_back({child.GetPageId(), child.GetDataSize()});
                        if (child.GetRowCount() >= EndRowId) {
                            break;
                        }
                    }
                }

                level.PagesPendingOffset++;
            }
        }

        void ShrinkPages(TLevel& level) noexcept
        {
            while (level.PagesBeginOffset && level.Pages.front().Ready()) {
                level.Pages.pop_front();
                level.PagesBeginOffset--;
                if (level.PagesPendingOffset) {
                    level.PagesPendingOffset--;
                }
            }
        }

        ui64 GetDataSize(TLevel& level) noexcept
        {
            if (&level == &Levels.back()) {
                return 
                    level.Trace.GetDataSize() +
                    (level.Pages.empty() 
                        ? 0 
                        // Note: for simplicity consider pages as sequential
                        : level.Pages.back().EndDataSize - level.Pages.front().EndDataSize + level.Pages.front().Size);
            } else {
                return level.Pages.empty() 
                    ? 0 
                    : level.Pages.back().EndDataSize - level.Pages.front().EndDataSize;
            }
        }

        void AdvanceNextPage(TLevel& level, TPageId pageId) noexcept
        {
            auto& queue = level.Queue;

            Y_ABORT_UNLESS(!queue.empty());
            Y_ABORT_UNLESS(queue.front().PageId <= pageId);
            while (!queue.empty() && queue.front().PageId < pageId) {
                queue.pop_front();
            }

            Y_ABORT_UNLESS(!queue.empty());
            Y_ABORT_UNLESS(queue.front().PageId == pageId);
        }

        void RequestNextPage(TLevel& level, IPageLoadingQueue *head) noexcept
        {
            Y_ABORT_UNLESS(!level.Queue.empty());
            auto pageId = level.Queue.front().PageId;

            auto type = &level == &Levels.back() ? EPage::DataPage : EPage::BTreeIndex;
            auto size = head->AddToQueue(pageId, type);

            Stat.Fetch += size;

            Y_ABORT_UNLESS(!level.Pages || level.Pages.back().PageId < pageId);
            level.Pages.push_back({TPage(pageId, size, 0, Max<TPageId>()), level.Queue.front().EndDataSize});
            level.Pages.back().Fetch = EFetch::Wait;

            level.Queue.pop_front();
        }

    private:
        const TPart* Part;
        const TGroupId GroupId;
        TRowId BeginRowId, EndRowId;

        TIndexPageLocator& IndexPageLocator;
        
        TVector<TLevel> Levels;
    };

    inline THolder<IPageLoadingLogic> CreateCache(const TPart* part, TIndexPageLocator& indexPageLocator, NPage::TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr) {
        if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
            return MakeHolder<TBTreeIndexCache>(part, indexPageLocator, groupId, slices);
        } else {
            return MakeHolder<TFlatIndexCache>(part, indexPageLocator, groupId, slices);
        }
    }
}
}
}
