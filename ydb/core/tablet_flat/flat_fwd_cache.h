#pragma once

#include "flat_page_index.h"
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

        ui64 GetDataSize() {
            ui64 result = 0;
            for (const auto& page : LoadedPages) {
                result += page.Data.size();
            }
            return result;
        }

    private:
        std::array<NPageCollection::TLoadedPage, Capacity> LoadedPages;
        ui32 Offset = 0;
        TPageId FirstUnseenPageId = 0;
    };

    class TIndexPageLocator {
        using TGroupId = NPage::TGroupId;

        struct TPageLocation {
            TGroupId GroupId;
            ui32 Level;
        };

    public:
        void Add(TPageId pageId, TGroupId groupId, ui32 level) {
            Y_ABORT_UNLESS(Map.emplace(pageId, TPageLocation(groupId, level)).second);
        }

        ui32 GetLevel(TPageId pageId, ui32 levelsCount) const {
            auto ptr = Map.FindPtr(pageId);
            return ptr ? ptr->Level : levelsCount - 1;
        }

        TGroupId GetGroup(TPageId pageId) {
            auto ptr = Map.FindPtr(pageId);
            Y_ABORT_UNLESS(ptr, "Unknown page");
            return ptr->GroupId;
        }

        const TMap<TPageId, TPageLocation>& GetMap() {
            return Map;
        }

    private:
        TMap<TPageId, TPageLocation> Map;
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

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            if (pageId == IndexPage.PageId) {
                if (IndexPage.Fetch == EFetch::None) {
                    Stat.Fetch += head->AddToQueue(pageId, EPage::Index);
                    IndexPage.Fetch = EFetch::Wait;
                }
                return {IndexPage.Touch(pageId, Stat), false, true};
            }

            Y_DEBUG_ABORT_UNLESS(Part->GetPageType(pageId, GroupId) == EPage::DataPage);

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
            Y_DEBUG_ABORT_UNLESS(Iter && Iter->GetRowId() < EndRowId);

            while (Iter && Iter->GetRowId() < EndRowId && OnHold + OnFetch < upper) {
                RequestNextPage(head);
            }
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            auto it = Pages.begin();

            for (auto &one: loaded) {
                Stat.Saved += one.Data.size();

                if (one.PageId == IndexPage.PageId) {
                    Y_DEBUG_ABORT_UNLESS(Part->GetPageType(one.PageId, {}) == EPage::Index);
                    Index.emplace(one.Data);
                    Iter = Index->LookupRow(BeginRowId);
                    IndexPage.Settle(one);
                    continue;
                }

                Y_DEBUG_ABORT_UNLESS(Part->GetPageType(one.PageId, GroupId) == EPage::DataPage);
                if (it == Pages.end() || it->PageId > one.PageId) {
                    it = std::lower_bound(Pages.begin(), it, one.PageId);
                } else if (it->PageId < one.PageId) {
                    it = std::lower_bound(++it, Pages.end(), one.PageId);
                }
                Y_ABORT_UNLESS(it != Pages.end() && it->PageId == one.PageId, "Got page that hasn't been requested for load");
                
                Y_ABORT_UNLESS(one.Data.size() <= OnFetch, "Forward cache ahead counters is out of sync");
                OnFetch -= one.Data.size();
                OnHold += it->Settle(one); // settle of a dropped page returns 0 and releases its data

                ++it;
            }

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
        std::optional<NPage::TIndex> Index;
        NPage::TIndex::TIter Iter;

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
            , IndexPageLocator(indexPageLocator)
            , GroupId(groupId)
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
            Levels[0].Queue.emplace_back(meta.PageId, meta.DataSize);
            IndexPageLocator.Add(meta.PageId, GroupId, 0);
        }

        ~TBTreeIndexCache()
        {
            for (auto &level : Levels) {
                for (auto &it : level.Pages) {
                    it.Release();
                }
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            auto levelId = IndexPageLocator.GetLevel(pageId, Levels.size());
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

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            TVector<TDeque<TPageEx>::iterator> iters;
            for (auto& level : Levels) {
                iters.emplace_back(level.Pages.begin());
            }

            for (auto &one: loaded) {
                Stat.Saved += one.Data.size();
              
                auto levelId = IndexPageLocator.GetLevel(one.PageId, Levels.size());
                auto& it = iters[levelId];
                auto& level = Levels[levelId];

                if (it == level.Pages.end() || it->PageId > one.PageId) {
                    it = std::lower_bound(level.Pages.begin(), it, one.PageId);
                } else if (it->PageId < one.PageId) {
                    it = std::lower_bound(++it, level.Pages.end(), one.PageId);
                }
                Y_ABORT_UNLESS(it != level.Pages.end() && it->PageId == one.PageId, "Got page that hasn't been requested for load");

                if (levelId + 2 < Levels.size()) { // next level is index
                    NPage::TBtreeIndexNode node(one.Data);
                    for (auto pos : xrange(node.GetChildrenCount())) {
                        IndexPageLocator.Add(node.GetShortChild(pos).PageId, GroupId, levelId + 1);
                    }
                }
                
                it->Settle(one); // settle of a dropped page releases its data

                ++it;
            }

            for (auto levelId : xrange<ui32>(Levels.size() - 1)) {
                QueueChildren(levelId);
            }

            for (auto& level : Levels) {
                ShrinkPages(level);
            }
        }

    private:
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
                    // Note: keep page in IndexPageLocator for simplicity
                } else {
                    page.Release();
                    *(page.Ready() ? &Stat.After : &Stat.Before) += page.Size;
                }

                // keep pending pages but increment offset
                level.PagesBeginOffset++;
            }
        }

        void QueueChildren(ui32 levelId) noexcept
        {
            Y_ABORT_UNLESS(levelId + 1 < Levels.size());

            auto& level = Levels[levelId];

            while (level.PagesPendingOffset < level.Pages.size()) {
                auto &page = level.Pages.at(level.PagesPendingOffset);

                if (!page.Ready()) {
                    break;
                }

                if (page) {
                    NPage::TBtreeIndexNode node(page.Data);
                    for (auto pos : xrange(node.GetChildrenCount())) {
                        auto& child = node.GetShortChild(pos);
                        if (child.RowCount <= BeginRowId) {
                            continue;
                        }
                        Y_ABORT_UNLESS(!Levels[levelId + 1].Queue || Levels[levelId + 1].Queue.back().PageId < child.PageId);
                        Levels[levelId + 1].Queue.emplace_back(child.PageId, child.DataSize);
                        if (child.RowCount >= EndRowId) {
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
                        : level.Pages.rbegin()->EndDataSize - level.Pages.begin()->EndDataSize + level.Pages.begin()->Size);
            } else {
                return level.Pages.empty() 
                    ? 0 
                    : level.Pages.rbegin()->EndDataSize - level.Pages.begin()->EndDataSize;
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
            level.Pages.emplace_back(TPage(pageId, size, 0, Max<TPageId>()), level.Queue.front().EndDataSize);
            level.Pages.back().Fetch = EFetch::Wait;

            level.Queue.pop_front();
        }

    private:
        const TPart* Part;
        TIndexPageLocator& IndexPageLocator;
        const TGroupId GroupId;
        TRowId BeginRowId, EndRowId;
        
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
