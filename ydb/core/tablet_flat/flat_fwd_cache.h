#pragma once

#include "flat_part_iface.h"
#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_page.h"
#include "flat_table_part.h"
#include "flat_part_slice.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    using TPageOffset = NPage::TPageOffset;
    using IPageCollection = NPageCollection::IPageCollection;

    template<size_t Capacity>
    class TLoadedPagesCircularBuffer {
    public:
        const TSharedData* Get(TPageOffset offset) const
        {
            if (!FirstUnseenOffset.IsMax() && offset <= FirstUnseenOffset) {
                for (const auto& page : LoadedPages) {
                    if (page.Location.Offset == offset) {
                        return &page.Data;
                    }
                }

                Y_TABLET_ERROR("Failed to locate page within forward trace");
            }

            // next pages may be requested, ignore them
            return nullptr;
        }

        // returns released data size
        ui64 Emplace(TPage &page)
        {
            Y_ENSURE(page, "Cannot push invalid page to trace cache");

            Position = (Position + 1) % Capacity;

            const ui64 releasedDataSize = LoadedPages[Position].Data.size();
            DataSize = DataSize - releasedDataSize + page.Size;

            LoadedPages[Position].Data = page.Release();
            LoadedPages[Position].Location.Offset = page.Offset;
            LoadedPages[Position].Location.Size = page.Size;
            LoadedPages[Position].Location.Crc32 = page.Crc32;
            FirstUnseenOffset = FirstUnseenOffset.IsMax() ? page.Offset : Max(FirstUnseenOffset, page.Offset);

            return releasedDataSize;
        }

        ui64 GetDataSize() {
            return DataSize;
        }

    private:
        std::array<NPageCollection::TLoadedPage, Capacity> LoadedPages;
        ui32 Position = 0;
        ui64 DataSize = 0;
        TPageOffset FirstUnseenOffset;
    };

    class TIndexPageLocator {
        using TGroupId = NPage::TGroupId;

        struct TIndexPageLocation {
            TGroupId GroupId;
            ui32 Level;
        };

    public:
        void Add(TPageOffset offset, TGroupId groupId, ui32 level) {
            Y_ENSURE(Map.emplace(offset, TIndexPageLocation{groupId, level}).second, "All index pages should be unique");
        }

        ui32 GetLevel(TPageOffset offset) const {
            auto ptr = Map.FindPtr(offset);
            Y_ENSURE(ptr, "Unknown page");
            return ptr->Level;
        }

        TGroupId GetGroup(TPageOffset offset) {
            auto ptr = Map.FindPtr(offset);
            Y_ENSURE(ptr, "Unknown page");
            return ptr->GroupId;
        }

        const TMap<TPageOffset, TIndexPageLocation>& GetMap() {
            return Map;
        }

    private:
        TMap<TPageOffset, TIndexPageLocation> Map;
    };

    class TFlatIndexCache : public IPageLoadingLogic {
    public:
        using TGroupId = NPage::TGroupId;

        TFlatIndexCache(const TPart* part, TIndexPageLocator& indexPageLocator, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices, TIntrusiveConstPtr<IPageCollection> groupPageCollection, TIntrusiveConstPtr<IPageCollection> indexPageCollection)
            : GroupId(groupId)
            , GroupPageCollection(std::move(groupPageCollection))
            , IndexPage(GetRootLocation(part, groupId, indexPageCollection.Get()), 0, Max<TPageId>())
        {
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }

            indexPageLocator.Add(IndexPage.Offset, GroupId, 0);
        }

        ~TFlatIndexCache()
        {
        }

        TResult Get(IPageLoadingQueue *head, TPageOffset offset, EPage type, ui64 lower) override
        {
            if (type == EPage::FlatIndex) {
                Y_ENSURE(offset == IndexPage.Offset);

                // Note: doesn't affect read ahead limits, only stats
                if (IndexPage.Fetch == EFetch::None) {
                    Stat.Fetch += head->AddToQueue(IndexPage.Offset, EPage::FlatIndex, IndexPage.Size, IndexPage.Crc32);
                    IndexPage.Fetch = EFetch::Wait;
                }
                return {IndexPage.Touch(offset, Stat), false, true};
            }

            Y_ENSURE(type == EPage::DataPage);

            if (auto *page = Trace.Get(offset)) {
                return {page, false, true};
            }

            DropPagesBefore(offset);
            ShrinkPages();

            bool grow = OnHold + OnFetch <= lower;

            if (PagesBeginOffset == Pages.size()) { // isn't processed yet
                AdvanceNextPage(offset);
                RequestNextPage(head);
            }

            grow &= Iter && Iter->GetRowId() < EndRowId;

            return {Pages.at(PagesBeginOffset).Touch(offset, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) override
        {
            while (Iter && Iter->GetRowId() < EndRowId && OnHold + OnFetch < upper) {
                RequestNextPage(head);
            }
        }

        void Fill(NPageCollection::TLoadedPage& page, NSharedCache::TSharedPageRef sharedPageRef, EPage type) override
        {
            Stat.Saved += page.Data.size();

            if (type == EPage::FlatIndex) {
                // Note: doesn't affect read ahead limits, only stats
                Y_ENSURE(page.Location.Offset == IndexPage.Offset);
                Index.emplace(page.Data);
                Iter = Index->LookupRow(BeginRowId);
                IndexPage.Settle(page, std::move(sharedPageRef));
                return;
            }

            Y_ENSURE(type == EPage::DataPage);

            auto it = std::lower_bound(Pages.begin(), Pages.end(), page.Location.Offset);
            Y_ENSURE(it != Pages.end() && it->Offset == page.Location.Offset, "Got page that hasn't been requested for load");

            Y_ENSURE(page.Data.size() <= OnFetch, "Forward cache ahead counters is out of sync");
            OnFetch -= page.Data.size();
            OnHold += it->Settle(page, std::move(sharedPageRef)); // settle of a dropped page returns 0 and releases its data

            ShrinkPages();
        }

    private:
        static TPageLocation GetRootLocation(const TPart* part, TGroupId groupId, const IPageCollection* indexPageCollection) {
            auto flatPageId = part->IndexPages.GetFlat(groupId);
            if (flatPageId == Max<TPageId>())
                return TPageLocation::Max();
            return indexPageCollection->GetLocation(flatPageId);
        }

        void DropPagesBefore(TPageOffset offset)
        {
            while (PagesBeginOffset < Pages.size()) {
                auto &page = Pages.at(PagesBeginOffset);

                if (page.Offset >= offset) {
                    break;
                }

                if (page.Size == 0) {
                    Y_TABLET_ERROR("Dropping page that has not been touched");
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

        void ShrinkPages()
        {
            while (PagesBeginOffset && Pages.front().Ready()) {
                Y_ENSURE(Pages.front().Released(), "Forward cache page still holds data");
                Pages.pop_front();
                PagesBeginOffset--;
            }
        }

        TPageOffset DataPageOffset(TPageId pageId) const {
            return GroupPageCollection->GetLocation(pageId).Offset;
        }

        void AdvanceNextPage(TPageOffset offset)
        {
            Y_ENSURE(Iter);
            auto iterOffset = DataPageOffset(Iter->GetPageId());
            Y_ENSURE(iterOffset <= offset);
            while (Iter) {
                if (iterOffset >= offset) break;
                Iter++;
                if (Iter) iterOffset = DataPageOffset(Iter->GetPageId());
            }

            Y_ENSURE(Iter);
            Y_ENSURE(iterOffset == offset);
        }

        void RequestNextPage(IPageLoadingQueue *head)
        {
            Y_ENSURE(Iter);

            auto loc = GroupPageCollection->GetLocation(Iter->GetPageId());
            head->AddToQueue(loc.Offset, EPage::DataPage, loc.Size, loc.Crc32);

            Stat.Fetch += loc.Size;
            OnFetch += loc.Size;

            Y_ENSURE(!Pages || Pages.back().Offset < loc.Offset);
            Pages.emplace_back(loc.Offset, loc.Size, 0, Max<TPageId>(), loc.Crc32);
            Pages.back().Fetch = EFetch::Wait;

            Iter++;
        }

    private:
        const TGroupId GroupId;
        TIntrusiveConstPtr<IPageCollection> GroupPageCollection;
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
            TPageOffset Offset;
            ui64 PageSize;
            ui64 EndDataSize;
            ui32 Crc32 = 0;

            bool operator < (const TNodeState& another) const {
                return Offset < another.Offset;
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
            TPageOffset BeginOffset = TPageOffset::Max();
            TPageOffset EndOffset = TPageOffset::Min();
        };

    public:
        using TGroupId = NPage::TGroupId;

        static TPageLocation GetRootLocation(const NPage::TBtreeIndexMeta& meta,
            const IPageCollection* indexPageCollection, const IPageCollection* groupPageCollection)
        {
            return meta.LevelCount
                ? indexPageCollection->GetLocation(meta.PageId_)
                : groupPageCollection->GetLocation(meta.PageId_);
        }

        TBTreeIndexCache(const TPart* part, TIndexPageLocator& indexPageLocator, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices, TIntrusiveConstPtr<IPageCollection> groupPageCollection, TIntrusiveConstPtr<IPageCollection> indexPageCollection)
            : Meta(part->IndexPages.GetBTree(groupId))
            , GroupId(groupId)
            , GroupPageCollection(std::move(groupPageCollection))
            , IndexPageCollection(std::move(indexPageCollection))
            , IndexPageLocator(indexPageLocator)
        {
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }

            auto rootLoc = GetRootLocation(Meta, IndexPageCollection.Get(), GroupPageCollection.Get());
            Levels.resize(Meta.LevelCount + 1);
            Levels[0].Queue.push_back({rootLoc.Offset, rootLoc.Size, Meta.GetDataSize(), rootLoc.Crc32});
            Levels[0].BeginOffset = rootLoc.Offset;
            Levels[0].EndOffset = rootLoc.Offset;
            if (Meta.LevelCount) {
                IndexPageLocator.Add(rootLoc.Offset, GroupId, 0);
            }
        }

        ~TBTreeIndexCache()
        {
        }

        TResult Get(IPageLoadingQueue *head, TPageOffset offset, EPage type, ui64 lower) override
        {
            auto levelId = GetLevel(offset, type);
            auto& level = Levels[levelId];

            if (auto *page = level.Trace.Get(offset)) {
                return {page, false, true};
            }

            Y_ENSURE(level.BeginOffset <= offset && offset <= level.EndOffset, "Requested page " << offset << " is out of loaded slice "
                << BeginRowId << " " << EndRowId << " " << level.BeginOffset << " " << level.EndOffset << " with index " << Meta.ToString());

            DropPagesBefore(level, offset);
            ShrinkPages(level);

            bool grow = GetDataSize(level) <= lower;

            if (level.PagesBeginOffset == level.Pages.size()) { // isn't processed yet
                AdvanceNextPage(level, offset);
                RequestNextPage(level, head);
            }

            grow &= !level.Queue.empty();

            return {level.Pages.at(level.PagesBeginOffset).Touch(offset, Stat), grow, true};
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) override
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

        void Fill(NPageCollection::TLoadedPage& page, NSharedCache::TSharedPageRef sharedPageRef, EPage type) override
        {
            Stat.Saved += page.Data.size();

            auto levelId = GetLevel(page.Location.Offset, type);
            auto& level = Levels[levelId];

            auto it = level.Pages.begin() + level.PagesPendingOffset;
            Y_ENSURE(it != level.Pages.end(), "No pending pages");
            Y_ENSURE(it->Offset <= page.Location.Offset, "Got page that hasn't been requested for load");
            if (it->Offset < page.Location.Offset) {
                it = std::lower_bound(it, level.Pages.end(), page.Location.Offset);
            }
            Y_ENSURE(it != level.Pages.end() && it->Offset == page.Location.Offset, "Got page that hasn't been requested for load");

            if (levelId + 2 < Levels.size()) { // next level is index
                NPage::TBtreeIndexNode node(page.Data);
                for (auto pos : xrange(node.GetChildrenCount())) {
                    auto& child = node.GetShortChild(pos);
                    auto childOffset = IndexPageCollection->GetLocation(child.GetPageId()).Offset;
                    IndexPageLocator.Add(childOffset, GroupId, levelId + 1);
                }
            }

            it->Settle(page, std::move(sharedPageRef)); // settle of a dropped page releases its data

            AdvancePending(levelId);
            ShrinkPages(level);
        }

    private:
        const IPageCollection* PageCollectionForLevel(ui32 levelId) const noexcept {
            return (&Levels[levelId] == &Levels.back())
                ? GroupPageCollection.Get()
                : IndexPageCollection.Get();
        }

        ui32 GetLevel(TPageOffset offset, EPage type) {
            switch (type) {
                case EPage::BTreeIndex:
                    return IndexPageLocator.GetLevel(offset);
                case EPage::DataPage:
                    return Levels.size() - 1;
                default:
                    Y_TABLET_ERROR("Unknown page type");
            }
        }

        void DropPagesBefore(TLevel& level, TPageOffset offset)
        {
            while (level.PagesBeginOffset < level.Pages.size()) {
                auto &page = level.Pages.at(level.PagesBeginOffset);

                if (page.Offset >= offset) {
                    break;
                }

                if (page.Size == 0) {
                    Y_TABLET_ERROR("Dropping page that has not been touched");
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

        void AdvancePending(ui32 levelId)
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
                    auto& nextLevel = Levels[levelId + 1];
                    for (auto pos : xrange(node.GetChildrenCount())) {
                        auto& child = node.GetShortChild(pos);
                        if (child.GetRowCount() <= BeginRowId) {
                            continue;
                        }
                        auto childLoc = PageCollectionForLevel(levelId + 1)->GetLocation(child.GetPageId());
                        Y_ENSURE(!nextLevel.Queue || nextLevel.Queue.back().Offset < childLoc.Offset);
                        nextLevel.Queue.push_back({childLoc.Offset, childLoc.Size, child.GetDataSize(), childLoc.Crc32});
                        nextLevel.BeginOffset = Min(nextLevel.BeginOffset, childLoc.Offset);
                        nextLevel.EndOffset = Max(nextLevel.EndOffset, childLoc.Offset);
                        if (child.GetRowCount() >= EndRowId) {
                            break;
                        }
                    }
                }

                level.PagesPendingOffset++;
            }
        }

        void ShrinkPages(TLevel& level)
        {
            while (level.PagesBeginOffset && level.Pages.front().Ready()) {
                Y_ENSURE(level.Pages.front().Released(), "Forward cache page still holds data");
                level.Pages.pop_front();
                level.PagesBeginOffset--;
                if (level.PagesPendingOffset) {
                    level.PagesPendingOffset--;
                }
            }
        }

        ui64 GetDataSize(TLevel& level)
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

        void AdvanceNextPage(TLevel& level, TPageOffset offset)
        {
            auto& queue = level.Queue;

            Y_ENSURE(!queue.empty());
            Y_ENSURE(queue.front().Offset <= offset);
            while (!queue.empty() && queue.front().Offset < offset) {
                queue.pop_front();
            }

            Y_ENSURE(!queue.empty());
            Y_ENSURE(queue.front().Offset == offset);
        }

        void RequestNextPage(TLevel& level, IPageLoadingQueue *head)
        {
            Y_ENSURE(!level.Queue.empty());
            auto& front = level.Queue.front();

            auto type = &level == &Levels.back() ? EPage::DataPage : EPage::BTreeIndex;
            head->AddToQueue(front.Offset, type, front.PageSize, front.Crc32);

            Stat.Fetch += front.PageSize;

            Y_ENSURE(!level.Pages || level.Pages.back().Offset < front.Offset);
            level.Pages.push_back({TPage(front.Offset, front.PageSize, 0, Max<TPageId>(), front.Crc32), front.EndDataSize});
            level.Pages.back().Fetch = EFetch::Wait;

            level.Queue.pop_front();
        }

    private:
        const NPage::TBtreeIndexMeta& Meta;
        const TGroupId GroupId;
        TIntrusiveConstPtr<IPageCollection> GroupPageCollection;
        TIntrusiveConstPtr<IPageCollection> IndexPageCollection;
        TRowId BeginRowId, EndRowId;

        TIndexPageLocator& IndexPageLocator;

        TVector<TLevel> Levels;
    };

    inline THolder<IPageLoadingLogic> CreateCache(const TPart* part, TIndexPageLocator& indexPageLocator, NPage::TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices, TIntrusiveConstPtr<IPageCollection> groupPageCollection, TIntrusiveConstPtr<IPageCollection> indexPageCollection) {
        if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
            return MakeHolder<TBTreeIndexCache>(part, indexPageLocator, groupId, slices, std::move(groupPageCollection), std::move(indexPageCollection));
        } else {
            return MakeHolder<TFlatIndexCache>(part, indexPageLocator, groupId, slices, std::move(groupPageCollection), std::move(indexPageCollection));
        }
    }
}
}
}
