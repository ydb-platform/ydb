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

    struct ICacheLineQueue {
        virtual void Advance(TPageId pageId) = 0;
        virtual bool Grow(ui64 onHold, ui64 onFetch, ui64 limit) = 0;
        virtual TPageId Next() = 0;
        virtual ~ICacheLineQueue() = default;
    };

    // class TBTreeIndexCacheLineQueue final : public ICacheLineQueue {
    // public:
    //     struct TNodeState {
    //         TPageId PageId;
    //         TRowId BeginRowId;
    //         TRowId EndRowId;
    //         ui64 BeginDataSize;
    //         ui64 EndDataSize;

    //         TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, ui64 beginDataSize, ui64 endDataSize)
    //             : PageId(pageId)
    //             , BeginRowId(beginRowId)
    //             , EndRowId(endRowId)
    //             , BeginDataSize(beginDataSize)
    //             , EndDataSize(endDataSize)
    //         {
    //         }
    //     };

    //     void Advance(TPageId pageId) override
    //     {
    //         Y_DEBUG_ABORT_UNLESS(!Queue.empty());
    //         Y_DEBUG_ABORT_UNLESS(Queue.begin()->PageId <= pageId);
    //         while (!Queue.empty() && Queue.begin()->PageId < pageId) {
    //             Queue.pop_front();
    //         }

    //         Y_DEBUG_ABORT_UNLESS(!Queue.empty());
    //         Y_DEBUG_ABORT_UNLESS(Queue.begin()->PageId == pageId);
    //         if (!Queue.empty() && Queue.begin()->PageId == pageId) {
    //             Queue.pop_front();
    //         }
    //     }

    // private:
    //     // const TBtreeIndexMeta Meta;
    //     TDeque<TNodeState> Queue;
    // };

    class TFlatIndexIndexCacheLineQueue final : public ICacheLineQueue {
    public:
        TFlatIndexIndexCacheLineQueue(TPageId indexPageId)
            : IndexPageId(indexPageId)
        {
        }

        void Advance(TPageId pageId) override {
            Y_ABORT_UNLESS(pageId == IndexPageId);
        }

        bool Grow(ui64, ui64, ui64) override {
            return false;
        }

        TPageId Next() override {
            Y_ABORT();
        }

    private:
        TPageId IndexPageId;
    };

    class TFlatIndexDataCacheLineQueue final : public ICacheLineQueue {
    public:
        TFlatIndexDataCacheLineQueue(TSharedData data, TRowId beginRowId, TRowId endRowId)
            : EndRowId(endRowId)
            , Index(std::move(data))
        {
            Iter = Index.LookupRow(beginRowId);
        }

        void Advance(TPageId pageId) override {
            Y_DEBUG_ABORT_UNLESS(Iter);
            Y_DEBUG_ABORT_UNLESS(Iter->GetPageId() <= pageId);
            while (Iter && Iter->GetPageId() < pageId) {
                Iter++;
            }

            Y_DEBUG_ABORT_UNLESS(Iter);
            Y_DEBUG_ABORT_UNLESS(Iter->GetPageId() == pageId);
            if (Iter && Iter->GetPageId() == pageId) {
                Iter++;
            }
        }

        bool Grow(ui64 onHold, ui64 onFetch, ui64 limit) override {
            return Iter && Iter->GetRowId() < EndRowId && onHold + onFetch < limit;
        }

        TPageId Next() override {
            Y_ABORT_UNLESS(Iter);
            auto result = Iter->GetPageId();
            Iter++;
            return result;
        }

    private:
        TRowId EndRowId;
        NPage::TIndex Index;
        NPage::TIndex::TIter Iter;
    };

    class TCacheLine {
        using TResult = IPageLoadingLogic::TResult;

    public:
        TCacheLine(TStat& stat)
            : Stat(stat)
        {
        }

        ~TCacheLine()
        {
            for (auto &it: Pages) {
                it.Release();
            }
        }

        void UseQueue(THolder<ICacheLineQueue> queue) noexcept
        {
            Y_ABORT_UNLESS(!Queue);

            Queue = std::move(queue);
        }

        TResult Get(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept
        {
            Y_ABORT_UNLESS(Queue);

            if (auto *page = Trace.Get(pageId)) {
                return { page, false, true };
            }

            DropPagesBefore(pageId);
            Shrink();

            if (PagesOffset == Pages.size()) { // isn't processed yet
                Queue->Advance(pageId);
                Request(head, pageId);
            }

            return {
                Pages.at(PagesOffset).Touch(pageId, Stat), 
                Queue->Grow(OnHold, OnFetch, lower), 
                true
            };
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept
        {
            Y_ABORT_UNLESS(Queue);

            while (Queue->Grow(OnHold, OnFetch, upper)) {
                Request(head, Queue->Next());
            }
        }

        void Fill(NPageCollection::TLoadedPage& page) noexcept
        {
            auto it = std::lower_bound(Pages.begin(), Pages.end(), page.PageId);
            if (it == Pages.end() || it->PageId != page.PageId) {
                Y_ABORT("Got page that hasn't been requested for load");
            } if (page.Data.size() > OnFetch) {
                Y_ABORT("Forward cache ahead counters is out of sync");
            }

            Stat.Saved += page.Data.size();
            OnFetch -= page.Data.size();
            OnHold += it->Settle(page); // settle of a dropped page returns 0 and releases its data

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
        ui64 OnHold = 0, OnFetch = 0;
        TStat& Stat;

        TLoadedPagesCircularBuffer<TPart::Trace> Trace;
        TDeque<TPage> Pages;
        ui32 PagesOffset = 0;

        THolder<ICacheLineQueue> Queue;
    };

    class TCache : public IPageLoadingLogic {
        enum EIndexState {
            DoStart,
            Valid,
            DoNext,
            Exhausted
        };

    public:
        using TGroupId = NPage::TGroupId;

        TCache(const TPart* part, TGroupId groupId, const TIntrusiveConstPtr<TSlices>& slices = nullptr)
            : Part(part)
        {
            if (slices && !slices->empty()) {
                BeginRowId = slices->front().BeginRowId();
                EndRowId = slices->back().EndRowId();
            } else {
                BeginRowId = 0;
                EndRowId = Max<TRowId>();
            }

            ui32 cacheLineCount = part->IndexPages.HasBTree() ? part->IndexPages.GetBTree(groupId).LevelCount + 1 : 2;
            for (ui32 level = 0; level < cacheLineCount; level++) {
                CacheLines.emplace_back(Stat);
            }
            if (part->IndexPages.HasBTree()) {
                // CacheLines[0].UseQueue(THolder<ICacheLineQueue> queue)
            } else {
                CacheLines[0].UseQueue(MakeHolder<TFlatIndexIndexCacheLineQueue>(part->IndexPages.GetFlat(groupId)));
            }
        }

        TResult Handle(IPageLoadingQueue *head, TPageId pageId, ui64 lower) noexcept override
        {
            auto type = Part->GetPageType(pageId);

            switch (type) {
                case EPage::BTreeIndex:
                    Y_ABORT();
                    break;
                case EPage::Index:
                    return CacheLines[0].Get(head, pageId, lower);
                case EPage::DataPage:
                    return CacheLines[1].Get(head, pageId, lower);
                default:
                    Y_ABORT("Unknown page type");
            }
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            for (auto& line : CacheLines) {
                line.Forward(head, upper);
            }
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            for (auto &page: loaded) {
                auto type = Part->GetPageType(page.PageId);

                switch (type) {
                    case EPage::BTreeIndex:
                        Y_ABORT();
                        break;
                    case EPage::Index:
                        CacheLines[1].UseQueue(MakeHolder<TFlatIndexDataCacheLineQueue>(page.Data, BeginRowId, EndRowId));
                        CacheLines[0].Fill(page);
                        break;
                    case EPage::DataPage:
                        CacheLines[1].Fill(page);
                        break;
                    default:
                        Y_ABORT("Unknown page type");
                }
            }
        }

    private:
        const TPart* const Part;
        TRowId BeginRowId, EndRowId;

        TDeque<TCacheLine> CacheLines;
    };
}
}
}
