#pragma once

#include "flat_fwd_iface.h"
#include "flat_fwd_misc.h"
#include "flat_fwd_cache.h"
#include "flat_fwd_blobs.h"
#include "flat_fwd_conf.h"
#include "flat_fwd_sieve.h"
#include "flat_fwd_warmed.h"
#include "flat_table_subset.h"
#include "flat_part_iface.h"
#include "flat_part_store.h"
#include "flat_sausage_fetch.h"
#include "util_fmt_abort.h"
#include <util/generic/deque.h>
#include <util/random/random.h>
#include <util/generic/intrlist.h>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    namespace {
        bool IsIndexPage(EPage type) noexcept {
            return type == EPage::FlatIndex || type == EPage::BTreeIndex;
        }
    }

    using IPageCollection = NPageCollection::IPageCollection;
    using TFetch = NPageCollection::TFetch;
    using TGroupId = NPage::TGroupId;

    class TPartGroupLoadingQueue : public IPageLoadingQueue, public TIntrusiveListItem<TPartGroupLoadingQueue> {
    public:
        TPartGroupLoadingQueue(ui32 partIndex, ui64 cookie, 
                TIntrusiveConstPtr<IPageCollection> indexPageCollection, TIntrusiveConstPtr<IPageCollection> groupPageCollection, 
                THolder<IPageLoadingLogic> pageLoadingLogic)
            : PartIndex(partIndex)
            , Cookie(cookie)
            , IndexPageCollection(std::move(indexPageCollection))
            , GroupPageCollection(std::move(groupPageCollection))
            , PageLoadingLogic(std::move(pageLoadingLogic))
        {
        }

        IPageLoadingLogic* operator->() const
        {
            return PageLoadingLogic.Get();
        }

        ui64 AddToQueue(TPageId pageId, EPage type) noexcept override
        {
            if (IsIndexPage(type)) {
                return AddToQueue(pageId, type, IndexPageCollection, IndexFetch);
            } else {
                return AddToQueue(pageId, type, GroupPageCollection, GroupFetch);
            }
        }

    private:
        ui64 AddToQueue(TPageId pageId, EPage type, const TIntrusiveConstPtr<IPageCollection>& pageCollection, TAutoPtr<TFetch>& fetch) {
            if (!fetch) {
                fetch.Reset(new TFetch(Cookie, pageCollection, { }));
                fetch->Pages.reserve(16);
            }

            const auto meta = pageCollection->Page(pageId);

            if (EPage(meta.Type) != type || meta.Size == 0) {
                Y_ABORT("Got an invalid page");
            }

            fetch->Pages.emplace_back(pageId);

            return meta.Size;
        }

    public:
        const ui32 PartIndex;
        const ui64 Cookie;
        const TIntrusiveConstPtr<IPageCollection> IndexPageCollection;
        const TIntrusiveConstPtr<IPageCollection> GroupPageCollection;
        const THolder<IPageLoadingLogic> PageLoadingLogic;
        bool Grow = false;  /* Should call Forward(...) for preloading */
        TAutoPtr<TFetch> GroupFetch;
        TAutoPtr<TFetch> IndexFetch;
    };

    struct TEnv : public IPages {
        struct TGroupPages {
            THolder<IPageLoadingLogic> PageLoadingLogic;
            TIntrusiveConstPtr<IPageCollection> IndexPageCollection;
            TIntrusiveConstPtr<IPageCollection> GroupPageCollection;
        };

    public:
        TEnv(const TConf &conf, const TSubset &subset)
            : Salt(RandomNumber<ui32>())
            , Conf(conf)
            , Subset(subset)
            , Keys(Subset.Scheme->Tags(true))
            , MemTable(new TMemTableHandler(Keys, Conf.Edge,
                                        Conf.Trace ? &subset.Frozen : nullptr))
        {
            Y_ABORT_UNLESS(Conf.AheadHi >= Conf.AheadLo);
            Y_ABORT_UNLESS(std::is_sorted(Keys.begin(), Keys.end()));

            for (auto &one: Subset.Flatten)
                AddPartView(one);
        }

        void AddCold(const TPartView& partView) noexcept
        {
            auto r = ColdParts.insert(partView.Part.Get());
            Y_ABORT_UNLESS(r.second, "Cannot add a duplicate cold part");

            AddPartView(partView);
        }

        bool MayProgress() const noexcept
        {
            return Pending == 0;
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            auto type = part->GetPageType(pageId, groupId);

            if (groupId.IsMain() && IsIndexPage(type)) {
                // redirect index page to its actual group queue:
                groupId = PartIndexPageLocator[part].GetGroup(pageId);
            }

            return Get(GetQueue(part, groupId), pageId, type).Page;
        }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept override
        {
            return MemTable->Locate(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
                Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
            }

            ui32 room = part->GroupsCount + (lob == ELargeObj::Extern ? 1 : 0);

            return Get(GetQueue(part, room), ref, EPage::Opaque);
        }

        void DoSave(TIntrusiveConstPtr<IPageCollection> pageCollection, ui64 cookie, TArrayRef<NPageCollection::TLoadedPage> pages)
        {
            const ui32 epoch = ui32(cookie) - Salt;
            if (epoch < Epoch) {
                return; // ignore pages requested before Reset
            }
            Y_ABORT_UNLESS(epoch == Epoch, "Got an invalid part cookie on pages absorption");

            const ui32 groupQueueIndex = cookie >> 32;
            Y_ABORT_UNLESS(groupQueueIndex < GroupQueues.size(), "Got save request for unknown TPart cookie index");
            auto& queue = GroupQueues.at(groupQueueIndex);

            Y_ABORT_UNLESS(pages.size() <= Pending, "Page fwd cache got more pages than was requested");
            Pending -= pages.size();

            for (auto& page : pages) {
                auto type = EPage(pageCollection->Page(page.PageId).Type);
                if (IsIndexPage(type)) {
                    Y_ABORT_UNLESS(queue.IndexPageCollection->Label() == pageCollection->Label(), "TPart head storage doesn't match with fetch result");
                    queue->Fill(page, type);
                } else {
                    Y_ABORT_UNLESS(queue.GroupPageCollection->Label() == pageCollection->Label(), "TPart head storage doesn't match with fetch result");
                    queue->Fill(page, type);
                }
            }
        }

        IPages* Reset() noexcept
        {
            /* Current version of cache works only on forward iterations over
                parts and cannot handle backward jumps. This call is temporary
                workaround for clients that wants to go back. */

            Y_ABORT_UNLESS(!Conf.Trace, "Cannot reset NFwd in blobs tracing state");

            // Ignore all pages fetched before Reset
            Pending = 0;
            ++Epoch;

            Total = Stats();
            PartGroupQueues.clear();
            PartIndexPageLocator.clear();
            GroupQueues.clear();
            ColdParts.clear();

            for (auto &one : Subset.Flatten)
                AddPartView(one);

            return this;
        }

        TStat Stats() const noexcept
        {
            auto aggr = [](auto &&stat, const TPartGroupLoadingQueue &q) {
                return stat += q->Stat;
            };

            return std::accumulate(GroupQueues.begin(), GroupQueues.end(), Total, aggr);
        }

        TAutoPtr<TFetch> GrabFetches() noexcept
        {
            while (FetchingQueues) {
                auto queue = FetchingQueues.Front();

                if (std::exchange(queue->Grow, false)) {
                    (*queue)->Forward(queue, Max(ui64(1), Conf.AheadHi));
                }

                if (auto request = std::move(queue->IndexFetch)) {
                    Y_ABORT_UNLESS(request->Pages, "Shouldn't send empty requests");
                    Pending += request->Pages.size();
                    return request;
                }
                if (auto request = std::move(queue->GroupFetch)) {
                    Y_ABORT_UNLESS(request->Pages, "Shouldn't send empty requests");
                    Pending += request->Pages.size();
                    return request;
                }

                FetchingQueues.PopFront();
            }

            return nullptr;
        }

        TAutoPtr<TSeen> GrabTraces() noexcept
        {
            TDeque<TSieve> sieves(PartGroupQueues.size() - ColdParts.size() + 1);

            sieves.back() = MemTable->Traced(); /* blobs of memtable */

            ui64 total = sieves.back().Total();
            ui64 seen = sieves.back().Removed();

            for (auto &it: PartGroupQueues) {
                const auto *part = it.first;
                if (ColdParts.contains(part)) {
                    continue; // we don't trace cold parts
                }
                if (auto &blobs = part->Blobs) {
                    auto &q = GetQueue(part, part->GroupsCount + 1);
                    auto &line = dynamic_cast<TBlobs&>(*q.PageLoadingLogic);

                    Y_ABORT_UNLESS(q.PartIndex < (sieves.size() - 1));
                    sieves.at(q.PartIndex) = {
                        blobs,
                        line.GetFrames(),
                        line.GetSlices(),
                        line.Traced()
                    };

                    total += sieves[q.PartIndex].Total();
                    seen += sieves[q.PartIndex].Removed();
                }
            }

            return new TSeen{total, seen, std::move(sieves)};
        }

    private:
        TResult Get(TPartGroupLoadingQueue &queue, TPageId pageId, EPage type) noexcept
        {
            auto got = queue->Get(&queue, pageId, type, Conf.AheadLo);

            // Note: different levels of B-Tree may have different grow mindset
            queue.Grow |= got.Grow;

            if (queue.Grow || queue.IndexFetch || queue.GroupFetch) {
                FetchingQueues.PushBack(&queue);
            } else {
                Y_ABORT_IF(got.Need && got.Page == nullptr, "Cache line head don't want to do fetch but should");
            }

            return { got.Need, got.Page };
        }

        void AddPartView(const TPartView& partView) noexcept
        {
            const auto *part = partView.Part.Get();

            auto it = PartGroupQueues.find(part);
            Y_ABORT_UNLESS(it == PartGroupQueues.end(), "Cannot handle multiple part references in the same subset");

            ui32 partIndex = PartGroupQueues.size();

            auto& partGroupQueues = PartGroupQueues[part];
            partGroupQueues.reserve(part->GroupsCount + 2 + part->HistoricGroupsCount);

            for (ui32 groupIndex : xrange(ui32(part->GroupsCount))) {
                partGroupQueues.push_back(Settle(MakeCache(part, NPage::TGroupId(groupIndex), partView.Slices), partIndex));
            }
            partGroupQueues.push_back(Settle(MakeOuter(part, partView.Slices), partIndex));
            partGroupQueues.push_back(Settle(MakeExtern(part, partView.Slices), partIndex));
            for (ui32 groupIndex : xrange(ui32(part->HistoricGroupsCount))) {
                partGroupQueues.push_back(Settle(MakeCache(part, NPage::TGroupId(groupIndex, true), nullptr), partIndex));
            }
        }

        TPartGroupLoadingQueue& GetQueue(const TPart *part, TGroupId groupId) noexcept
        {
            ui16 room = (groupId.Historic ? part->GroupsCount + 2 : 0) + groupId.Index;
            return GetQueue(part, room);
        }

        TPartGroupLoadingQueue& GetQueue(const TPart *part, ui16 room) noexcept
        {
            auto it = PartGroupQueues.FindPtr(part);
            Y_ABORT_UNLESS(it, "NFwd cache trying to access part outside of subset");
            Y_ABORT_UNLESS(room < it->size(), "NFwd cache trying to access room out of bounds");
            
            auto queue = it->at(room);
            Y_ABORT_UNLESS(queue, "NFwd cache trying to access room that is not assigned");

            return *queue;
        }

        TPartGroupLoadingQueue* Settle(TGroupPages pages, ui32 partIndex) noexcept
        {
            if (pages.PageLoadingLogic || pages.IndexPageCollection || pages.GroupPageCollection) {
                const ui64 cookie = (ui64(GroupQueues.size()) << 32) | ui32(Salt + Epoch);

                GroupQueues.emplace_back(partIndex, cookie, 
                    std::move(pages.IndexPageCollection), std::move(pages.GroupPageCollection), 
                    std::move(pages.PageLoadingLogic));
                
                return &GroupQueues.back();
            } else {
                return nullptr; /* Will fail on access in GetQueue(...) */
            }
        }

        TGroupPages MakeCache(const TPart *part, NPage::TGroupId groupId, TIntrusiveConstPtr<TSlices> slices) noexcept
        {
            auto *partStore = CheckedCast<const TPartStore*>(part);

            Y_ABORT_UNLESS(groupId.Index < partStore->PageCollections.size(), "Got part without enough page collections");

            return {CreateCache(part, PartIndexPageLocator[part], groupId, slices), 
                partStore->PageCollections[0]->PageCollection,
                partStore->PageCollections[groupId.Index]->PageCollection};
        }

        TGroupPages MakeExtern(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
        {
            if (auto blobs = part->Blobs) {
                /* Should always materialize key columns to values since
                    current impl cannot handle outlined blobs in keys cells.
                    And have to materialize blobs of alien tablets in some
                    cases, this behaviour is ruled by Conf.Tablet value.
                 */

                const auto tablet = part->Label.TabletID();

                TVector<ui32> edge(part->Large->Stats().Tags.size(), Conf.Edge);

                for (auto& col : part->Scheme->AllColumns) {
                    if (TRowScheme::HasTag(Keys, col.Tag)) {
                        edge.at(col.Pos) = Max<ui32>();
                    } else if (Conf.Tablet && Conf.Tablet != tablet) {
                        edge.at(col.Pos) = Max<ui32>();
                    }
                }

                bool trace = Conf.Trace && !ColdParts.contains(part);

                return {MakeHolder<TBlobs>(part->Large, std::move(bounds), edge, trace), nullptr, blobs};
            } else {
                return {nullptr, nullptr, nullptr};
            }
        }

        TGroupPages MakeOuter(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
        {
            if (auto small = part->Small) {
                auto *partStore = CheckedCast<const TPartStore*>(part);

                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                auto pageCollection = partStore->PageCollections.at(partStore->GroupsCount)->PageCollection;

                return {MakeHolder<TBlobs>(small, std::move(bounds), edge, false), nullptr, pageCollection};
            } else {
                return {nullptr, nullptr, nullptr};
            }
        }

    private:
        const ui32 Salt;
        ui32 Epoch = 0;
        const TConf Conf;
        const TSubset &Subset;
        const TVector<ui32> Keys; /* Tags to expand ELargeObj references */

        TDeque<TPartGroupLoadingQueue> GroupQueues;
        THashMap<const TPart*, TVector<TPartGroupLoadingQueue*>> PartGroupQueues;
        THashMap<const TPart*, TIndexPageLocator> PartIndexPageLocator;
        THashSet<const TPart*> ColdParts;
        // Wrapper for memtable blobs
        TAutoPtr<TMemTableHandler> MemTable;
        TIntrusiveList<TPartGroupLoadingQueue> FetchingQueues;
        // Pending pages to load
        ui64 Pending = 0;
        // Accumulated stats on resets
        TStat Total;
    };
}
}
}
