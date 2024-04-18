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
    using IPageCollection = NPageCollection::IPageCollection;
    using TFetch = NPageCollection::TFetch;

    class TPageLoadingQueue : public IPageLoadingQueue, public TIntrusiveListItem<TPageLoadingQueue> {
    public:
        TPageLoadingQueue(ui32 partIndex, ui64 cookie, TIntrusiveConstPtr<IPageCollection> pageCollection, TAutoPtr<IPageLoadingLogic> line)
            : PartIndex(partIndex)
            , Cookie(cookie)
            , PageCollection(std::move(pageCollection))
            , PageLoadingLogic(line)
        {

        }

        IPageLoadingLogic* operator->() const
        {
            return PageLoadingLogic.Get();
        }

        ui64 AddToQueue(TPageId pageId, EPage type) noexcept override
        {
            if (!Fetch) {
                Fetch.Reset(new TFetch(Cookie, PageCollection, { }));
                Fetch->Pages.reserve(16);
            }

            const auto meta = PageCollection->Page(pageId);

            if (meta.Type != ui16(type) || meta.Size == 0)
                Y_ABORT("Got an invalid page");

            Fetch->Pages.emplace_back(pageId);

            return meta.Size;
        }

    public:
        const ui32 PartIndex;
        const ui64 Cookie;
        const TIntrusiveConstPtr<IPageCollection> PageCollection;
        const THolder<IPageLoadingLogic> PageLoadingLogic;
        bool Grow = false;  /* Should call Forward(...) for preloading */
        TAutoPtr<TFetch> Fetch;
    };

    struct TEnv : public IPages {
        struct TPages {
            THolder<IPageLoadingLogic> PageLoadingLogic;
            TIntrusiveConstPtr<IPageCollection> PageCollection;
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

        const TSharedData* TryGetPage(const TPart* part, TPageId ref, TGroupId groupId) override
        {
            ui16 room = (groupId.Historic ? part->GroupsCount + 2 : 0) + groupId.Index;
            ui32 queueIndex = GetQueueIndex(part, room);

            return Handle(Queues.at(queueIndex), ref).Page;
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

            return Handle(GetQueue(part, room), ref);
        }

        void DoSave(TIntrusiveConstPtr<IPageCollection> pageCollection, ui64 cookie, TArrayRef<NPageCollection::TLoadedPage> pages)
        {
            const ui32 epoch = ui32(cookie) - Salt;
            if (epoch < Epoch) {
                return; // ignore pages requested before Reset
            }

            Y_ABORT_UNLESS(epoch == Epoch,
                    "Got an invalid part cookie on pages absorption");

            const ui32 part = cookie >> 32;

            Y_ABORT_UNLESS(part < Queues.size(),
                    "Got save request for unknown TPart cookie index");

            Y_ABORT_UNLESS(Queues.at(part).PageCollection->Label() == pageCollection->Label(),
                    "TPart head storage doesn't match with fetch result");

            Y_ABORT_UNLESS(pages.size() <= Pending,
                    "Page fwd cache got more pages than was requested");

            Pending -= pages.size();

            Queues.at(part)->Apply(pages);
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
            PartQueues.clear();
            Queues.clear();
            ColdParts.clear();

            for (auto &one : Subset.Flatten)
                AddPartView(one);

            return this;
        }

        TStat Stats() const noexcept
        {
            auto aggr = [](auto &&stat, const TPageLoadingQueue &q) {
                return stat += q->Stat;
            };

            return std::accumulate(Queues.begin(), Queues.end(), Total, aggr);
        }

        TAutoPtr<TFetch> GrabFetches() noexcept
        {
            while (auto *q = FetchingQueues ? FetchingQueues.PopFront() : nullptr) {
                if (std::exchange(q->Grow, false)) {
                    (*q)->Forward(q, Max(ui64(1), Conf.AheadHi));
                }

                if (auto req = std::move(q->Fetch)) {
                    Y_ABORT_UNLESS(req->Pages, "Shouldn't sent an empty requests");

                    Pending += req->Pages.size();

                    return req;
                }
            }

            return nullptr;
        }

        TAutoPtr<TSeen> GrabTraces() noexcept
        {
            TDeque<TSieve> sieves(PartQueues.size() - ColdParts.size() + 1);

            sieves.back() = MemTable->Traced(); /* blobs of memtable */

            ui64 total = sieves.back().Total();
            ui64 seen = sieves.back().Removed();

            for (auto &it: PartQueues) {
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

            return new TSeen{ total, seen, std::move(sieves) };
        }

    private:
        TResult Handle(TPageLoadingQueue &q, TPageId ref) noexcept
        {
            auto got = q->Handle(&q, ref, Conf.AheadLo);

            if ((q.Grow = got.Grow) || bool(q.Fetch)) {
                FetchingQueues.PushBack(&q);
            } else if (got.Need && got.Page == nullptr) {
                // temporary hack for index pages as they are always stored in group 0 
                Y_ABORT_UNLESS(!FetchingQueues.Empty(), "Cache line head don't want to do fetch but should");
            }

            return { got.Need, got.Page };
        }

        void AddPartView(const TPartView& partView) noexcept
        {
            const auto *part = partView.Part.Get();

            auto it = PartQueues.find(part);
            Y_ABORT_UNLESS(it == PartQueues.end(), "Cannot handle multiple part references in the same subset");

            ui32 partIndex = PartQueues.size();

            auto& indexes = PartQueues[part];
            indexes.reserve(part->GroupsCount + 2 + part->HistoricGroupsCount);

            for (ui32 group : xrange(ui32(part->GroupsCount))) {
                indexes.push_back(Settle(MakeCache(part, NPage::TGroupId(group), partView.Slices), partIndex));
            }
            indexes.push_back(Settle(MakeOuter(part, partView.Slices), partIndex));
            indexes.push_back(Settle(MakeExtern(part, partView.Slices), partIndex));
            for (ui32 group : xrange(ui32(part->HistoricGroupsCount))) {
                indexes.push_back(Settle(MakeCache(part, NPage::TGroupId(group, true), nullptr), partIndex));
            }

            Y_ABORT_UNLESS(Queues.at(indexes[0]).PageCollection->Label() == part->Label,
                "Cannot handle multiple parts on the same page collection");
        }

        ui32 GetQueueIndex(const TPart *part, ui16 room) noexcept
        {
            auto it = PartQueues.find(part);
            Y_ABORT_UNLESS(it != PartQueues.end(),
                "NFwd cache trying to access part outside of subset");
            Y_ABORT_UNLESS(room < it->second.size(),
                "NFwd cache trying to access room out of bounds");
            Y_ABORT_UNLESS(it->second[room] != Max<ui16>(),
                "NFwd cache trying to access room that is not assigned");
            Y_ABORT_UNLESS(Queues.at(it->second[0]).PageCollection->Label() == part->Label,
                "Cannot handle multiple parts on the same page collection");

            return it->second[room];
        }

        TPageLoadingQueue& GetQueue(const TPart *part, ui16 room) noexcept
        {
            return Queues.at(GetQueueIndex(part, room));
        }

        ui32 Settle(TPages pages, ui32 partIndex) noexcept
        {
            if (pages.PageLoadingLogic || pages.PageCollection) {
                const ui64 cookie = (ui64(Queues.size()) << 32) | ui32(Salt + Epoch);

                Queues.emplace_back(partIndex, cookie, std::move(pages.PageCollection), pages.PageLoadingLogic);

                return Queues.size() - 1;
            } else {
                return Max<ui32>(); /* Will fail on access in GetQueue(...) */
            }
        }

        TPages MakeCache(const TPart *part, NPage::TGroupId groupId, TIntrusiveConstPtr<TSlices> slices) noexcept
        {
            auto *partStore = CheckedCast<const TPartStore*>(part);

            Y_ABORT_UNLESS(groupId.Index < partStore->PageCollections.size(), "Got part without enough page collections");

            auto& cache = partStore->PageCollections[groupId.Index];
            
            return {CreateCache(part, groupId, slices), cache->PageCollection};
        }

        TPages MakeExtern(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
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

                return {MakeHolder<TBlobs>(part->Large, std::move(bounds), edge, trace), blobs};
            } else {
                return {nullptr, nullptr};
            }
        }

        TPages MakeOuter(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
        {
            if (auto small = part->Small) {
                auto *partStore = CheckedCast<const TPartStore*>(part);

                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                auto pageCollection = partStore->PageCollections.at(partStore->GroupsCount)->PageCollection;

                return {MakeHolder<TBlobs>(small, std::move(bounds), edge, false), pageCollection};
            } else {
                return {nullptr, nullptr};
            }
        }

    private:
        const ui32 Salt;
        ui32 Epoch = 0;
        const TConf Conf;
        const TSubset &Subset;
        const TVector<ui32> Keys; /* Tags to expand ELargeObj references */

        TDeque<TPageLoadingQueue> Queues;
        THashMap<const TPart*, TSmallVec<ui32>> PartQueues;
        THashSet<const TPart*> ColdParts;
        // Wrapper for memtable blobs
        TAutoPtr<TMemTableHandler> MemTable;
        TIntrusiveList<TPageLoadingQueue> FetchingQueues;
        // Pending pages to load
        ui64 Pending = 0;
        // Accumulated stats on resets
        TStat Total;
    };
}
}
}
