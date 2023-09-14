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
#include "flat_sausage_packet.h"
#include "flat_sausage_fetch.h"
#include "util_fmt_abort.h"
#include "util_basics.h"
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
        TPageLoadingQueue(ui32 slot, ui64 cookie, TIntrusiveConstPtr<IPageCollection> pageCollection, TAutoPtr<IPageLoadingLogic> line)
            : Cookie(cookie)
            , PageCollection(std::move(pageCollection))
            , PageLoadingLogic(line)
            , Slot(slot)
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
                Y_FAIL("Got a non-data page while part index traverse");

            Fetch->Pages.emplace_back(pageId);

            return meta.Size;
        }

    public:
        const ui64 Cookie = Max<ui64>();
        const TIntrusiveConstPtr<IPageCollection> PageCollection;
        const TAutoPtr<IPageLoadingLogic> PageLoadingLogic;
        const ui32 Slot = Max<ui32>();
        bool Grow = false;  /* Should call Forward(...) for preloading */
        TAutoPtr<TFetch> Fetch;
    };

    struct TEnv: public IPages {
        using TSlot = ui32;
        using TSlotVec = TSmallVec<TSlot>;

        struct TEgg {
            TAutoPtr<IPageLoadingLogic> PageLoadingLogic;
            TIntrusiveConstPtr<IPageCollection> PageCollection;
        };

        struct TSimpleEnv {
            TMap<TPageId, NPageCollection::TLoadedPage> Pages;
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
            Y_VERIFY(Conf.AheadHi >= Conf.AheadLo);
            Y_VERIFY(std::is_sorted(Keys.begin(), Keys.end()));

            for (auto &one: Subset.Flatten)
                AddPartView(one);
        }

        void AddCold(const TPartView& partView) noexcept
        {
            auto r = ColdParts.insert(partView.Part.Get());
            Y_VERIFY(r.second, "Cannot add a duplicate cold part");

            AddPartView(partView);
        }

        bool MayProgress() const noexcept
        {
            return Pending == 0;
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId ref, TGroupId groupId) override
        {
            ui16 room = (groupId.Historic ? part->Groups + 2 : 0) + groupId.Index;
            TSlot slot = GetQueueSlot(part, room);

            if (part->IndexPages.Has(groupId, ref)) {
                return TryGetIndexPage(slot, ref);
            }
                
            return Handle(Queues.at(slot), ref).Page;
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

            ui32 room = part->Groups + (lob == ELargeObj::Extern ? 1 : 0);

            return Handle(GetQueue(part, room), ref);
        }

        void DoSave(TIntrusiveConstPtr<IPageCollection> pageCollection, ui64 cookie, TArrayRef<NPageCollection::TLoadedPage> pages)
        {
            const ui32 epoch = ui32(cookie) - Salt;
            if (epoch < Epoch) {
                return; // ignore pages requested before Reset
            }

            Y_VERIFY(epoch == Epoch,
                    "Got an invalid part cookie on pages absorption");

            const ui32 part = cookie >> 32;

            Y_VERIFY(part < Queues.size(),
                    "Got save request for unknown TPart cookie index");

            Y_VERIFY(Queues.at(part).PageCollection->Label() == pageCollection->Label(),
                    "TPart head storage doesn't match with fetch result");

            Y_VERIFY(pages.size() <= Pending,
                    "Page fwd cache got more pages than was requested");

            Pending -= pages.size();

            TVector<NPageCollection::TLoadedPage> queuePages(Reserve(pages.size()));
            for (auto& page : pages) {
                const auto &meta = pageCollection->Page(page.PageId);
                if (NTable::EPage(meta.Type) == EPage::Index) {
                    IndexPages.at(part).Pages[page.PageId] = page;
                } else {
                    queuePages.push_back(page);
                }
            }

            Queues.at(part)->Apply(queuePages);
        }

        IPages* Reset() noexcept
        {
            /* Current version of cache works only on forward iterations over
                parts and cannot handle backward jumps. This call is temporary
                workaround for clients that wants to go back. */

            Y_VERIFY(!Conf.Trace, "Cannot reset NFwd in blobs tracing state");

            // Ignore all pages fetched before Reset
            Pending = 0;
            ++Epoch;

            Total = Stats();
            Parts.clear();
            Queues.clear();
            IndexPages.clear();
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

            return
                std::accumulate(Queues.begin(), Queues.end(), Total, aggr);
        }

        TAutoPtr<TFetch> GrabFetches() noexcept
        {
            while (auto *q = Queue ? Queue.PopFront() : nullptr) {
                if (std::exchange(q->Grow, false))
                    (*q)->Forward(q, Max(ui64(1), Conf.AheadHi));

                if (auto req = std::move(q->Fetch)) {
                    Y_VERIFY(req->Pages, "Shouldn't sent an empty requests");

                    Pending += req->Pages.size();

                    return req;
                }
            }

            return nullptr;
        }

        TAutoPtr<TSeen> GrabTraces() noexcept
        {
            TDeque<TSieve> sieves(Parts.size() - ColdParts.size() + 1);

            sieves.back() = MemTable->Traced(); /* blobs of memtable */

            ui64 total = sieves.back().Total();
            ui64 seen = sieves.back().Removed();

            for (auto &it: Parts) {
                const auto *part = it.first;
                if (ColdParts.contains(part)) {
                    continue; // we don't trace cold parts
                }
                if (auto &blobs = part->Blobs) {
                    auto &q = GetQueue(part, part->Groups + 1);
                    auto &line = dynamic_cast<TBlobs&>(*q.PageLoadingLogic);

                    Y_VERIFY(q.Slot < (sieves.size() - 1));
                    sieves.at(q.Slot) = {
                        blobs,
                        line.GetFrames(),
                        line.GetSlices(),
                        line.Traced()
                    };

                    total += sieves[q.Slot].Total();
                    seen += sieves[q.Slot].Removed();
                }
            }

            return new TSeen{ total, seen, std::move(sieves) };
        }

    private:
        const TSharedData* TryGetIndexPage(TSlot slot, TPageId pageId) noexcept
        {
            // TODO: count index pages in Stats later

            auto &env = IndexPages.at(slot);
            auto pageIt = env.Pages.find(pageId);

            if (pageIt != env.Pages.end()) {
                return &pageIt->second.Data;
            } else {
                auto &queue = Queues.at(slot);
                queue.AddToQueue(pageId, EPage::Index);
                Queue.PushBack(&queue);
                return nullptr;
            }            
        }

        TResult Handle(TPageLoadingQueue &q, TPageId ref) noexcept
        {
            auto got = q->Handle(&q, ref, Conf.AheadLo);

            if ((q.Grow = got.Grow) || bool(q.Fetch)) {
                Queue.PushBack(&q);
            } else if (got.Need && got.Page == nullptr) {
                Y_FAIL("Cache line head don't want to do fetch but should");
            }

            return { got.Need, got.Page };
        }

        void AddPartView(const TPartView& partView) noexcept
        {
            const auto *part = partView.Part.Get();

            auto it = Parts.find(part);
            Y_VERIFY(it == Parts.end(), "Cannot handle multiple part references in the same subset");

            ui32 partSlot = Parts.size();

            auto& slots = Parts[part];
            slots.reserve(part->Groups + 2 + part->HistoricIndexes.size());

            for (ui32 group : xrange(ui32(part->Groups))) {
                slots.push_back(Settle(MakeCache(part, NPage::TGroupId(group), partView.Slices), partSlot));
            }
            slots.push_back(Settle(MakeOuter(part, partView.Slices), partSlot));
            slots.push_back(Settle(MakeExtern(part, partView.Slices), partSlot));
            for (ui32 group : xrange(ui32(part->HistoricIndexes.size()))) {
                slots.push_back(Settle(MakeCache(part, NPage::TGroupId(group, true), nullptr), partSlot));
            }

            Y_VERIFY(Queues.at(slots[0]).PageCollection->Label() == part->Label,
                "Cannot handle multiple parts on the same page collection");
        }

        TSlot GetQueueSlot(const TPart *part, ui16 room) noexcept
        {
            auto it = Parts.find(part);
            Y_VERIFY(it != Parts.end(),
                "NFwd cache tyring to access part outside of subset");
            Y_VERIFY(room < it->second.size(),
                "NFwd cache trying to access room out of bounds");
            Y_VERIFY(it->second[room] != Max<ui16>(),
                "NFwd cache trying to access room that is not assigned");
            Y_VERIFY(Queues.at(it->second[0]).PageCollection->Label() == part->Label,
                "Cannot handle multiple parts on the same page collection");

            return it->second[room];
        }

        TPageLoadingQueue& GetQueue(const TPart *part, ui16 room) noexcept
        {
            return Queues.at(GetQueueSlot(part, room));
        }

        TSlot Settle(TEgg egg, ui32 slot) noexcept
        {
            if (egg.PageLoadingLogic || egg.PageCollection) {
                const ui64 cookie = (ui64(Queues.size()) << 32) | ui32(Salt + Epoch);

                Queues.emplace_back(slot, cookie, std::move(egg.PageCollection), egg.PageLoadingLogic);
                IndexPages.emplace_back();

                return Queues.size() - 1;
            } else {
                return Max<TSlot>(); /* Will fail on access in GetQueue(...) */
            }
        }

        TEgg MakeCache(const TPart *part, NPage::TGroupId groupId, TIntrusiveConstPtr<TSlices> bounds) const noexcept
        {
            auto *partStore = dynamic_cast<const TPartStore*>(part);

            Y_VERIFY(groupId.Index < partStore->PageCollections.size(), "Got part without enough page collections");

            auto& cache = partStore->PageCollections[groupId.Index];
            auto& index = part->GetGroupIndex(groupId);
            if (cache->PageCollection->Total() < index.UpperPage())
                Y_FAIL("Part index last page is out of storage capacity");

            return { new NFwd::TCache(index, bounds), cache->PageCollection };
        }

        TEgg MakeExtern(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
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

                return
                    { new NFwd::TBlobs(part->Large, std::move(bounds), edge, trace), blobs};
            } else {
                return { nullptr, nullptr };
            }
        }

        TEgg MakeOuter(const TPart *part, TIntrusiveConstPtr<TSlices> bounds) const noexcept
        {
            if (auto small = part->Small) {
                auto *partStore = CheckedCast<const TPartStore*>(part);

                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                auto pageCollection = partStore->PageCollections.at(partStore->Groups)->PageCollection;

                return
                    { new NFwd::TBlobs(small, std::move(bounds), edge, false), pageCollection };
            } else {
                return { nullptr, nullptr };
            }
        }

    private:
        const ui32 Salt;
        ui32 Epoch = 0;
        const TConf Conf;
        const TSubset &Subset;
        const TVector<ui32> Keys; /* Tags to expand ELargeObj references */

        TDeque<TPageLoadingQueue> Queues;
        TDeque<TSimpleEnv> IndexPages;
        THashMap<const TPart*, TSlotVec> Parts;
        THashSet<const TPart*> ColdParts;
        // Wrapper for memable blobs
        TAutoPtr<TMemTableHandler> MemTable;
        // Waiting for read aheads
        TIntrusiveList<TPageLoadingQueue> Queue;
        // Pending pages to load
        ui64 Pending = 0;
        // Accumulated stats on resets
        TStat Total;
    };
}
}
}
