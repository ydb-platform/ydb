#pragma once

#include "test_part.h"
#include <ydb/core/tablet_flat/flat_table_misc.h>
#include <ydb/core/tablet_flat/flat_fwd_cache.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_fwd_warmed.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>

#include <util/generic/cast.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    using NPage::IsIndexPage;

    enum class ELargeObjNeed {
        Has = 0,
        Yes = 1,
        No = 2,
    };

    struct TNoEnv : public TTestEnv {
        TNoEnv() = default;

        TNoEnv(bool pages, ELargeObjNeed lobs) : Pages(pages) , Lobs(lobs) { }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return TTestEnv::Locate(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            const bool pass = Lobs == ELargeObjNeed::Has;
            const bool need = Lobs == ELargeObjNeed::Yes;

            return
                pass ? TTestEnv::Locate(part, ref, lob) : TResult{need, nullptr };
        }

        const TSharedData* TryGetPage(const TPart *part, const TPageLocation& location, TGroupId groupId) override
        {
            return Pages ? TTestEnv::TryGetPage(part, location, groupId) : nullptr;
        }

        const TSharedData* TryGetIndexPage(const TPart *part, const TPageLocation& location, TGroupId groupId) override
        {
            return Pages ? TTestEnv::TryGetIndexPage(part, location, groupId) : nullptr;
        }

        bool Pages = false;
        ELargeObjNeed Lobs = ELargeObjNeed::Yes;
    };

    template<typename TRandom>
    class TFailEnv: public TTestEnv {
        struct TSeen {
            TSeen() = default;

            TSeen(const void *token, ui64 ref)
                : Token(token) , Ref(ref) { }

            bool operator==(const TSeen &seen) const
            {
                return Token == seen.Token && Ref == seen.Ref;
            }
            
            bool operator<(const TSeen &seen) const
            {
                return Token < seen.Token || Token == seen.Token && Ref < seen.Ref;
            }

            const void *Token = nullptr;
            ui64 Ref = Max<ui64>();
        };

    public:
        TFailEnv(TRandom &rnd, double rate = 1.)
            : Rate(rate)
            , Rnd(rnd)
            , Trace(8)
        {

        }

        ~TFailEnv()
        {
            if (Touches == 0 || (Rate < 1. && Success == Touches)) {
                Y_ABORT_S("Fail env was touched " << Touches << " times w/o fails");
            }
        }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return TTestEnv::Locate(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            if (ShouldPass((const void*)part->Large.Get(), ref, false)) {
                return TTestEnv::Locate(part, ref, lob);
            } else {
                return { true, nullptr };
            }
        }

        const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            return ShouldPass(part, location, part->DataSlot(groupId), false)
                ? TTestEnv::TryGetPage(part, location, groupId)
                : nullptr;
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            return ShouldPass(part, location, part->IndexSlot(groupId), true)
                ? TTestEnv::TryGetIndexPage(part, location, groupId)
                : nullptr;
        }

        bool ShouldPass(const TPart* part, const TPageLocation& location, ui32 slot, bool isIndex)
        {
            return ShouldPass((const void*)part,
                static_cast<ui64>(THash<TPageOffset>()(location.Offset)) ^ (ui64(slot) << 48),
                isIndex);
        }

        bool ShouldPass(const void *token, ui64 id, bool isIndex)
        {
            const TSeen seen(token, id);
            const auto pass = IsRecent(seen, isIndex) || AmILucky();

            Touches++, Success += pass ? 1 : 0;

            if (!pass) {
                if (isIndex) {
                    // allow to pass on index page next ttl times
                    IndexTraceTtl[seen] = Rnd.Uniform(5, 10);
                } else {
                    Offset = (Offset + 1) % Trace.size();
                    Trace[Offset] = seen;
                }
            }

            return pass;
        }

        bool IsRecent(TSeen seen, bool isIndex)
        {
            if (isIndex) {
                auto it = IndexTraceTtl.find(seen);
                if (it == IndexTraceTtl.end()) {
                    return false;
                }
                if (it->second-- <= 1) {
                    IndexTraceTtl.erase(it);
                }
                return true;
            } else {
                return std::find(Trace.begin(), Trace.end(), seen) != Trace.end();
            }
        }

        bool AmILucky()
        {
            return Rate >= 1. || Rnd.GenRandReal4() <= Rate;
        }

        const double Rate = 1.;
        TRandom &Rnd;
        ui64 Touches = 0;
        ui64 Success = 0;
        TVector<TSeen> Trace;
        TMap<TSeen, ui64> IndexTraceTtl;
        ui32 Offset = Max<ui32>();
    };

    class TForwardEnv : public IPages {
        using TPageLocation = NTable::NPage::TPageLocation;

        struct TPartGroupLoadingQueue : private NFwd::IPageLoadingQueue {
            TPartGroupLoadingQueue(TIntrusiveConstPtr<TStore> store, ui32 indexRoom, ui32 groupRoom,
                    TIntrusiveConstPtr<NPageCollection::IPageCollection> indexPageCollection,
                    TIntrusiveConstPtr<NPageCollection::IPageCollection> groupPageCollection,
                    THolder<NFwd::IPageLoadingLogic> line)
                : IndexPageCollection(std::move(indexPageCollection))
                , GroupPageCollection(std::move(groupPageCollection))
                , IndexRoom(indexRoom)
                , GroupRoom(groupRoom)
                , Store(std::move(store))
                , PageLoadingLogic(std::move(line))
            {
            }

            TResult DoLoad(NFwd::TPageOffset offset, EPage type, ui64 lower, ui64 upper)
            {
                if (std::exchange(Grow, false)) {
                    PageLoadingLogic->Forward(this, upper);
                }

                {
                    for (auto &fetchEntry: IndexFetch) {
                        auto* pageData = Store->GetPage(IndexRoom, fetchEntry.Offset);
                        NPageCollection::TLoadedPage page(
                            TPageLocation(fetchEntry.Offset, pageData->size(), fetchEntry.Type),
                            *pageData);
                        PageLoadingLogic->Fill(page, {}, fetchEntry.Type);
                    }
                }
                {
                    for (auto &fetchEntry: GroupFetch) {
                        auto* pageData = Store->GetPage(GroupRoom, fetchEntry.Offset);
                        NPageCollection::TLoadedPage page(fetchEntry, *pageData);
                        PageLoadingLogic->Fill(page, {}, fetchEntry.Type);
                    }
                }

                IndexFetch.clear();
                GroupFetch.clear();

                auto got = PageLoadingLogic->Get(this, offset, type, lower);

                Y_ENSURE((Grow = got.Grow) || IndexFetch || GroupFetch || got.Page);

                return { got.Need, got.Page };
            }

        private:
            ui64 AddToQueue(TPageOffset offset, EPage type, ui64 size, ui32 crc32) override
            {
                if (IsIndexPage(type)) {
                    IndexFetch.emplace_back(offset, size, type,  crc32);
                } else {
                    GroupFetch.emplace_back(offset, size, type,  crc32);
                }
                return size;
            }

        public:
            const TIntrusiveConstPtr<NPageCollection::IPageCollection> IndexPageCollection;
            const TIntrusiveConstPtr<NPageCollection::IPageCollection> GroupPageCollection;

        private:
            const ui32 IndexRoom = Max<ui32>();
            const ui32 GroupRoom = Max<ui32>();
            TVector<TPageLocation> IndexFetch;
            TVector<TPageLocation> GroupFetch;
            TIntrusiveConstPtr<TStore> Store;
            THolder<NFwd::IPageLoadingLogic> PageLoadingLogic;
            bool Grow = false;
        };

    public:
        using TTags = TArrayRef<const ui32>;

        TForwardEnv(ui64 raLo, ui64 raHi)
            : AheadLo(raLo)
            , AheadHi(raHi)
        {

        }

        TForwardEnv(ui64 raLo, ui64 raHi, TTags keys, ui32 edge)
            : AheadLo(raLo)
            , AheadHi(raHi)
            , Edge(edge)
            , MemTable(new NFwd::TMemTableHandler(keys, edge, nullptr))
        {

        }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return
                MemTable
                    ? MemTable->Locate(memTable, ref, tag)
                    : MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            InitPart(part);

            auto* partStore = CheckedCast<const TPartStore*>(part);

            if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
                Y_TABLET_ERROR("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
            }

            const auto room = (lob == ELargeObj::Extern)
                ? partStore->Store->GetExternRoom()
                : partStore->Store->GetOuterRoom();

            return Get(part, room).DoLoad(TPageOffset::FromPageIndex(ref), EPage::Opaque, AheadLo, AheadHi);
        }

        const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            Y_ENSURE(!IsIndexPage(location.Type), "Index page " << location << " is requested as a data page");
            return Get(part, location, groupId);
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            Y_ENSURE(location.Type == EPage::Undef || IsIndexPage(location.Type),
                "Page " << location << " is requested as an index page");
            // the group queue loads both index and data pages of the group
            return Get(part, location, groupId);
        }

    private:
        const TSharedData* Get(const TPart* part, const TPageLocation& location, TGroupId groupId)
        {
            InitPart(part);

            auto* partStore = CheckedCast<const TPartStore*>(part);

            Y_ENSURE(groupId.Index < partStore->Store->GetGroupCount());

            ui32 queueIndex = (groupId.Historic ? partStore->Store->GetRoomCount() : 0) + groupId.Index;

            return Get(part, queueIndex).DoLoad(location.Offset, location.Type, AheadLo, AheadHi).Page;
        }

        TPartGroupLoadingQueue& Get(const TPart *part, ui32 queueIndex)
        {
            auto& partGroupQueues = PartGroupQueues[part];

            Y_ENSURE(queueIndex < partGroupQueues.size());
            Y_ENSURE(partGroupQueues[queueIndex]);

            return *partGroupQueues[queueIndex];
        }

        void InitPart(const TPart *part)
        {
            auto* partStore = CheckedCast<const TPartStore*>(part);
            auto& partGroupQueues = PartGroupQueues[part];

            if (partGroupQueues.empty()) {
                auto makeCollection = [partStore](ui32 room)
                    -> TIntrusiveConstPtr<NPageCollection::IPageCollection>
                {
                    return TIntrusiveConstPtr<NPageCollection::IPageCollection>(
                        MakeIntrusive<TStorePageCollection>(partStore->Store, room).Release());
                };

                auto makeGroupQueue = [&](NPage::TGroupId groupId, TIntrusiveConstPtr<TSlices> slices) {
                    const ui32 indexRoom = part->IndexSlot(groupId);
                    const ui32 groupRoom = part->DataSlot(groupId);
                    auto indexPageCollection = makeCollection(indexRoom);
                    auto groupPageCollection = makeCollection(groupRoom);
                    return Settle(partStore, indexRoom, groupRoom,
                        NFwd::CreateCache(part, PartIndexPageLocators[part][groupId], groupId, slices,
                            groupPageCollection, indexPageCollection),
                        groupPageCollection, indexPageCollection);
                };

                partGroupQueues.reserve(partStore->Store->GetRoomCount() + part->HistoricGroupsCount);
                for (ui32 room : xrange(partStore->Store->GetRoomCount())) {
                    if (room < partStore->Store->GetGroupCount()) {
                        partGroupQueues.push_back(makeGroupQueue(NPage::TGroupId(room), partStore->Slices));
                        continue;
                    }
                    auto pageCollection = makeCollection(room);
                    if (room == partStore->Store->GetOuterRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, room, MakeOuter(partStore, pageCollection), pageCollection, pageCollection));
                    } else if (room == partStore->Store->GetExternRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, room, MakeExtern(partStore, pageCollection), pageCollection, pageCollection));
                    } else {
                        Y_TABLET_ERROR("Don't know how to work with room " << room);
                    }
                }
                for (ui32 group : xrange(part->HistoricGroupsCount)) {
                    partGroupQueues.push_back(makeGroupQueue(NPage::TGroupId(group, true), nullptr));
                }
            }
        }

        TPartGroupLoadingQueue* Settle(const TPartStore *part, ui32 indexRoom, ui32 groupRoom,
                THolder<NFwd::IPageLoadingLogic> line,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> groupPageCollection,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> indexPageCollection)
        {
            if (line) {
                GroupQueues.emplace_back(part->Store, indexRoom, groupRoom,
                    std::move(indexPageCollection), std::move(groupPageCollection),
                    std::move(line));
                return &GroupQueues.back();
            } else {
                return nullptr; /* Will fail on access in Head(...) */
            }
        }

        THolder<NFwd::IPageLoadingLogic> MakeExtern(const TPartStore *part, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection) const
        {
            if (auto &large = part->Large) {
                Y_ENSURE(part->Blobs, "Part has frames but not blobs");

                TVector<ui32> edges(large->Stats().Tags.size(), Edge);

                return MakeHolder<NFwd::TBlobs>(large, TSlices::All(), edges, false, std::move(pageCollection));
            } else
                return nullptr;
        }

        THolder<NFwd::IPageLoadingLogic> MakeOuter(const TPart *part, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection) const
        {
            if (auto &small = part->Small) {
                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                return MakeHolder<NFwd::TBlobs>(small, TSlices::All(), edge, false, std::move(pageCollection));
            } else
                return nullptr;
        }

    private:
        const ui64 AheadLo = 0;
        const ui64 AheadHi = 0;
        const ui32 Edge = Max<ui32>();
        TDeque<TPartGroupLoadingQueue> GroupQueues;
        TMap<const TPart*, TVector<TPartGroupLoadingQueue*>> PartGroupQueues;
        THashMap<const TPart*, TMap<TGroupId, NFwd::TIndexPageLocator>> PartIndexPageLocators;
        TAutoPtr<NFwd::TMemTableHandler> MemTable;   /* Wrapper for memtable blobs    */
    };

}
}
}
