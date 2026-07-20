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

    namespace {
        bool IsIndexPage(EPage type) noexcept {
            return type == EPage::FlatIndex || type == EPage::BTreeIndex;
        }
    }

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

        const TSharedData* TryGetPage(const TPart *part, TPageLocation location, TGroupId groupId) override
        {
            return Pages ? TTestEnv::TryGetPage(part, location, groupId) : nullptr;
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

        const TSharedData* TryGetPage(const TPart* part, TPageLocation location, TGroupId groupId) override
        {
            auto pass = ShouldPass((const void*)part,
                static_cast<ui64>(THash<TPageOffset>()(location.Offset)) ^ (ui64(groupId.Raw()) << 48),
                location.Type == EPage::FlatIndex || location.Type == EPage::BTreeIndex);

            return pass ? TTestEnv::TryGetPage(part, location, groupId) : nullptr;
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

    class TStorePageCollection : public NPageCollection::IPageCollection {
        TIntrusiveConstPtr<TStore> Store;
        ui32 Room;
    public:
        TStorePageCollection(TIntrusiveConstPtr<TStore> store, ui32 room)
            : Store(std::move(store)), Room(room)
        {}

        const TLogoBlobID& Label() const noexcept override {
            static TLogoBlobID dummy(0, 0, 0, 0, 0, 0);
            return dummy;
        }

        ui32 Total() const noexcept override {
            return Store->PageCollectionPagesCount(Room);
        }

        NPageCollection::TInfo Page(ui32 page) const override {
            return {Store->GetPageSize(Room, page), 0};
        }

        NPageCollection::TBorder Bounds(ui32 page) const override {
            ui32 size = Store->GetPageSize(Room, page);
            return { size, { page, 0 }, { page, size } };
        }

        NPageCollection::TBorder Bounds(TPageLocation location) const override {
            return Bounds(location.Offset.AsPageIndex());
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            Y_TABLET_ERROR("Not implemented");
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return true;
        }

        bool Verify(TPageLocation location, TArrayRef<const char> data) const override {
            return data.size() == location.Size;
        }

        size_t BackingSize() const noexcept override {
            return Store->PageCollectionBytes(Room);
        }

        NTable::NPage::TPageLocation GetLocation(ui32 pageId) const override {
            auto* data = Store->GetPage(Room, pageId);
            return NTable::NPage::TPageLocation::FromPageIndex(pageId, data->size(), NTable::NPage::EPage::Undef, Store->GetPageChecksum(Room, pageId));
        }
    };

    class TForwardEnv : public IPages {
        using TPageLocation = NTable::NPage::TPageLocation;

        struct TPartGroupLoadingQueue : private NFwd::IPageLoadingQueue {
            TPartGroupLoadingQueue(TIntrusiveConstPtr<TStore> store, ui32 groupRoom,
                    TIntrusiveConstPtr<NPageCollection::IPageCollection> indexPageCollection,
                    TIntrusiveConstPtr<NPageCollection::IPageCollection> groupPageCollection,
                    THolder<NFwd::IPageLoadingLogic> line)
                : IndexPageCollection(std::move(indexPageCollection))
                , GroupPageCollection(std::move(groupPageCollection))
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
            const ui32 IndexRoom = 0;
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

        const TSharedData* TryGetPage(const TPart* part, TPageLocation location, TGroupId groupId) override
        {
            InitPart(part);

            auto* partStore = CheckedCast<const TPartStore*>(part);

            Y_ENSURE(groupId.Index < partStore->Store->GetGroupCount());

            auto type = location.Type;
            ui32 queueIndex = (groupId.Historic ? partStore->Store->GetRoomCount() : 0) + groupId.Index;

            if (groupId.IsMain() && IsIndexPage(type)) {
                // redirect index page to its actual group queue:
                groupId = PartIndexPageLocator[part].GetGroup(location.Offset);
                queueIndex = (groupId.Historic ? partStore->Store->GetRoomCount() : 0) + groupId.Index;
            }

            return Get(part, queueIndex).DoLoad(location.Offset, type, AheadLo, AheadHi).Page;
        }

    private:
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

                auto indexPageCollection = makeCollection(0);
                partGroupQueues.reserve(partStore->Store->GetRoomCount() + part->HistoricGroupsCount);
                for (ui32 room : xrange(partStore->Store->GetRoomCount())) {
                    auto groupPageCollection = makeCollection(room);
                    if (room < partStore->Store->GetGroupCount()) {
                        NPage::TGroupId groupId(room);
                        partGroupQueues.push_back(Settle(partStore, room,
                            NFwd::CreateCache(part, PartIndexPageLocator[part], groupId, partStore->Slices,
                                groupPageCollection, indexPageCollection),
                            groupPageCollection, indexPageCollection));
                    } else if (room == partStore->Store->GetOuterRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, MakeOuter(partStore, groupPageCollection), groupPageCollection, indexPageCollection));
                    } else if (room == partStore->Store->GetExternRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, MakeExtern(partStore, groupPageCollection), groupPageCollection, indexPageCollection));
                    } else {
                        Y_TABLET_ERROR("Don't know how to work with room " << room);
                    }
                }
                for (ui32 group : xrange(part->HistoricGroupsCount)) {
                    NPage::TGroupId groupId(group, true);
                    auto groupPageCollection = makeCollection(group);
                    partGroupQueues.push_back(Settle(partStore, group,
                        NFwd::CreateCache(part, PartIndexPageLocator[part], groupId,
                            nullptr, groupPageCollection, indexPageCollection),
                        groupPageCollection, indexPageCollection));
                }
            }
        }

        TPartGroupLoadingQueue* Settle(const TPartStore *part, ui16 room,
                THolder<NFwd::IPageLoadingLogic> line,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> groupPageCollection,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> indexPageCollection)
        {
            if (line) {
                GroupQueues.emplace_back(part->Store, room,
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
        THashMap<const TPart*, NFwd::TIndexPageLocator> PartIndexPageLocator;
        TAutoPtr<NFwd::TMemTableHandler> MemTable;   /* Wrapper for memtable blobs    */
    };

}
}
}
