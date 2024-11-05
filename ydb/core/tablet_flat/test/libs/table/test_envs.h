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

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept override
        {
            return TTestEnv::Locate(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            const bool pass = Lobs == ELargeObjNeed::Has;
            const bool need = Lobs == ELargeObjNeed::Yes;

            return
                pass ? TTestEnv::Locate(part, ref, lob) : TResult{need, nullptr };
        }

        const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
        {
            return Pages ? TTestEnv::TryGetPage(part, pageId, groupId) : nullptr;
        }

        bool Pages = false;
        ELargeObjNeed Lobs = ELargeObjNeed::Yes;
    };

    template<typename TRandom>
    class TFailEnv: public TTestEnv {
        struct TSeen {
            TSeen() = default;

            TSeen(const void *token, ui64 ref) noexcept
                : Token(token) , Ref(ref) { }

            bool operator==(const TSeen &seen) const noexcept
            {
                return Token == seen.Token && Ref == seen.Ref;
            }
            
            bool operator<(const TSeen &seen) const noexcept
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
                Y_Fail("Fail env was touched " << Touches << " times w/o fails");
            }
        }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept override
        {
            return TTestEnv::Locate(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            if (ShouldPass((const void*)part->Large.Get(), ref, false)) {
                return TTestEnv::Locate(part, ref, lob);
            } else {
                return { true, nullptr };
            }
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            auto pass = ShouldPass((const void*)part, pageId | (ui64(groupId.Raw()) << 32), 
                part->GetPageType(pageId, groupId) == EPage::FlatIndex || part->GetPageType(pageId, groupId) == EPage::BTreeIndex);

            return pass ? TTestEnv::TryGetPage(part, pageId, groupId) : nullptr;
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

        bool IsRecent(TSeen seen, bool isIndex) noexcept
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

        bool AmILucky() noexcept
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
        struct TPartGroupLoadingQueue : private NFwd::IPageLoadingQueue {
            TPartGroupLoadingQueue(TIntrusiveConstPtr<TStore> store, ui32 groupRoom, THolder<NFwd::IPageLoadingLogic> line)
                : GroupRoom(groupRoom)
                , Store(std::move(store))
                , PageLoadingLogic(std::move(line))
            {
            }

            TResult DoLoad(TPageId pageId, EPage type, ui64 lower, ui64 upper) noexcept
            {
                if (std::exchange(Grow, false)) {
                    PageLoadingLogic->Forward(this, upper);
                }

                for (auto &seq: IndexFetch) {
                    NPageCollection::TLoadedPage page(seq, *Store->GetPage(IndexRoom, seq));
                    PageLoadingLogic->Fill(page, Store->GetPageType(IndexRoom, seq)); /* will move data */
                }
                for (auto &seq: GroupFetch) {
                    NPageCollection::TLoadedPage page(seq, *Store->GetPage(GroupRoom, seq));
                    PageLoadingLogic->Fill(page, Store->GetPageType(GroupRoom, seq)); /* will move data */
                }

                IndexFetch.clear();
                GroupFetch.clear();

                auto got = PageLoadingLogic->Get(this, pageId, type, lower);

                Y_ABORT_UNLESS((Grow = got.Grow) || IndexFetch || GroupFetch || got.Page);

                return { got.Need, got.Page };
            }

        private:
            ui64 AddToQueue(TPageId pageId, EPage type) noexcept override
            {
                if (IsIndexPage(type)) {
                    IndexFetch.push_back(pageId);
                    return Store->GetPage(IndexRoom, pageId)->size();
                } else {
                    GroupFetch.push_back(pageId);
                    return Store->GetPage(GroupRoom, pageId)->size();
                }
            }

        private:
            const ui32 IndexRoom = 0;
            const ui32 GroupRoom = Max<ui32>();
            TVector<TPageId> IndexFetch;
            TVector<TPageId> GroupFetch;
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

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept override
        {
            return
                MemTable
                    ? MemTable->Locate(memTable, ref, tag)
                    : MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            InitPart(part);

            auto* partStore = CheckedCast<const TPartStore*>(part);

            if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
                Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
            }

            const auto room = (lob == ELargeObj::Extern)
                ? partStore->Store->GetExternRoom()
                : partStore->Store->GetOuterRoom();

            return Get(part, room).DoLoad(ref, EPage::Opaque, AheadLo, AheadHi);
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            InitPart(part);

            auto* partStore = CheckedCast<const TPartStore*>(part);

            Y_ABORT_UNLESS(groupId.Index < partStore->Store->GetGroupCount());

            auto type = partStore->GetPageType(pageId, groupId);
            if (groupId.IsMain() && IsIndexPage(type)) {
                // redirect index page to its actual group queue:
                groupId = PartIndexPageLocator[part].GetGroup(pageId);
            }

            ui32 queueIndex = (groupId.Historic ? partStore->Store->GetRoomCount() : 0) + groupId.Index;
            return Get(part, queueIndex).DoLoad(pageId, type, AheadLo, AheadHi).Page;
        }

    private:
        TPartGroupLoadingQueue& Get(const TPart *part, ui32 queueIndex) noexcept
        {
            auto& partGroupQueues = PartGroupQueues[part];

            Y_ABORT_UNLESS(queueIndex < partGroupQueues.size());
            Y_ABORT_UNLESS(partGroupQueues[queueIndex]);

            return *partGroupQueues[queueIndex];
        }

        void InitPart(const TPart *part)
        {
            auto* partStore = CheckedCast<const TPartStore*>(part);
            auto& partGroupQueues = PartGroupQueues[part];

            if (partGroupQueues.empty()) {
                partGroupQueues.reserve(partStore->Store->GetRoomCount() + part->HistoricGroupsCount);
                for (ui32 room : xrange(partStore->Store->GetRoomCount())) {
                    if (room < partStore->Store->GetGroupCount()) {
                        NPage::TGroupId groupId(room);
                        partGroupQueues.push_back(Settle(partStore, room, NFwd::CreateCache(part, PartIndexPageLocator[part], groupId)));
                    } else if (room == partStore->Store->GetOuterRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, MakeOuter(partStore)));
                    } else if (room == partStore->Store->GetExternRoom()) {
                        partGroupQueues.push_back(Settle(partStore, room, MakeExtern(partStore)));
                    } else {
                        Y_ABORT("Don't know how to work with room %" PRIu32, room);
                    }
                }
                for (ui32 group : xrange(part->HistoricGroupsCount)) {
                    NPage::TGroupId groupId(group, true);
                    partGroupQueues.push_back(Settle(partStore, group, NFwd::CreateCache(part, PartIndexPageLocator[part], groupId)));
                }
            }
        }

        TPartGroupLoadingQueue* Settle(const TPartStore *part, ui16 room, THolder<NFwd::IPageLoadingLogic> line)
        {
            if (line) {
                GroupQueues.emplace_back(part->Store, room, std::move(line));
                return &GroupQueues.back();
            } else {
                return nullptr; /* Will fail on access in Head(...) */
            }
        }

        THolder<NFwd::IPageLoadingLogic> MakeExtern(const TPartStore *part) const noexcept
        {
            if (auto &large = part->Large) {
                Y_ABORT_UNLESS(part->Blobs, "Part has frames but not blobs");

                TVector<ui32> edges(large->Stats().Tags.size(), Edge);

                return MakeHolder<NFwd::TBlobs>(large, TSlices::All(), edges, false);
            } else
                return nullptr;
        }

        THolder<NFwd::IPageLoadingLogic> MakeOuter(const TPart *part) const noexcept
        {
            if (auto &small = part->Small) {
                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                return MakeHolder<NFwd::TBlobs>(small, TSlices::All(), edge, false);
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
