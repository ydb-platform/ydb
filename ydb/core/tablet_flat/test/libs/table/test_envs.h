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

        const TSharedData* TryGetPage(const TPart *part, TPageId ref, TGroupId groupId) override
        {
            return Pages ? TTestEnv::TryGetPage(part, ref, groupId) : nullptr;
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

        const TSharedData* TryGetPage(const TPart* part, TPageId ref, TGroupId groupId) override
        {
            auto pass = ShouldPass((const void*)part, ref | (ui64(groupId.Raw()) << 32), part->IndexPages.Has(groupId, ref));

            return pass ? TTestEnv::TryGetPage(part, ref, groupId) : nullptr;
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
        using TSlot = ui32;
        using TSlotVec = TSmallVec<TSlot>;

        struct TPageLoadingQueue : private NFwd::IPageLoadingQueue {
            TPageLoadingQueue(TIntrusiveConstPtr<TStore> store, ui32 room, TAutoPtr<NFwd::IPageLoadingLogic> line)
                : Room(room)
                , Store(std::move(store))
                , PageLoadingLogic(line)
            {

            }

            TResult DoLoad(ui32 page, ui64 lower, ui64 upper) noexcept
            {
                if (std::exchange(Grow, false))
                    PageLoadingLogic->Forward(this, upper);

                for (auto &seq: Fetch) {
                    NPageCollection::TLoadedPage page(seq, *Store->GetPage(Room, seq));

                    PageLoadingLogic->Apply({ &page, 1 }); /* will move data */
                }

                Fetch.clear();

                auto got = PageLoadingLogic->Handle(this, page, lower);

                Y_VERIFY((Grow = got.Grow) || Fetch || got.Page);

                return { got.Need, got.Page };
            }

        private:
            ui64 AddToQueue(TPageId pageId, EPage) noexcept override
            {
                Fetch.push_back(pageId);

                return Store->GetPage(Room, pageId)->size();
            }

        private:
            const ui32 Room = Max<ui32>();
            TVector<TPageId> Fetch;
            TIntrusiveConstPtr<TStore> Store;
            TAutoPtr<NFwd::IPageLoadingLogic> PageLoadingLogic;
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
            auto* partStore = CheckedCast<const TPartStore*>(part);

            if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
                Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
            }

            const auto room = (lob == ELargeObj::Extern)
                ? partStore->Store->GetExternRoom()
                : partStore->Store->GetOuterRoom();

            return Get(part, room).DoLoad(ref, AheadLo, AheadHi);
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            auto* partStore = CheckedCast<const TPartStore*>(part);

            Y_VERIFY(groupId.Index < partStore->Store->GetGroupCount());

            if (part->IndexPages.Has(groupId, pageId)) {
                return partStore->Store->GetPage(groupId.Index, pageId);
            } else {
                ui32 room = (groupId.Historic ? partStore->Store->GetRoomCount() : 0) + groupId.Index;
                return Get(part, room).DoLoad(pageId, AheadLo, AheadHi).Page;
            }
        }

    private:
        TPageLoadingQueue& Get(const TPart *part, ui32 room) noexcept
        {
            auto* partStore = CheckedCast<const TPartStore*>(part);

            auto& slots = Parts[part];
            if (slots.empty()) {
                slots.reserve(partStore->Store->GetRoomCount() + part->HistoricIndexes.size());
                for (ui32 room : xrange(partStore->Store->GetRoomCount())) {
                    if (room < partStore->Store->GetGroupCount()) {
                        NPage::TGroupId groupId(room);
                        slots.push_back(Settle(partStore, room, new NFwd::TCache(partStore->GetGroupIndex(groupId))));
                    } else if (room == partStore->Store->GetOuterRoom()) {
                        slots.push_back(Settle(partStore, room, MakeOuter(partStore)));
                    } else if (room == partStore->Store->GetExternRoom()) {
                        slots.push_back(Settle(partStore, room, MakeExtern(partStore)));
                    } else {
                        Y_FAIL("Don't know how to work with room %" PRIu32, room);
                    }
                }
                for (ui32 group : xrange(part->HistoricIndexes.size())) {
                    NPage::TGroupId groupId(group, true);
                    slots.push_back(Settle(partStore, group, new NFwd::TCache(partStore->GetGroupIndex(groupId))));
                }
            }

            Y_VERIFY(room < slots.size());

            return Queues.at(slots[room]);
        }

        TSlot Settle(const TPartStore *part, ui16 room, NFwd::IPageLoadingLogic *line)
        {
            if (line) {
                Queues.emplace_back(part->Store, room, line);

                return Queues.size() - 1;
            } else {
                return Max<TSlot>(); /* Will fail on access in Head(...) */
            }
        }

        NFwd::IPageLoadingLogic* MakeExtern(const TPartStore *part) const noexcept
        {
            if (auto &large = part->Large) {
                Y_VERIFY(part->Blobs, "Part has frames but not blobs");

                TVector<ui32> edges(large->Stats().Tags.size(), Edge);

                return new NFwd::TBlobs(large, TSlices::All(), edges, false);
            } else
                return nullptr;
        }

        NFwd::IPageLoadingLogic* MakeOuter(const TPart *part) const noexcept
        {
            if (auto &small = part->Small) {
                TVector<ui32> edge(small->Stats().Tags.size(), Max<ui32>());

                return new NFwd::TBlobs(small, TSlices::All(), edge, false);
            } else
                return nullptr;
        }

    private:
        const ui64 AheadLo = 0;
        const ui64 AheadHi = 0;
        const ui32 Edge = Max<ui32>();
        TDeque<TPageLoadingQueue> Queues;
        TMap<const TPart*, TSlotVec> Parts;
        TAutoPtr<NFwd::TMemTableHandler> MemTable;   /* Wrapper for memable blobs    */
    };

}
}
}
