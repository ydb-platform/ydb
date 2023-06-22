#pragma once

#include "test_store.h"
#include <ydb/core/tablet_flat/flat_table_misc.h>
#include <ydb/core/tablet_flat/flat_table_part.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_part_laid.h>
#include <ydb/core/tablet_flat/flat_part_iface.h>
#include <ydb/core/tablet_flat/flat_fwd_cache.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>

#include <util/generic/cast.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TPartStore : public NTable::TPart {
    protected:
        TPartStore(const TPartStore& src, NTable::TEpoch epoch)
            : TPart(src, epoch)
            , Store(src.Store)
            , Slices(src.Slices)
        { }

    public:
        TPartStore(TIntrusiveConstPtr<TStore> store, TLogoBlobID label, TPart::TParams params, TStat stat,
                    TIntrusiveConstPtr<TSlices> slices)
            : TPart(label, params, stat)
            , Store(std::move(store))
            , Slices(std::move(slices))
        {

        }

        ui64 DataSize() const override
        {
            return Store->PageCollectionBytes(0);
        }

        ui64 BackingSize() const override
        {
            return Store->PageCollectionBytes(0) + Store->PageCollectionBytes(Store->GetOuterRoom());
        }

        ui64 GetPageSize(NPage::TPageId id, NPage::TGroupId groupId) const override
        {
            return Store->GetPage(groupId.Index, id)->size();
        }

        ui8 GetPageChannel(NPage::TPageId id, NPage::TGroupId groupId) const override
        {
            Y_UNUSED(id);
            Y_UNUSED(groupId);
            return 0;
        }

        ui8 GetPageChannel(ELargeObj lob, ui64 ref) const override
        {
            Y_UNUSED(lob);
            Y_UNUSED(ref);
            return 0;
        }

        TIntrusiveConstPtr<NTable::TPart> CloneWithEpoch(NTable::TEpoch epoch) const override
        {
            return new TPartStore(*this, epoch);
        }

        const TIntrusiveConstPtr<TStore> Store;
        const TIntrusiveConstPtr<TSlices> Slices;
    };

    class TTestEnv: public IPages {
    public:
        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept override
        {
            return MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            auto* partStore = CheckedCast<const TPartStore*>(part);

            if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
                Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
            }

            ui32 room = (lob == ELargeObj::Extern)
                ? partStore->Store->GetExternRoom()
                : partStore->Store->GetOuterRoom();

            return { true, Get(part, room, ref) };
        }

        const TSharedData* TryGetPage(const TPart *part, TPageId ref, TGroupId groupId) override
        {
            return Get(part, groupId.Index, ref);
        }

    private:
        const TSharedData* Get(const TPart *part, ui32 room, ui32 ref) const
        {
            Y_VERIFY(ref != Max<ui32>(), "Got invalid page reference");

            return CheckedCast<const TPartStore*>(part)->Store->GetPage(room, ref);
        }
    };

    struct TPartEggs {
        const TIntrusiveConstPtr<TPartStore>& At(size_t num) const noexcept
        {
            return Parts.at(num);
        }

        const TIntrusiveConstPtr<TPartStore>& Lone() const noexcept
        {
            Y_VERIFY(Parts.size() == 1, "Need egg with one part inside");

            return Parts[0];
        }

        bool NoResult() const noexcept
        {
            return Written == nullptr;  /* compaction was aborted */
        }

        TPartView ToPartView() const noexcept
        {
            return { Lone(), nullptr, Lone()->Slices };
        }

        TAutoPtr<TWriteStats> Written;
        TIntrusiveConstPtr<TRowScheme> Scheme;
        TVector<TIntrusiveConstPtr<TPartStore>> Parts;
    };

    TString DumpPart(const TPartStore&, ui32 depth = 10) noexcept;

}}}
