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
            return Store->GetPageSize(groupId.Index, id);
        }

        NPage::EPage GetPageType(NPage::TPageId id, NPage::TGroupId groupId) const override
        {
            return Store->GetPageType(groupId.Index, id);
        }

        ui8 GetGroupChannel(NPage::TGroupId groupId) const override
        {
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
            Y_ABORT_UNLESS(ref != Max<ui32>(), "Got invalid page reference");

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
            Y_ABORT_UNLESS(Parts.size() == 1, "Need egg with one part inside");

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

    namespace IndexTools {
        using TGroupId = NPage::TGroupId;

        inline size_t CountMainPages(const TPart& part) {
            size_t result = 0;

            TTestEnv env;
            TPartIndexIt index(&part, &env, { });
            for (size_t i = 0; ; i++) {
                auto ready = i == 0 ? index.Seek(0) : index.Next();
                if (ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready != EReady::Page, "Unexpected page fault");
                    break;
                }
                result++;
            }

            return result;
        }

        inline TRowId GetEndRowId(const TPart& part) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, { });
            return index.GetEndRowId();
        }

        inline const TPartIndexIt::TRecord * GetLastRecord(const TPart& part) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, { });
            Y_ABORT_UNLESS(index.SeekLast() == EReady::Data);
            return index.GetLastRecord();
        }

        inline TRowId GetRowId(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, { });

            Y_ABORT_UNLESS(index.Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index.Next() == EReady::Data);
            }

            return index.GetRowId();
        }

        inline const TPartIndexIt::TRecord * GetRecord(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, { });

            Y_ABORT_UNLESS(index.Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index.Next() == EReady::Data);
            }

            return index.GetRecord();
        }

        inline TPageId GetFirstPageId(const TPart& part, TGroupId groupId) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, groupId);
            index.Seek(0);
            return index.GetPageId();
        }

        inline TPageId GetLastPageId(const TPart& part, TGroupId groupId) {
            TTestEnv env;
            TPartIndexIt index(&part, &env, groupId);
            index.Seek(index.GetEndRowId() - 1);
            return index.GetPageId();
        }

        inline TSlice MakeSlice(const TPartStore& part, ui32 pageIndex1Inclusive, ui32 pageIndex2Exclusive) {
            auto mainPagesCount = CountMainPages(part);
            Y_ABORT_UNLESS(pageIndex1Inclusive < pageIndex2Exclusive);
            Y_ABORT_UNLESS(pageIndex2Exclusive <= mainPagesCount);
            auto getKey = [&] (const NPage::TIndex::TRecord* record) {
                TSmallVec<TCell> key;
                for (const auto& info : part.Scheme->Groups[0].ColsKeyIdx) {
                    key.push_back(record->Cell(info));
                }
                return TSerializedCellVec(key);
            };
            TSlice slice;
            slice.FirstInclusive = true;
            slice.FirstRowId = IndexTools::GetRowId(part, pageIndex1Inclusive);
            slice.FirstKey = pageIndex1Inclusive > 0 
                ? getKey(IndexTools::GetRecord(part, pageIndex1Inclusive)) 
                : part.Slices->begin()->FirstKey;
            slice.LastInclusive = false;
            slice.LastRowId = pageIndex2Exclusive < mainPagesCount 
                ? IndexTools::GetRowId(part, pageIndex2Exclusive)
                : part.Stat.Rows;
            slice.LastKey = pageIndex2Exclusive < mainPagesCount 
                ? getKey(IndexTools::GetRecord(part, pageIndex2Exclusive)) 
                : part.Slices->rbegin()->LastKey;
            return slice;
        }
    }

}}}
