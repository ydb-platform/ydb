#pragma once

#include "test_store.h"
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_fwd_cache.h>
#include <ydb/core/tablet_flat/flat_part_iface.h>
#include <ydb/core/tablet_flat/flat_part_index_iter_flat_index.h>
#include <ydb/core/tablet_flat/flat_part_laid.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_table_misc.h>
#include <ydb/core/tablet_flat/flat_table_part.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
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

        ui64 GetPageSize(NPage::TPageId pageId, NPage::TGroupId groupId) const override
        {
            return Store->GetPageSize(groupId.Index, pageId);
        }

        ui64 GetPageSize(ELargeObj lob, ui64 ref) const override
        {
            Y_UNUSED(lob);
            Y_UNUSED(ref);
            return 0;
        }

        NPage::EPage GetPageType(NPage::TPageId pageId, NPage::TGroupId groupId) const override
        {
            return Store->GetPageType(groupId.Index, pageId);
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

        const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
        {
            return Get(part, groupId.Index, pageId);
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

        inline const TPartGroupFlatIndexIter::TRecord * GetFlatRecord(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            TPartGroupFlatIndexIter index(&part, &env, { });

            Y_ABORT_UNLESS(index.Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index.Next() == EReady::Data);
            }

            return index.GetRecord();
        }

        inline const TPartGroupFlatIndexIter::TRecord * GetFlatLastRecord(const TPart& part) {
            TTestEnv env;
            TPartGroupFlatIndexIter index(&part, &env, { });
            Y_ABORT_UNLESS(index.SeekLast() == EReady::Data);
            return index.GetLastRecord();
        }

        inline size_t CountMainPages(const TPart& part) {
            size_t result = 0;

            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, { });
            for (size_t i = 0; ; i++) {
                auto ready = i == 0 ? index->Seek(0) : index->Next();
                if (ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready != EReady::Page, "Unexpected page fault");
                    break;
                }
                result++;
            }

            return result;
        }

        inline ui64 CountDataSize(const TPart& part, TGroupId groupId) {
            size_t result = 0;

            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, groupId);
            for (size_t i = 0; ; i++) {
                auto ready = i == 0 ? index->Seek(0) : index->Next();
                if (ready != EReady::Data) {
                    Y_ABORT_UNLESS(ready != EReady::Page, "Unexpected page fault");
                    break;
                }
                result += part.GetPageSize(index->GetPageId(), groupId);
            }

            return result;
        }

        inline TRowId GetEndRowId(const TPart& part) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, { });
            return index->GetEndRowId();
        }

        inline TRowId GetPageId(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, { });

            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index->Next() == EReady::Data);
            }

            return index->GetPageId();
        }

        inline TRowId GetRowId(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, { });

            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index->Next() == EReady::Data);
            }

            return index->GetRowId();
        }

        inline TPageId GetFirstPageId(const TPart& part, TGroupId groupId) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, groupId);
            index->Seek(0);
            return index->GetPageId();
        }

        inline TPageId GetLastPageId(const TPart& part, TGroupId groupId) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, groupId);
            index->Seek(index->GetEndRowId() - 1);
            return index->GetPageId();
        }

        inline TVector<TCell> GetKey(const TPart& part, ui32 pageIndex) {
            TTestEnv env;
            auto index = CreateIndexIter(&part, &env, { });

            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            for (TPageId p = 0; p < pageIndex; p++) {
                Y_ABORT_UNLESS(index->Next() == EReady::Data);
            }

            TVector<TCell> key;
            for (auto i : xrange(index->GetKeyCellsCount())) {
                key.push_back(index->GetKeyCell(i));
            }

            return key;
        }

        inline TSlice MakeSlice(const TPartStore& part, ui32 pageIndex1Inclusive, ui32 pageIndex2Exclusive) {
            auto mainPagesCount = CountMainPages(part);
            Y_ABORT_UNLESS(pageIndex1Inclusive < pageIndex2Exclusive);
            Y_ABORT_UNLESS(pageIndex2Exclusive <= mainPagesCount);
            
            TSlice slice;
            slice.FirstInclusive = pageIndex1Inclusive > 0
                ? true
                : part.Slices->begin()->FirstInclusive;
            slice.FirstRowId = pageIndex1Inclusive > 0
                ? IndexTools::GetRowId(part, pageIndex1Inclusive)
                : part.Slices->begin()->FirstRowId;
            slice.FirstKey = pageIndex1Inclusive > 0 
                ? TSerializedCellVec(GetKey(part, pageIndex1Inclusive))
                : part.Slices->begin()->FirstKey;
            slice.LastInclusive = pageIndex2Exclusive < mainPagesCount
                ? false
                : part.Slices->rbegin()->LastInclusive;
            slice.LastRowId = pageIndex2Exclusive < mainPagesCount 
                ? IndexTools::GetRowId(part, pageIndex2Exclusive)
                : part.Slices->rbegin()->LastRowId;
            slice.LastKey = pageIndex2Exclusive < mainPagesCount 
                ? TSerializedCellVec(GetKey(part, pageIndex2Exclusive))
                : part.Slices->rbegin()->LastKey;
            return slice;
        }
    }

}}}
