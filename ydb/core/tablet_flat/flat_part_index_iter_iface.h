#pragma once

#include "flat_page_base.h"
#include "flat_part_iface.h"

namespace NKikimr::NTable {
    
    struct IPartGroupIndexIter {
        using TCells = NPage::TCells;

        virtual EReady Seek(TRowId rowId) = 0;
        virtual EReady SeekLast() = 0;
        virtual EReady Seek(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) = 0;
        virtual EReady SeekReverse(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) = 0;
        virtual EReady Next() = 0;
        virtual EReady Prev() = 0;

        virtual bool IsValid() const = 0;

        virtual TRowId GetEndRowId() const = 0;
        virtual TPageId GetPageId() const = 0;
        virtual TRowId GetRowId() const = 0;
        virtual TRowId GetNextRowId() const = 0;

        virtual TPos GetKeyCellsCount() const = 0;
        virtual TCell GetKeyCell(TPos index) const = 0;
        virtual void GetKeyCells(TSmallVec<TCell>& keyCells) const = 0;

        virtual ~IPartGroupIndexIter() = default;
    };

    THolder<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId);
    
}
