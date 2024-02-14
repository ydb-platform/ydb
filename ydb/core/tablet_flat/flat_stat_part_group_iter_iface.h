#pragma once

#include "flat_part_iface.h"
#include "ydb/core/scheme/scheme_tablecell.h"

namespace NKikimr::NTable {
    
    struct IStatsPartGroupIterator {
        virtual EReady Start() = 0;
        virtual EReady Next() = 0;

        virtual bool IsValid() const = 0;

        virtual TRowId GetEndRowId() const = 0;
        virtual TPageId GetPageId() const = 0;
        virtual TRowId GetRowId() const = 0;

        virtual TPos HasKeyCells() const = 0;
        virtual TCell GetKeyCell(TPos index) const = 0;

        virtual ~IStatsPartGroupIterator() = default;
    };

    THolder<IStatsPartGroupIterator> CreateStatsPartGroupIterator(const TPart* part, IPages* env, NPage::TGroupId groupId);
    
}
