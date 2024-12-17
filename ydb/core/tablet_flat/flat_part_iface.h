#pragma once

#include "flat_page_iface.h"
#include "flat_sausage_solid.h"
#include "flat_table_stats.h"
#include "flat_row_eggs.h"
#include "util_basics.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NTable {

    class TColdPart;
    class TPart;
    class TMemTable;

    using TPageId = NPage::TPageId;
    using EPage = NPage::EPage;

    struct ISaver {
        using TGlobId = NPageCollection::TGlobId;

        virtual void Save(TSharedData raw, NPage::TGroupId groupId) noexcept = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, const TGlobId &glob) noexcept = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, TArrayRef<const char> blob) noexcept = 0;
    };

    class IPageWriter {
    public:
        using EPage = NPage::EPage;
        using TPageId = NPage::TPageId;

        virtual ~IPageWriter() = default;
        virtual TPageId Write(TSharedData page, EPage type, ui32 group) = 0;
        virtual TPageId WriteOuter(TSharedData) noexcept = 0;
        virtual void WriteInplace(TPageId page, TArrayRef<const char> body) = 0;
        virtual NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) noexcept = 0;
        virtual void Finish(TString overlay) noexcept = 0;
    };

    struct IPages {
        using ELargeObj = NTable::ELargeObj;
        using TPart = NTable::TPart;
        using TMemTable = NTable::TMemTable;
        using TPageId = NPage::TPageId;
        using TGroupId = NPage::TGroupId;

        virtual ~IPages() = default;

        struct TResult {
            explicit operator bool() const noexcept
            {
                return bool(Page);
            }

            const TSharedData* operator*() const noexcept
            {
                return Page;
            }

            bool Need;
            const TSharedData *Page;
        };

        virtual TResult Locate(const TMemTable*, ui64 ref, ui32 tag) noexcept = 0;
        virtual TResult Locate(const TPart*, ui64 ref, ELargeObj lob) noexcept = 0;
        virtual const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) = 0;

        /**
         * Hook for cleaning up env on DB.RollbackChanges()
         */
        virtual void OnRollbackChanges() noexcept {
            // nothing by default
        }
    };

}
}
