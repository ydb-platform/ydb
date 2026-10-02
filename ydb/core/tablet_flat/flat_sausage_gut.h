#pragma once

#include "flat_sausage_misc.h"
#include "flat_sausage_solid.h"

#include <util/generic/vector.h>

namespace NKikimr {
namespace NPageCollection {

    class IPageCollection : public TAtomicRefCount<IPageCollection> {
    public:
        virtual ~IPageCollection() = default;

        virtual const TLogoBlobID& Label() const noexcept = 0;
        virtual ui32 Total() const noexcept = 0;
        /* Number of structural pages addressable by TPageId */
        virtual ui32 MetaPages() const noexcept { return Total(); }
        virtual TInfo Page(ui32 page) const = 0;
        virtual TBorder Bounds(ui32 page) const = 0;
        /// Maps a page location to the blob storage range containing it
        virtual TBorder Bounds(const TPageLocation&) const = 0;
        virtual TGlobId Glob(ui32 blob) const = 0;
        virtual bool Verify(ui32 page, TArrayRef<const char>) const = 0;
        /// Verifies page data using the location's size and checksum
        virtual bool Verify(const TPageLocation&, TArrayRef<const char>) const = 0;
        virtual size_t BackingSize() const noexcept = 0;
        virtual NTable::NPage::TPageLocation GetLocation(ui32 pageId) const = 0;
        /// Returns true when the collection carries both BTreeIndex and BTreeIndexV2 pages
        virtual bool SkipBTreeIndexV1Shadow() const noexcept { return false; }
        virtual void SetSkipBTreeIndexV1Shadow(bool) const noexcept { }
    };

    /// A page that must not be loaded: an excluded page, or a dead V1 shadow index.
    inline bool IsDeadPage(NTable::NPage::EPage type, bool skipBTreeIndexV1Shadow) noexcept
    {
        return type == NTable::NPage::EPage::Skip
            || (skipBTreeIndexV1Shadow && type == NTable::NPage::EPage::BTreeIndex);
    }

    inline bool IsDeadPage(const IPageCollection& collection, TPageId pageId)
    {
        return IsDeadPage(static_cast<NTable::NPage::EPage>(collection.Page(pageId).Type),
            collection.SkipBTreeIndexV1Shadow());
    }

}
}
