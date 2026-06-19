#pragma once

#include "flat_sausage_misc.h"
#include "flat_page_iface.h"

namespace NKikimr {
namespace NPageCollection {

    class IDataPageCollection: public TAtomicRefCount<IDataPageCollection> {
    public:
        virtual ~IDataPageCollection() = default;

        /// Maps a page location to the blob storage range containing it
        virtual TBorder Bounds(NTable::NPage::TPageLocation location) const = 0;

        /// Verifies that data matches the expected size and checksum
        virtual bool Verify(NTable::NPage::TPageLocation location, TArrayRef<const char> data) const = 0;
    };

}   // namespace NPageCollection
}   // namespace NKikimr
