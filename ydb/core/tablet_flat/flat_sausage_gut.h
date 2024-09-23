#pragma once

#include "flat_sausage_misc.h"
#include "flat_sausage_solid.h"

namespace NKikimr {
namespace NPageCollection {

    class IPageCollection : public TAtomicRefCount<IPageCollection> {
    public:
        virtual ~IPageCollection() = default;

        virtual const TLogoBlobID& Label() const noexcept = 0;
        virtual ui32 Total() const noexcept = 0;
        virtual TInfo Page(ui32 page) const noexcept = 0;
        virtual TBorder Bounds(ui32 page) const noexcept = 0;
        virtual TGlobId Glob(ui32 blob) const noexcept = 0;
        virtual bool Verify(ui32 page, TArrayRef<const char>) const noexcept = 0;
        virtual size_t BackingSize() const noexcept = 0;
    };

}
}
