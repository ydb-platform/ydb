#pragma once

#include "flat_part_screen.h"
#include "flat_part_slice.h"
#include "util_basics.h"

namespace NKikimr {
namespace NTable {

    struct TOverlay {
        static TOverlay Decode(TArrayRef<const char> opaque, TArrayRef<const char> ext) noexcept;
        TString Encode() const noexcept;
        void Validate() const noexcept;

        void ApplyDelta(TArrayRef<const char> delta) noexcept;

        static TString EncodeRemoveSlices(const TIntrusiveConstPtr<TSlices>& slices) noexcept;
        static TString EncodeChangeSlices(TConstArrayRef<TSlice> slices) noexcept;

        /**
         * Returns a modified opaque with redundant splits in slices stitched back
         *
         * Returns an empty string if no modifications are needed
         */
        static TString MaybeUnsplitSlices(const TString& opaque, size_t maxSize = 1024 * 1024) noexcept;

        TIntrusiveConstPtr<TScreen> Screen;
        TIntrusiveConstPtr<TSlices> Slices;
    };

}
}
