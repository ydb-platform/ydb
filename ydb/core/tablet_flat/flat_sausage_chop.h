#pragma once

#include "flat_sausage_solid.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NPageCollection {

    struct TGroupBlobsByCookie {
        using TArray = TArrayRef<const TLogoBlobID>;

        TGroupBlobsByCookie(TArray array) : Array(array) { }

        explicit operator bool() const noexcept
        {
            return Head < Array.size();
        }

        TArray Do() noexcept
        {
            auto left = Array.size() - Min(Head, Array.size());

            for (size_t offset = 0; offset < left; Head++, offset++)
                if (!Near(Array[Head - offset], Array[Head], offset))
                    break;

            return { Array.end() - left, Array.begin() + Head };
        }

        static bool Near(const TLogoBlobID &left, const TLogoBlobID &right, ui32 way)
        {
            return
                TGroupBlobsByCookie::IsInPlane(left, right)
                && left.Cookie() + way == right.Cookie()
                && left.Cookie() != TLogoBlobID::MaxCookie - way;
        }

        static TLargeGlobId ToLargeGlobId(TArray logo, ui32 group = TLargeGlobId::InvalidGroup) noexcept
        {
            if (logo) {
                for (auto num : xrange(logo.size())) {
                    auto &base = logo[logo.size() - 1 == num ? num : 0];

                    Y_ABORT_UNLESS(logo[num].BlobSize() == base.BlobSize());
                }

                const ui32 bulk = (logo.size() - 1) * logo[0].BlobSize();

                return { group, logo[0], bulk + logo.back().BlobSize() };
            } else {
                return { }; /* invalid TLargeGlobId object */
            }
        }

        static bool IsInPlane(const TLogoBlobID &one, const TLogoBlobID &two)
        {
            return
                one.TabletID() == two.TabletID()
                && one.Generation() == two.Generation()
                && one.Step() == two.Step();
        }

    private:
        TArray Array;
        size_t Head = 0;
    };
}
}
