#pragma once

#include <util/system/types.h>
#include <util/generic/array_ref.h>

namespace NKikimr {
namespace NPageCollection {

    ui32 Checksum(TArrayRef<const char> body) noexcept;

    struct TBorder {
        struct TOn {
            ui32 Blob;
            ui32 Skip;
        };

        constexpr explicit operator bool() const noexcept
        {
            return Lo.Blob != Max<ui32>();
        }

        constexpr ui32 Blobs() const noexcept
        {
            return 1 + (Up.Blob - Lo.Blob);
        }

        ui64 Bytes; /* Total bounded bytes  */
        TOn Lo;     /* Lower edge point [   */
        TOn Up;     /* Upper edge point )   */
    };

    struct TInfo /* page meta info */ {
        explicit operator bool() const
        {
            return Size > 0;
        }

        ui64 Size;
        ui32 Type;
    };

}
}
