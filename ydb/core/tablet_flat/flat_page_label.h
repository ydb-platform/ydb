#pragma once

#include "defs.h"

#include "flat_page_iface.h"
#include "util_basics.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/array_ref.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    // N.B. this struct includes only fields from old style label,
    // new style label (most significant bit is set is Format) contains
    // extra 8 bytes, first one is codec, the rest is reserved.
    // See TLabelWrapper::Read()
    struct TLabel {
        EPage Type;
        ui16 Format; // most significant bit is format indicator, the rest is version
        TSize Size;

        bool IsExtended() const noexcept {
            return Format & 0x8000;
        }

        bool IsHuge() const noexcept {
            return Size == Max<ui32>();
        }

        static TLabel Encode(EPage type, ui16 format, ui64 size) noexcept {
            TSize lsize;

            if (Y_UNLIKELY(size >> 32)) {
                // This size is used as a huge page marker
                lsize = Max<ui32>();
            } else {
                lsize = TSize(size);
            }

            return TLabel{
                .Type = type,
                .Format = format,
                .Size = lsize,
            };
        }
    };

    static_assert(sizeof(TLabel) == 8, "Invalid page TLabel unit");

    struct TLabelExt {
        ECodec Codec;
        char Reserved[7];

        static TLabelExt Encode(ECodec codec) noexcept {
            return TLabelExt{
                .Codec = codec,
                .Reserved = {},
            };
        }
    };

    static_assert(sizeof(TLabelExt) == 8, "Invalid page TLabelExt size");

    struct TLabelWrapper {
        struct TResult {
            bool operator==(NPage::ECodec codec) const noexcept
            {
                return Codec == codec;
            }

            bool operator==(NPage::EPage kind) const noexcept
            {
                return Type == kind;
            }

            TArrayRef<const char> operator*() const noexcept
            {
                return Page;
            }

            EPage Type;
            ui16 Version;
            ECodec Codec;
            TArrayRef<const char> Page;
        };

        TResult Read(TArrayRef<const char>, EPage type = EPage::Undef) const noexcept;
        static TSharedData Wrap(TArrayRef<const char>, EPage, ui16 version) noexcept;
        static TString WrapString(TArrayRef<const char>, EPage, ui16 version) noexcept;
    };

}
}
}
