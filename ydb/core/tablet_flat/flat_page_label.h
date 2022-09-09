#pragma once

#include "defs.h"

#include "flat_page_iface.h"
#include "util_basics.h"

#include <library/cpp/actors/util/shared_data.h>

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

        void Init(EPage type, ui16 format, ui64 size) noexcept;
        void SetSize(ui64 size) noexcept;
    };

    static_assert(sizeof(TLabel) == 8, "Invalid page TLabel unit");

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
