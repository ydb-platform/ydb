#pragma once

#include "flat_page_iface.h"
#include "util_basics.h"

#include <ydb/core/base/shared_data.h>

#include <util/generic/array_ref.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    struct TLabel {
        EPage Type;
        ui16 Format;
        TSize Size;

        void Init(EPage type, ui16 format, ui64 size) noexcept;
        void SetSize(ui64 size) noexcept;
    };

    static_assert(sizeof(TLabel) == 8, "Invalid page TLabel unit");

    struct THello {
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
