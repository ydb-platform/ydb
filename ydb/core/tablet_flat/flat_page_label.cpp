#include "flat_page_label.h"
#include "util_deref.h"

#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    void TLabel::Init(EPage type, ui16 format, ui64 size) noexcept {
        Type = type;
        Format = format;
        SetSize(size);
    }

    void TLabel::SetSize(ui64 size) noexcept {
        // We use Max<ui32>() as a huge (>=4GB) page marker
        Size = Y_LIKELY(size < Max<ui32>()) ? ui32(size) : Max<ui32>();
    }

    TLabelWrapper::TResult TLabelWrapper::Read(TArrayRef<const char> raw, EPage type) const noexcept
    {
        auto label = TDeref<TLabel>::Copy(raw.begin(), 0);

        if (raw.size() < 8) {
            Y_FAIL("NPage blob is too small to hold label");
        } else if (label.Type != type && type != EPage::Undef) {
            Y_FAIL("NPage blob has an unexpected label type");
        } else if (label.Size != raw.size() && label.Size != Max<ui32>()) {
            Y_FAIL("NPage label size doesn't match data size");
        } else if (label.Size == Max<ui32>()) {
            Y_VERIFY(raw.size() >= Max<ui32>(), "NPage label huge page marker doesn't match data size");
        }

        const ui16 version = label.Format & 0x7fff;

        if (label.Format & 0x8000) {
            /* New style NPage label, has at least extra 8 bytes, the
                current impl use only 1st byte. Futher is reserved for
                in-place crc (may be) or other pages common metadata.
             */

            // this ugly construct is just to get ui8 at raw.begin() + sizeof(TLabel)
            auto codec = ECodec(*TDeref<ui8>::At(TDeref<TLabel>::At(raw.begin(), 0) + 1, 0));

            auto *on = raw.begin() + sizeof(TLabel) + 8;

            return { label.Type, version, codec, { on, raw.end() } };

        } else {
            /* old style NPage label, has no any extra attached data */

            auto *on = raw.begin() + sizeof(TLabel);

            return
                { label.Type, version, ECodec::Plain, { on, raw.end() } };
        }
    }

    TSharedData TLabelWrapper::Wrap(TArrayRef<const char> plain, EPage page, ui16 version) noexcept
    {
        Y_VERIFY(!(version >> 15), "Version can use only 15 bits");

        TSharedData blob = TSharedData::Uninitialized(plain.size() + 8);

        TDeref<TLabel>::At(blob.mutable_begin(), 0)->Init(page, version, blob.size());

        std::copy(plain.begin(), plain.end(), blob.mutable_begin() + 8);

        NSan::CheckMemIsInitialized(blob.data(), blob.size());

        return blob;
    }

    TString TLabelWrapper::WrapString(TArrayRef<const char> plain, EPage page, ui16 version) noexcept
    {
        Y_VERIFY(!(version >> 15), "Version can use only 15 bits");

        TString blob = TString::Uninitialized(plain.size() + 8);

        TDeref<TLabel>::At(blob.begin(), 0)->Init(page, version, blob.size());

        std::copy(plain.begin(), plain.end(), blob.begin() + 8);

        NSan::CheckMemIsInitialized(blob.data(), blob.size());

        return blob;
    }

}
}
}
