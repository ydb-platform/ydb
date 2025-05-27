#include "flat_page_label.h"
#include "util_deref.h"

#include <util/system/sanitizers.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    TLabelWrapper::TResult TLabelWrapper::Read(TArrayRef<const char> raw, EPage type) const
    {
        Y_ENSURE(raw.size() >= sizeof(TLabel), "Page blob is too small to hold label");

        auto label = ReadUnaligned<TLabel>(raw.data());

        Y_ENSURE(label.Type == type || type == EPage::Undef,
            "Page blob has an unexpected label type");

        if (Y_UNLIKELY(label.IsHuge())) {
            Y_ENSURE(raw.size() >= Max<ui32>(), "Page label huge page marker doesn't match data size");
        } else {
            Y_ENSURE(label.Size == raw.size(), "Page label size doesn't match data size");
        }

        const ui16 version = label.Format & 0x7fff;

        ECodec codec = ECodec::Plain;

        auto* begin = raw.begin() + sizeof(TLabel);

        if (label.IsExtended()) {
            /* New style NPage label, has at least 8 extra bytes, the
                current impl use only 1st byte. Futher is reserved for
                in-place crc (may be) or other pages common metadata.
             */

            Y_ENSURE(raw.size() >= sizeof(TLabel) + sizeof(TLabelExt), "Page extended label doesn't match data size");

            auto ext = ReadUnaligned<TLabelExt>(begin);
            codec = ext.Codec;
            begin += sizeof(TLabelExt);
        }

        return { label.Type, version, codec, { begin, raw.end() } };
    }

    TSharedData TLabelWrapper::Wrap(TArrayRef<const char> plain, EPage page, ui16 version)
    {
        Y_ENSURE(!(version >> 15), "Version can use only 15 bits");

        TSharedData blob = TSharedData::Uninitialized(plain.size() + 8);

        WriteUnaligned<TLabel>(blob.mutable_begin(), TLabel::Encode(page, version, blob.size()));

        std::copy(plain.begin(), plain.end(), blob.mutable_begin() + 8);

        NSan::CheckMemIsInitialized(blob.data(), blob.size());

        return blob;
    }

    TString TLabelWrapper::WrapString(TArrayRef<const char> plain, EPage page, ui16 version)
    {
        Y_ENSURE(!(version >> 15), "Version can use only 15 bits");

        TString blob = TString::Uninitialized(plain.size() + 8);

        WriteUnaligned<TLabel>(blob.begin(), TLabel::Encode(page, version, blob.size()));

        std::copy(plain.begin(), plain.end(), blob.begin() + 8);

        NSan::CheckMemIsInitialized(blob.data(), blob.size());

        return blob;
    }

}
}
}
