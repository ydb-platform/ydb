#include "factory.h"

#include <library/cpp/streams/bzip2/bzip2.h>
#include <library/cpp/streams/factory/open_common/factory.h>
#include <util/stream/holder.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/system/file.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/store_policy.h>

namespace {
    template <class T, class TDecoder>
    class TCompressed: public TEmbedPolicy<T>, public TDecoder {
    public:
        template <class C>
        inline TCompressed(const C& c)
            : TEmbedPolicy<T>(c)
            , TDecoder(TEmbedPolicy<T>::Ptr())
        {
        }

        template <class C>
        inline TCompressed(const C& c, size_t compressionLevel, size_t buflen)
            : TEmbedPolicy<T>(c)
            , TDecoder(this->Ptr(), compressionLevel, buflen)
        {
        }

        ~TCompressed() override {
        }
    };

    class TGZipCompress: public TZLibCompress {
    public:
        TGZipCompress(IOutputStream* output)
            : TZLibCompress(output, ZLib::GZip)
        {
        }

        TGZipCompress(IOutputStream* output, size_t compressionLevel, size_t buflen)
            : TZLibCompress(output, ZLib::GZip, compressionLevel, buflen)
        {
        }
    };
}

THolder<IInputStream> OpenInput(const TString& url) {
    if (!url || url == TStringBuf("-")) {
        return OpenStdin();
    }

    if (url.EndsWith(TStringBuf(".gz"))) {
        return MakeHolder<TCompressed<TFileInput, TBufferedZLibDecompress>>(url);
    }

    if (url.EndsWith(TStringBuf(".bz2"))) {
        return MakeHolder<TCompressed<TFileInput, TBZipDecompress>>(url);
    }

    return MakeHolder<TFileInput>(url);
}

THolder<IOutputStream> OpenOutput(const TString& url, ECompression compressionLevel, size_t buflen) {
    if (!url || url == TStringBuf("-")) {
        return MakeHolder<TFileOutput>(Duplicate(1));
    } else if (url.EndsWith(TStringBuf(".gz"))) {
        return MakeHolder<TCompressed<TFileOutput, TGZipCompress>>(url, size_t(compressionLevel), buflen);
    } else if (url.EndsWith(TStringBuf(".bz2"))) {
        return MakeHolder<TCompressed<TFileOutput, TBZipCompress>>(url, size_t(compressionLevel), buflen);
    }

    return MakeHolder<TFileOutput>(url);
}
