#include "lz.h"

#include <util/system/yassert.h>
#include <util/system/byteorder.h>
#include <util/memory/addstorage.h>
#include <util/generic/buffer.h>
#include <util/generic/utility.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/stream/mem.h>

#include <library/cpp/streams/lz/common/compressor.h>

#include <library/cpp/streams/lz/lz4/block.h>
#include <library/cpp/streams/lz/snappy/block.h>

#include <contrib/libs/fastlz/fastlz.h>

#ifndef OPENSOURCE
#include "minilzo.h"
#include "quicklz.h"
#endif

/*
 * FastLZ
 */
class TFastLZ {
public:
    static const char signature[];

    static inline size_t Hint(size_t len) noexcept {
        return Max<size_t>((size_t)(len * 1.06), 100);
    }

    inline size_t Compress(const char* data, size_t len, char* ptr, size_t /*dstMaxSize*/) {
        return fastlz_compress(data, len, ptr);
    }

    inline size_t Decompress(const char* data, size_t len, char* ptr, size_t max) {
        return fastlz_decompress(data, len, ptr, max);
    }

    inline void InitFromStream(IInputStream*) const noexcept {
    }

    static inline bool SaveIncompressibleChunks() noexcept {
        return false;
    }
};

const char TFastLZ::signature[] = "YLZF";

DEF_COMPRESSOR(TLzfCompress, TFastLZ)
DEF_DECOMPRESSOR(TLzfDecompress, TFastLZ)

template <class T>
static TAutoPtr<IInputStream> TryOpenLzDecompressorX(const TDecompressSignature& s, T input) {
    if (s.Check<TLZ4>())
        return new TLzDecompressInput<T, TLZ4>(input);

    if (s.Check<TSnappy>())
        return new TLzDecompressInput<T, TSnappy>(input);

#ifndef OPENSOURCE
    if (auto result = TryOpenMiniLzoDecompressor(s, input))
        return result;
#endif

    if (s.Check<TFastLZ>())
        return new TLzDecompressInput<T, TFastLZ>(input);

#ifndef OPENSOURCE
    if (auto result = TryOpenQuickLzDecompressor(s, input))
        return result;
#endif

    return nullptr;
}

template <class T>
static inline TAutoPtr<IInputStream> TryOpenLzDecompressorImpl(const TStringBuf& signature, T input) {
    if (signature.size() == SIGNATURE_SIZE) {
        TMemoryInput mem(signature.data(), signature.size());
        TDecompressSignature s(&mem);

        return TryOpenLzDecompressorX(s, input);
    }

    return nullptr;
}

template <class T>
static inline TAutoPtr<IInputStream> TryOpenLzDecompressorImpl(T input) {
    TDecompressSignature s(&*input);

    return TryOpenLzDecompressorX(s, input);
}

template <class T>
static inline TAutoPtr<IInputStream> OpenLzDecompressorImpl(T input) {
    TAutoPtr<IInputStream> ret = TryOpenLzDecompressorImpl(input);

    if (!ret) {
        ythrow TDecompressorError() << "Unknown compression format";
    }

    return ret;
}

TAutoPtr<IInputStream> OpenLzDecompressor(IInputStream* input) {
    return OpenLzDecompressorImpl(input);
}

TAutoPtr<IInputStream> TryOpenLzDecompressor(IInputStream* input) {
    return TryOpenLzDecompressorImpl(input);
}

TAutoPtr<IInputStream> TryOpenLzDecompressor(const TStringBuf& signature, IInputStream* input) {
    return TryOpenLzDecompressorImpl(signature, input);
}

TAutoPtr<IInputStream> OpenOwnedLzDecompressor(TAutoPtr<IInputStream> input) {
    return OpenLzDecompressorImpl(input);
}

TAutoPtr<IInputStream> TryOpenOwnedLzDecompressor(TAutoPtr<IInputStream> input) {
    return TryOpenLzDecompressorImpl(input);
}

TAutoPtr<IInputStream> TryOpenOwnedLzDecompressor(const TStringBuf& signature, TAutoPtr<IInputStream> input) {
    return TryOpenLzDecompressorImpl(signature, input);
}
