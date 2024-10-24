#include "codec.h"
#include "bzip2.h"
#include "lz.h"
#include "lzma.h"
#include "snappy.h"
#include "zlib.h"
#include "zstd.h"
#include "brotli.h"

#include <util/generic/algorithm.h>

namespace NYT::NCompression {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

template <class TCodec>
struct TCompressedBlockTag { };

template <class TCodec>
struct TDecompressedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

template <class TCodec>
class TCodecBase
    : public ICodec
{
public:
    TSharedRef Compress(const TSharedRef& block) override
    {
        return Run(&TCodec::DoCompress, GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>(), block);
    }

    TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return Run(&TCodec::DoCompress, GetRefCountedTypeCookie<TCompressedBlockTag<TCodec>>(), blocks);
    }

    TSharedRef Decompress(const TSharedRef& block) override
    {
        return Run(&TCodec::DoDecompress, GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>(), block);
    }

    TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return Run(&TCodec::DoDecompress, GetRefCountedTypeCookie<TDecompressedBlockTag<TCodec>>(), blocks);
    }

private:
    TSharedRef Run(
        void (TCodec::*converter)(TSource* source, TBlob* output),
        TRefCountedTypeCookie blobCookie,
        const TSharedRef& ref)
    {
        TRefSource input(ref);
        auto outputBlob = TBlob(blobCookie, 0, false);
        (static_cast<TCodec*>(this)->*converter)(&input, &outputBlob);
        return FinalizeBlob(&outputBlob, blobCookie);
    }

    TSharedRef Run(
        void (TCodec::*converter)(TSource* source, TBlob* output),
        TRefCountedTypeCookie blobCookie,
        const std::vector<TSharedRef>& refs)
    {
        if (refs.size() == 1) {
            return Run(converter, blobCookie, refs.front());
        }

        TRefsVectorSource input(refs);
        auto outputBlob = TBlob(blobCookie, 0, false);
        (static_cast<TCodec*>(this)->*converter)(&input, &outputBlob);
        return FinalizeBlob(&outputBlob, blobCookie);
    }

    static TSharedRef FinalizeBlob(TBlob* blob, TRefCountedTypeCookie blobCookie)
    {
        // For blobs smaller than 16K, do nothing.
        // For others, allow up to 5% capacity overhead.
        if (blob->Capacity() >= 16_KB &&
            blob->Capacity() >= 1.05 * blob->Size())
        {
            *blob = TBlob(blobCookie, blob->ToRef());
        }
        return TSharedRef::FromBlob(std::move(*blob));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNoneCodec
    : public ICodec
{
public:
    TSharedRef Compress(const TSharedRef& block) override
    {
        return block;
    }

    TSharedRef Compress(const std::vector<TSharedRef>& blocks) override
    {
        return MergeRefsToRef<TCompressedBlockTag<TNoneCodec>>(blocks);
    }

    TSharedRef Decompress(const TSharedRef& block) override
    {
        return block;
    }

    TSharedRef Decompress(const std::vector<TSharedRef>& blocks) override
    {
        return MergeRefsToRef<TDecompressedBlockTag<TNoneCodec>>(blocks);
    }

    ECodec GetId() const override
    {
        return ECodec::None;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnappyCodec
    : public TCodecBase<TSnappyCodec>
{
public:
    void DoCompress(TSource* source, TBlob* output)
    {
        SnappyCompress(source, output);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        SnappyDecompress(source, output);
    }

    ECodec GetId() const override
    {
        return ECodec::Snappy;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZlibCodec
    : public TCodecBase<TZlibCodec>
{
public:
    explicit TZlibCodec(int level)
        : Level_(level)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        ZlibCompress(Level_, source, output);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        ZlibDecompress(source, output);
    }

    ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Zlib_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                YT_ABORT();
        }
    }

private:
    const int Level_;
};

////////////////////////////////////////////////////////////////////////////////

class TLz4Codec
    : public TCodecBase<TLz4Codec>
{
public:
    explicit TLz4Codec(bool highCompression)
        : HighCompression_(highCompression)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        Lz4Compress(source, output, HighCompression_);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        Lz4Decompress(source, output);
    }

    ECodec GetId() const override
    {
        return HighCompression_ ? ECodec::Lz4HighCompression : ECodec::Lz4;
    }

private:
    const bool HighCompression_;
};

////////////////////////////////////////////////////////////////////////////////

class TZstdCodec
    : public TCodecBase<TZstdCodec>
{
public:
    explicit TZstdCodec(int level)
        : Level_(level)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        ZstdCompress(Level_, source, output);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        ZstdDecompress(source, output);
    }

    ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Zstd_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)(13)(14)(15)(16)(17)(18)(19)(20)(21))
#undef CASE

            default:
                YT_ABORT();
        }
    }

private:
    const int Level_;
};

////////////////////////////////////////////////////////////////////////////////

class TBrotliCodec
    : public TCodecBase<TBrotliCodec>
{
public:
    explicit TBrotliCodec(int level)
        : Level_(level)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        BrotliCompress(Level_, source, output);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        BrotliDecompress(source, output);
    }

    ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Brotli_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11))
#undef CASE

            default:
                YT_ABORT();
        }
    }

private:
    const int Level_;
};

class TLzmaCodec
    : public TCodecBase<TLzmaCodec>
{
public:
    explicit TLzmaCodec(int level)
        : Level_(level)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        LzmaCompress(Level_, source, output);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        LzmaDecompress(source, output);
    }

    ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Lzma_, level);
            PP_FOR_EACH(CASE, (0)(1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                YT_ABORT();
        }
    }

private:
    const int Level_;
};

class TBzip2Codec
    : public TCodecBase<TBzip2Codec>
{
public:
    explicit TBzip2Codec(int level)
        : Level_(level)
    { }

    void DoCompress(TSource* source, TBlob* output)
    {
        Bzip2Compress(source, output, Level_);
    }

    void DoDecompress(TSource* source, TBlob* output)
    {
        Bzip2Decompress(source, output);
    }

    ECodec GetId() const override
    {
        switch (Level_) {

#define CASE(level) case level: return PP_CONCAT(ECodec::Bzip2_, level);
            PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CASE

            default:
                YT_ABORT();
        }
    }

private:
    const int Level_;
};

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    switch (id) {
        case ECodec::None: {
            static TNoneCodec result;
            return &result;
        }

        case ECodec::Snappy: {
            static TSnappyCodec result;
            return &result;
        }

        case ECodec::Lz4: {
            static TLz4Codec result(false);
            return &result;
        }

        case ECodec::Lz4HighCompression: {
            static TLz4Codec result(true);
            return &result;
        }

#define CASE(param)                                                 \
    case ECodec::PP_CONCAT(CODEC, PP_CONCAT(_, param)): {           \
        static PP_CONCAT(T, PP_CONCAT(CODEC, Codec)) result(param); \
        return &result;                                             \
    }

#define CODEC Zlib
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Brotli
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11))
#undef CODEC

#define CODEC Lzma
        PP_FOR_EACH(CASE, (0)(1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Bzip2
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9))
#undef CODEC

#define CODEC Zstd
        PP_FOR_EACH(CASE, (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)(13)(14)(15)(16)(17)(18)(19)(20)(21))
#undef CODEC

#undef CASE

        default:
            THROW_ERROR_EXCEPTION("Unsupported compression codec %Qlv",
                id);
    }
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<ECodec>& GetForbiddenCodecs()
{
    static const THashSet<ECodec> deprecatedCodecs{
        ECodec::QuickLz
    };
    return deprecatedCodecs;
}

const THashMap<TString, TString>& GetForbiddenCodecNameToAlias()
{
    static const THashMap<TString, TString> deprecatedCodecNameToAlias = {
        {"zlib6", FormatEnum(ECodec::Zlib_6)},
        {"gzip_normal", FormatEnum(ECodec::Zlib_6)},
        {"zlib9", FormatEnum(ECodec::Zlib_9)},
        {"gzip_best_compression", FormatEnum(ECodec::Zlib_9)},
        {"zstd", FormatEnum(ECodec::Zstd_3)},
        {"brotli3", FormatEnum(ECodec::Brotli_3)},
        {"brotli5", FormatEnum(ECodec::Brotli_5)},
        {"brotli8", FormatEnum(ECodec::Brotli_8)}
    };
    return deprecatedCodecNameToAlias;
}

const std::vector<ECodec>& GetSupportedCodecs()
{
    static const std::vector<ECodec> supportedCodecs = [] {
        std::vector<ECodec> supportedCodecs;
        for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
            if (!GetForbiddenCodecs().contains(codecId)) {
                supportedCodecs.push_back(codecId);
            }
        }
        SortUnique(supportedCodecs);
        return supportedCodecs;
    }();
    return supportedCodecs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
