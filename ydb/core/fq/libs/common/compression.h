#pragma once

#include <util/generic/string.h>

namespace NBlockCodecs {
    struct ICodec;
}

namespace NFq {

class TCompressor {
public:
    explicit TCompressor(const TString& compressionMethod, ui64 minCompressionSize = 0);
    bool IsEnabled() const;
    // return (compressionMethod, possiblyCompressedData)
    // if compressionMethod is empty data is not compressed
    std::pair<TString, TString> Compress(const TString& data) const;
    TString Decompress(const TString& data) const;

private:
    const TString CompressionMethod;
    const ui64 MinCompressionSize;
    const NBlockCodecs::ICodec* Codec;
};


}  // namespace NFq
