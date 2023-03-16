#include "compression.h"
#include <library/cpp/blockcodecs/codecs.h>

namespace NFq {

namespace {
    const NBlockCodecs::ICodec* ProvideCodec(const TString& compressionMethod) {
        auto codec = NBlockCodecs::Codec(compressionMethod);
        if (!codec) {
            throw yexception() << "Unable to find codec for compression method " << compressionMethod;
        }
        return codec;
    }
}

TCompressor::TCompressor(const TString& compressionMethod, ui64 minCompressionSize)
    : CompressionMethod(compressionMethod)
    , MinCompressionSize(minCompressionSize)
    , Codec(compressionMethod ? ProvideCodec(compressionMethod) : nullptr)
{
}

bool TCompressor::IsEnabled() const {
    return Codec != nullptr;
}

std::pair<TString, TString> TCompressor::Compress(const TString& data) const {
    if (!IsEnabled() || data.size() < MinCompressionSize) {
        return { "", data };
    }
    return { CompressionMethod, Codec->Encode(data) };
}

TString TCompressor::Decompress(const TString& data) const {
    if (!IsEnabled()) {
        return data;
    }

    return Codec->Decode(data);
}

}  // namespace NFq
