#pragma once

#include "type_codecs_defs.h"
#include "type_coders.h"
#include "type_decoders.h"

namespace NKikimr {
namespace NScheme {

template <typename TCoder, typename TDecoder>
class TCodecImpl : public ICodec {
public:
    static inline TCodecSig Sig() {
        return TCoder::Sig();
    }

public:
    TCodecImpl() {
        Y_ABORT_UNLESS(TCoder::Sig() == TDecoder::Sig(), "Codecs signatures mismatch (cd: %u, dc: %u).",
               ui16(TCoder::Sig()), ui16(TDecoder::Sig()));
    }

    TCodecSig Signature() const override {
        Y_DEBUG_ABORT_UNLESS(TCoder::Sig() == TDecoder::Sig());
        return TCoder::Sig();
    }

    TAutoPtr<IChunkCoder> MakeChunk(TBuffer& output) const override {
        return new TCoder(output);
    }

    IChunkDecoder::TPtr ReadChunk(const TDataRef& data) const override {
        return new TDecoder(data);
    }

    IChunkDecoder::TPtr ReadChunk(const TDataRef& data, const TTypeCodecs* codecs) const override {
        return TDecoder::ReadNextImpl(data, codecs);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, bool IsNullable>
class TFixedLenCodec : public TCodecImpl<TFixedLenCoder<Size, IsNullable>, TFixedLenDecoder<Size, IsNullable>> { };

template <bool IsNullable>
class TVarLenCodec : public TCodecImpl<TVarLenCoder<IsNullable>, TVarLenDecoder<IsNullable>> { };

template <typename TIntType, bool IsNullable>
class TVarIntCodec : public TCodecImpl<TVarIntCoder<TIntType, IsNullable>, TVarIntDecoder<TIntType, IsNullable>> { };

template <typename TIntType, bool IsNullable>
class TZigZagCodec : public TCodecImpl<TZigZagCoder<TIntType, IsNullable>, TZigZagDecoder<TIntType, IsNullable>> { };

template <typename TIntType, bool IsNullable>
class TDeltaVarIntCodec : public TCodecImpl<TDeltaVarIntCoder<TIntType, IsNullable>, TDeltaVarIntDecoder<TIntType, IsNullable>> { };

template <typename TIntType, bool IsNullable>
class TDeltaRevVarIntCodec : public TCodecImpl<TDeltaRevVarIntCoder<TIntType, IsNullable>, TDeltaRevVarIntDecoder<TIntType, IsNullable>> { };

template <typename TIntType, bool IsNullable>
class TDeltaZigZagCodec : public TCodecImpl<TDeltaZigZagCoder<TIntType, IsNullable>, TDeltaZigZagDecoder<TIntType, IsNullable>> { };

template <bool IsNullable>
class TBoolCodec : public TCodecImpl<TBoolCoder<IsNullable>, TBoolDecoder<IsNullable>> { };

} // namespace NScheme
} // namespace NKikimr

