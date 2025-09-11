#pragma once

#include "type_codecs_defs.h"

#include <library/cpp/containers/bitseq/bititerator.h>
#include <library/cpp/packedtypes/longs.h>
#include <library/cpp/packedtypes/zigzag.h>

#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NScheme {

using TMaskIterator = TBitIterator<ui8>;

////////////////////////////////////////////////////////////////////////////////

class TDefaultChunkDecoder : public IChunkDecoder {
public:
    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TDefaultChunkIterator(this);
    }

    TAutoPtr<IChunkBidirIterator> MakeBidirIterator() const override {
        return new TDefaultChunkBidirIterator(this);
    }

private:
    template <typename TIterator>
    class TDefaultChunkIteratorOps : public TIterator {
    public:
        TDefaultChunkIteratorOps(const IChunkDecoder* decoder)
            : Decoder(decoder)
            , Index(0)
        { }

        TDataRef Next() override {
            return Decoder->GetValue(Index++);
        }

        TDataRef Peek() const override {
            return Decoder->GetValue(Index);
        }

    protected:
        const IChunkDecoder* Decoder;
        size_t Index;
    };

    using TDefaultChunkIterator = TDefaultChunkIteratorOps<IChunkIterator>;

    class TDefaultChunkBidirIterator : public TDefaultChunkIteratorOps<IChunkBidirIterator> {
    public:
        TDefaultChunkBidirIterator(const IChunkDecoder* decoder)
            : TDefaultChunkIteratorOps<IChunkBidirIterator>(decoder)
        { }

        void Back() override {
            Y_DEBUG_ABORT_UNLESS(Index > 0);
            --Index;
        }
    };
};

template <typename TDerived, ui16 Type, bool IsNullable>
class TChunkDecoderBase : public TDefaultChunkDecoder {
public:
    inline static TCodecSig Sig() {
        return TCodecSig(TCodecType(Type), IsNullable);
    }

    TCodecSig Signature() const override { return Sig(); }

    IChunkDecoder::TPtr ReadNext(const TDataRef& data, const TTypeCodecs* codecs) const override {
        return ReadNextImpl(data, codecs);
    }

    static inline IChunkDecoder::TPtr ReadNextImpl(const TDataRef& data, const TTypeCodecs* codecs) {
        Y_DEBUG_ABORT_UNLESS(data.Size() >= sizeof(TCodecSig));
        const TCodecSig sig = ReadUnaligned<TCodecSig>(data.Data());
        if (Y_LIKELY(sig == Sig())) {
            return new TDerived(data);
        } else {
            auto codec = codecs->GetCodec(sig);
            Y_ABORT_UNLESS(codec, "Unknown codec signature.");
            return codec->ReadChunk(data);
        }
    }

protected:
    static inline void VerifySignature(const TDataRef& data) {
        Y_ABORT_UNLESS(data.Size() >= sizeof(TCodecSig));
        Y_ABORT_UNLESS(ReadUnaligned<TCodecSig>(data.Data()) == Sig());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TDecoderMask;

template <bool IsNullable>
class TDecoderMaskIterator;

template <>
class TDecoderMask<true> {
public:
    TDecoderMask(const TDataRef& data)
        : MaskSize(ReadUnaligned<ui32>(data.End() - sizeof(ui32)))
        , Bits((const ui8*)(data.End() - Size()))
    { }

    inline size_t Size() const { return MaskSize + sizeof(ui32); }

    inline bool IsNotNull(size_t index) const {
        TMaskIterator iter(Bits);
        iter.Forward(index);
        return iter.Peek();
    }

private:
    size_t MaskSize;
    const ui8* Bits;
};

template <>
class TDecoderMaskIterator<true> {
public:
    TDecoderMaskIterator(const TDataRef& data) {
        const char* sizePtr = data.Data() + data.Size() - sizeof(ui32);
        BitIter.Reset((const ui8*)(sizePtr - ReadUnaligned<ui32>(sizePtr)));
    }
    inline bool IsNotNull() const { return BitIter.Peek(); }
    inline bool Next() { return BitIter.Next(); }

private:
    TMaskIterator BitIter;
};

template <>
class TDecoderMask<false> {
public:
    TDecoderMask(const TDataRef&) { }

    inline size_t Size() const { return 0; }
    inline bool IsNotNull(size_t) const { return true; }
};

template <>
class TDecoderMaskIterator<false> {
public:
    TDecoderMaskIterator(const TDataRef&) { }

    inline bool IsNotNull() const { return true; }
    inline bool Next() { return true; }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, bool IsNullable>
class TFixedLenDecoder : public TChunkDecoderBase<TFixedLenDecoder<Size, IsNullable>, ui16(TCodecType::FixedLen), IsNullable> {
public:
    TFixedLenDecoder(const TDataRef& data)
        : Data(data)
        , Mask(Data)
    {
        TFixedLenDecoder::VerifySignature(data);
    }

    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TChunkIterator(Data, Mask.Size());
    }

    TDataRef GetValue(size_t index) const override {
        if (Mask.IsNotNull(index)) {
            const char* data = Data.Data() + sizeof(TCodecSig) + index * Size;
            return TDataRef(data, Size);
        }
        return TDataRef();
    }

private:
    class TChunkIterator : public IChunkIterator {
    public:
        TChunkIterator(const TDataRef& data, const size_t maskSize)
            : Current(data.Data() + sizeof(TCodecSig))
            , End(data.End() - maskSize)
            , MaskIter(data)
        { }

        TDataRef Next() override {
            const char* data = Current;
            Current += Size;
            Y_ABORT_UNLESS(Current <= End);
            return MaskIter.Next() ? TDataRef(data, Size) : TDataRef();
        }

        TDataRef Peek() const override {
            Y_ABORT_UNLESS(Current + Size <= End);
            return MaskIter.IsNotNull() ? TDataRef(Current, Size) : TDataRef();
        }

    private:
        const char* Current;
        const char* End;
        TDecoderMaskIterator<IsNullable> MaskIter;
    };

private:
    TDataRef Data;
    TDecoderMask<IsNullable> Mask;
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TVarLenDecoder : public TChunkDecoderBase<TVarLenDecoder<IsNullable>, ui16(TCodecType::VarLen), IsNullable> {
public:
    TVarLenDecoder(const TDataRef& data)
        : Data(data)
        , Sizes((const ui32*)(data.Data() + sizeof(TCodecSig)))
        , Mask(Data)
    {
        TVarLenDecoder::VerifySignature(data);
        const char* sizesEnd = data.End() - Mask.Size();
        if ((const char*)(Sizes + 1) <= sizesEnd) {
            auto dataSize = ReadUnaligned<ui32>((const ui32*)sizesEnd - 1);
            Sizes = (const ui32*)((const char*)Sizes + dataSize);
        }
    }

    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TChunkIterator(Data, Sizes, Data.End() - Mask.Size());
    }

    TDataRef GetValue(size_t index) const override {
        if (Mask.IsNotNull(index)) {
            Y_ABORT_UNLESS(ReadUnaligned<ui32>(Sizes + index) + sizeof(TCodecSig) <= Data.Size() - Mask.Size());
            ui32 begin = index ? ReadUnaligned<ui32>(Sizes + index - 1) : 0;
            return TDataRef(Data.Data() + sizeof(TCodecSig) + begin, ReadUnaligned<ui32>(Sizes + index) - begin);
        }
        return TDataRef();
    }

private:
    class TChunkIterator : public IChunkIterator {
    public:
        TChunkIterator(const TDataRef& data, const ui32* sizes, const char* end)
            : Current(data.Data() + sizeof(TCodecSig))
            , End(end)
            , LastOffset(0)
            , CurrentOffset(sizes)
            , MaskIter(data)
        { }

        TDataRef Next() override {
            if (MaskIter.Next()) {
                ui32 endOffset = ReadUnaligned<ui32>(CurrentOffset);
                CurrentOffset++;
                ui32 size = endOffset - LastOffset;
                LastOffset = endOffset;

                const char* data = Current;
                Current += size;
                Y_ABORT_UNLESS(data + size <= End);
                return TDataRef(data, size);
            }
            ++CurrentOffset;
            return TDataRef();
        }

        TDataRef Peek() const override {
            if (MaskIter.IsNotNull()) {
                Y_ABORT_UNLESS(Current + ReadUnaligned<ui32>(CurrentOffset) - LastOffset <= End);
                return TDataRef(Current, ReadUnaligned<ui32>(CurrentOffset) - LastOffset);
            }
            return TDataRef();
        }

    private:
        const char* Current;
        const char* End;
        ui32 LastOffset;
        const ui32* CurrentOffset;
        TDecoderMaskIterator<IsNullable> MaskIter;
    };

private:
    TDataRef Data;
    const ui32* Sizes;
    TDecoderMask<IsNullable> Mask;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TIntType>
class TVarIntValueDecoder {
public:
    using TType = TIntType;

    inline TType Peek(const char* data, const char* end) const {
        i64 value;
        auto bytes = in_long(value, data);
        Y_ABORT_UNLESS(data + bytes <= end);
        return value;
    }

    inline size_t Load(const char* data, const char* end, TType& value) const {
        i64 loaded = 0;
        auto bytes = in_long(loaded, data);
        Y_ABORT_UNLESS(data + bytes <= end);
        value = loaded;
        return bytes;
    }
};

template <typename TIntType>
class TZigZagValueDecoder {
public:
    using TType = TIntType;
    using TUnsigned = std::make_unsigned_t<TType>;

    inline TType Peek(const char* data, const char* end) const {
        i64 value;
        auto bytes = in_long(value, data);
        Y_ABORT_UNLESS(data + bytes <= end);
        return ZigZagDecode(static_cast<TUnsigned>(value));
    }

    inline size_t Load(const char* data, const char* end, TType& value) const {
        i64 loaded = 0;
        auto bytes = in_long(loaded, data);
        Y_ABORT_UNLESS(data + bytes <= end);
        value = ZigZagDecode(static_cast<TUnsigned>(loaded));
        return bytes;
    }
};

template <typename TValueDecoder, bool Rev = false>
class TDeltaValueDecoder : public TValueDecoder {
public:
    using TType = typename TValueDecoder::TType;

    inline TType Peek(const char* data, const char* end) const {
        TType value;
        TValueDecoder::Load(data, end, value);
        return Rev ? Last - value : Last + value;
    }

    inline size_t Load(const char* data, const char* end, TType& value) {
        auto bytes = TValueDecoder::Load(data, end, value);
        if (Rev)
            Last -= value;
        else
            Last += value;
        value = Last;
        return bytes;
    }

private:
    TType Last = 0;
};

template <typename TDerived, typename TValueDecoder, ui16 CodecType, bool IsNullable>
class TIntDecoderImpl : public TChunkDecoderBase<TDerived, CodecType, IsNullable> {
public:
    using TType = typename TValueDecoder::TType;

    TIntDecoderImpl(const TDataRef& data)
        : Data(data)
        , Mask(Data)
        , Fetched(Data.Data() + sizeof(TCodecSig))
        , MaskIter(Data)
    {
        TIntDecoderImpl::VerifySignature(data);
    }

    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TChunkIterator(Data, Mask.Size());
    }

    TDataRef GetValue(size_t index) const override {
        if (Mask.IsNotNull(index)) {
            FetchTo(index);
            return GetFromCache(index);
        }
        return TDataRef();
    }

private:
    inline void FetchTo(size_t index) const {
        if (index < Cache.size())
            return;
        TType value;
        for (size_t count = index + 1 - Cache.size(); count; --count)
            if (MaskIter.Next()) {
                Fetched += ValueDecoder.Load(Fetched, Data.End() - Mask.Size(), value);
                Cache.push_back(value);
            } else {
                Cache.push_back(0);
            }
    }

    inline TDataRef GetFromCache(size_t index) const {
        Y_DEBUG_ABORT_UNLESS(index < Cache.size());
        return TDataRef((const char*)&Cache[index], sizeof(TType));
    }

private:
    class TChunkIterator : public IChunkIterator {
    public:
        TChunkIterator(const TDataRef& data, const size_t maskSize)
            : Current(data.Data() + sizeof(TCodecSig))
            , End(data.End() - maskSize)
            , MaskIter(data)
        { }

        TDataRef Next() override {
            if (MaskIter.Next()) {
                TType value;
                Current += ValueDecoder.Load(Current, End, value);
                return TDataRef((const char*)&value, sizeof(value), true);
            }
            return TDataRef();
        }

        TDataRef Peek() const override {
            if (MaskIter.IsNotNull()) {
                const TType value = ValueDecoder.Peek(Current, End);
                return TDataRef((const char*)&value, sizeof(value), true);
            }
            return TDataRef();
        }

    private:
        const char* Current;
        const char* End;
        TDecoderMaskIterator<IsNullable> MaskIter;
        TValueDecoder ValueDecoder;
    };

    using TCache = TDeque<TType>;

private:
    TDataRef Data;
    TDecoderMask<IsNullable> Mask;

    mutable const char* Fetched;
    mutable TDecoderMaskIterator<IsNullable> MaskIter;
    mutable TCache Cache;
    mutable TValueDecoder ValueDecoder;
};

template <typename TIntType, bool IsNullable>
class TVarIntDecoder : public TIntDecoderImpl<TVarIntDecoder<TIntType, IsNullable>, TVarIntValueDecoder<TIntType>, ui16(TCodecType::VarInt), IsNullable> {
public:
    TVarIntDecoder(const TDataRef& data)
        : TIntDecoderImpl<TVarIntDecoder<TIntType, IsNullable>, TVarIntValueDecoder<TIntType>, ui16(TCodecType::VarInt), IsNullable>(data)
    { }
};

template <typename TIntType, bool IsNullable>
class TZigZagDecoder : public TIntDecoderImpl<TZigZagDecoder<TIntType, IsNullable>, TZigZagValueDecoder<TIntType>, ui16(TCodecType::ZigZag), IsNullable> {
public:
    TZigZagDecoder(const TDataRef& data)
        : TIntDecoderImpl<TZigZagDecoder<TIntType, IsNullable>, TZigZagValueDecoder<TIntType>, ui16(TCodecType::ZigZag), IsNullable>(data)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaVarIntDecoder : public TIntDecoderImpl<TDeltaVarIntDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TVarIntValueDecoder<TIntType>>, ui16(TCodecType::DeltaVarInt), IsNullable> {
public:
    TDeltaVarIntDecoder(const TDataRef& data)
        : TIntDecoderImpl<TDeltaVarIntDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TVarIntValueDecoder<TIntType>>, ui16(TCodecType::DeltaVarInt), IsNullable>(data)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaRevVarIntDecoder : public TIntDecoderImpl<TDeltaRevVarIntDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TVarIntValueDecoder<TIntType>, true>, ui16(TCodecType::DeltaRevVarInt), IsNullable> {
public:
    TDeltaRevVarIntDecoder(const TDataRef& data)
        : TIntDecoderImpl<TDeltaRevVarIntDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TVarIntValueDecoder<TIntType>, true>, ui16(TCodecType::DeltaRevVarInt), IsNullable>(data)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaZigZagDecoder : public TIntDecoderImpl<TDeltaZigZagDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TZigZagValueDecoder<TIntType>>, ui16(TCodecType::DeltaZigZag), IsNullable> {
public:
    TDeltaZigZagDecoder(const TDataRef& data)
        : TIntDecoderImpl<TDeltaZigZagDecoder<TIntType, IsNullable>, TDeltaValueDecoder<TZigZagValueDecoder<TIntType>>, ui16(TCodecType::DeltaZigZag), IsNullable>(data)
    { }
};

/// TODO: FetchAll / FetchOnDemand / DoNotCache options.

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TBoolDecoder : public TChunkDecoderBase<TBoolDecoder<IsNullable>, ui16(TCodecType::Bool), IsNullable> {
public:
    TBoolDecoder(const TDataRef& data)
        : Data(data)
    {
        TBoolDecoder::VerifySignature(data);
    }

    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TChunkIterator(Data.Data() + sizeof(TCodecSig));
    }

    TDataRef GetValue(size_t index) const override {
        TMaskIterator iter((const ui8*)(Data.Data() + sizeof(TCodecSig)));
        iter.Forward(IsNullable ? index * 2 : index);
        if (IsNullable && !iter.Next())
            return TDataRef();
        return GetValue(iter.Peek());
    }

private:
    class TChunkIterator : public IChunkIterator {
    public:
        TChunkIterator(const char* data)
            : BitIter((const ui8*)data)
        { }

        TDataRef Next() override {
            return IsNullable ? ParseNullable(BitIter.Read(2)) : GetValue(BitIter.Next());
        }

        TDataRef Peek() const override {
            return IsNullable ? ParseNullable(BitIter.Peek(2)) : GetValue(BitIter.Peek());
        }

    private:
        static inline TDataRef ParseNullable(ui16 word) {
            return (word & 1) ? GetValue(word >> 1) : TDataRef();
        }

    private:
        TMaskIterator BitIter;
    };

private:
    static const bool Values[2];

    static inline TDataRef GetValue(int index) {
        return TDataRef((const char*)&Values[index], sizeof(bool));
    }

    TDataRef Data;
};

template <bool IsNullable>
const bool TBoolDecoder<IsNullable>::Values[2] = {false, true};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheme
} // namespace NKikimr

