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
            AFL_ENSURE(codec)("description", "Unknown codec signature.");
            return codec->ReadChunk(data);
        }
    }

protected:
    static inline void VerifySignature(const TDataRef& data) {
        AFL_ENSURE(data.Size() >= sizeof(TCodecSig));
        AFL_ENSURE(ReadUnaligned<TCodecSig>(data.Data()) == Sig());
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
        : MaskSize(0)
        , Bits(nullptr)
    {
        AFL_ENSURE(data.Size() >= sizeof(ui32))
            ("description", "Masked chunk is too small")("chunkSize", data.Size());
        MaskSize = ReadUnaligned<ui32>(data.End() - sizeof(ui32));
        AFL_ENSURE(Size() <= data.Size())
            ("description", "Mask is out of chunk bounds")("maskSize", MaskSize)("chunkSize", data.Size());
        Bits = (const ui8*)(data.End() - Size());
    }

    inline size_t Size() const { return MaskSize + sizeof(ui32); }

    inline bool IsNotNull(size_t index) const {
        AFL_ENSURE(index < MaskSize * 8)
            ("description", "Mask index is out of mask bounds")("index", index)("maskBits", MaskSize * 8);
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
        AFL_ENSURE(data.Size() >= sizeof(ui32))
            ("description", "Masked chunk is too small")("chunkSize", data.Size());
        const char* sizePtr = data.Data() + data.Size() - sizeof(ui32);
        const ui32 maskSize = ReadUnaligned<ui32>(sizePtr);
        AFL_ENSURE(static_cast<size_t>(maskSize) + sizeof(ui32) <= data.Size())
            ("description", "Mask is out of chunk bounds")("maskSize", maskSize)("chunkSize", data.Size());
        BitIter.Reset((const ui8*)(sizePtr - maskSize));
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
            const size_t valuesCount = (Data.Size() - Mask.Size() - sizeof(TCodecSig)) / Size;
            AFL_ENSURE(index < valuesCount)
                ("description", "FixedLen chunk: value index is out of chunk")
                ("index", index)("valuesCount", valuesCount);
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
            AFL_ENSURE(Current <= End);
            return MaskIter.Next() ? TDataRef(data, Size) : TDataRef();
        }

        TDataRef Peek() const override {
            AFL_ENSURE(Current + Size <= End);
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
        const char* payloadStart = Data.Data() + sizeof(TCodecSig);
        const char* chunkEnd = Data.End() - Mask.Size();
        AFL_ENSURE(chunkEnd >= payloadStart)
            ("description", "VarLen chunk is too small")("chunkSize", Data.Size());
        const size_t tailSize = static_cast<size_t>(chunkEnd - payloadStart);
        OffsetsEnd = (const ui32*)chunkEnd;
        if (tailSize >= sizeof(ui32)) {
            PayloadSize = ReadUnaligned<ui32>((const ui32*)chunkEnd - 1);
            AFL_ENSURE(static_cast<size_t>(PayloadSize) + sizeof(ui32) <= tailSize)
                ("description", "VarLen chunk: data size is out of chunk bounds")
                ("dataSize", PayloadSize)("chunkSize", Data.Size());
            AFL_ENSURE((tailSize - PayloadSize) % sizeof(ui32) == 0)
                ("description", "VarLen chunk: misaligned offsets table");
            Sizes = (const ui32*)(payloadStart + PayloadSize);
        } else {
            AFL_ENSURE(tailSize == 0)
                ("description", "VarLen chunk: malformed tail")("chunkSize", Data.Size());
        }
    }

    TAutoPtr<IChunkIterator> MakeIterator() const override {
        return new TChunkIterator(Data, Sizes, OffsetsEnd, PayloadSize);
    }

    TDataRef GetValue(size_t index) const override {
        if (Mask.IsNotNull(index)) {
            AFL_ENSURE(index < static_cast<size_t>(OffsetsEnd - Sizes))
                ("description", "VarLen chunk: value index is out of offsets table")
                ("index", index)("valuesCount", static_cast<size_t>(OffsetsEnd - Sizes));
            ui32 begin = index ? ReadUnaligned<ui32>(Sizes + index - 1) : 0;
            ui32 endOffset = ReadUnaligned<ui32>(Sizes + index);
            AFL_ENSURE(endOffset >= begin && endOffset <= PayloadSize)
                ("description", "VarLen chunk: invalid cumulative offset")
                ("endOffset", endOffset)("beginOffset", begin)("dataSize", PayloadSize);
            return TDataRef(Data.Data() + sizeof(TCodecSig) + begin, endOffset - begin);
        }
        return TDataRef();
    }

private:
    class TChunkIterator : public IChunkIterator {
    public:
        TChunkIterator(const TDataRef& data, const ui32* sizes, const ui32* offsetsEnd, const ui32 payloadSize)
            : Current(data.Data() + sizeof(TCodecSig))
            , PayloadEnd(data.Data() + sizeof(TCodecSig) + payloadSize)
            , LastOffset(0)
            , CurrentOffset(sizes)
            , OffsetsEnd(offsetsEnd)
            , PayloadSize(payloadSize)
            , MaskIter(data)
        { }

        TDataRef Next() override {
            AFL_ENSURE(CurrentOffset + 1 <= OffsetsEnd)
                ("description", "VarLen chunk: value requested beyond the offsets table");
            if (MaskIter.Next()) {
                ui32 endOffset = ReadUnaligned<ui32>(CurrentOffset);
                AFL_ENSURE(endOffset >= LastOffset && endOffset <= PayloadSize)
                    ("description", "VarLen chunk: invalid cumulative offset")
                    ("endOffset", endOffset)("lastOffset", LastOffset)("dataSize", PayloadSize);
                ++CurrentOffset;
                ui32 size = endOffset - LastOffset;
                LastOffset = endOffset;

                const char* data = Current;
                Current += size;
                AFL_ENSURE(data + size <= PayloadEnd);
                return TDataRef(data, size);
            }
            ++CurrentOffset;
            return TDataRef();
        }

        TDataRef Peek() const override {
            AFL_ENSURE(CurrentOffset + 1 <= OffsetsEnd)
                ("description", "VarLen chunk: value requested beyond the offsets table");
            if (MaskIter.IsNotNull()) {
                ui32 endOffset = ReadUnaligned<ui32>(CurrentOffset);
                AFL_ENSURE(endOffset >= LastOffset && endOffset <= PayloadSize)
                    ("description", "VarLen chunk: invalid cumulative offset")
                    ("endOffset", endOffset)("lastOffset", LastOffset)("dataSize", PayloadSize);
                return TDataRef(Current, endOffset - LastOffset);
            }
            return TDataRef();
        }

    private:
        const char* Current;
        const char* PayloadEnd;
        ui32 LastOffset;
        const ui32* CurrentOffset;
        const ui32* OffsetsEnd;
        ui32 PayloadSize;
        TDecoderMaskIterator<IsNullable> MaskIter;
    };

private:
    TDataRef Data;
    const ui32* Sizes;
    TDecoderMask<IsNullable> Mask;
    const ui32* OffsetsEnd = nullptr;
    ui32 PayloadSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TIntType>
class TVarIntValueDecoder {
public:
    using TType = TIntType;

    inline TType Peek(const char* data, const char* end) const {
        AFL_ENSURE(data < end)("description", "No data left in the chunk");
        i64 value;
        auto bytes = in_long(value, data);
        AFL_ENSURE(data + bytes <= end)("description", "Value is out of chunk bounds");
        return value;
    }

    inline size_t Load(const char* data, const char* end, TType& value) const {
        AFL_ENSURE(data < end)("description", "No data left in the chunk");
        i64 loaded = 0;
        auto bytes = in_long(loaded, data);
        AFL_ENSURE(data + bytes <= end)("description", "Value is out of chunk bounds");
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
        AFL_ENSURE(data < end)("description", "No data left in the chunk");
        i64 value;
        auto bytes = in_long(value, data);
        AFL_ENSURE(data + bytes <= end)("description", "Value is out of chunk bounds");
        return ZigZagDecode(static_cast<TUnsigned>(value));
    }

    inline size_t Load(const char* data, const char* end, TType& value) const {
        AFL_ENSURE(data < end)("description", "No data left in the chunk");
        i64 loaded = 0;
        auto bytes = in_long(loaded, data);
        AFL_ENSURE(data + bytes <= end)("description", "Value is out of chunk bounds");
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

