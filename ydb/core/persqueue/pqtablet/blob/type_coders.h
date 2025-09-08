#pragma once

#include "type_codecs_defs.h"

#include <library/cpp/containers/bitseq/bitvector.h>
#include <library/cpp/packedtypes/longs.h>
#include <library/cpp/packedtypes/zigzag.h>

#include <util/generic/typetraits.h>
#include <util/generic/buffer.h>

namespace NKikimr {
namespace NScheme {


template <typename T>
void AppendPOD(TBuffer& output, const T& dt) {
    output.Append((const char*)&dt, (ui32)sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

template <ui16 Type, bool IsNullable>
class TChunkCoderBase : public IChunkCoder {
public:
    static inline TCodecSig Sig() {
        return TCodecSig(TCodecType(Type), IsNullable);
    }

public:
    TCodecSig Signature() const override { return Sig(); }
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TCoderMask;

template <>
class TCoderMask<true> {
public:
    inline size_t MaxSize() const {
        return Mask.Words() * sizeof(TMask::TWord) + sizeof(ui32);
    }

    inline void AddNull() { Mask.Append(0, 1); }
    inline void AddNonNull() { Mask.Append(1, 1); }

    inline void Seal(TBuffer& output) {
        const ui32 maskSize = Mask.Words() * sizeof(TMask::TWord);
        output.Append((const char*)Mask.Data(), maskSize);
        output.Append((const char*)&maskSize, sizeof(maskSize)); // TODO: Reduce size of small masks (embed size into the first word).
    }

private:
    using TMask = TBitVector<ui16>;
    TMask Mask;
};

template <>
class TCoderMask<false> {
public:
    inline size_t MaxSize() const { return 0; }
    inline void AddNull() { Y_ABORT("Null values are not supported."); }
    inline void AddNonNull() { }
    inline void Seal(TBuffer&) { }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, bool IsNullable>
class TFixedLenCoder : public TChunkCoderBase<ui16(TCodecType::FixedLen), IsNullable> {
public:
    TFixedLenCoder(TBuffer& output)
        : DataSize(0)
        , Output(output)
    {
        AppendPOD(output, TFixedLenCoder::Sig());
    }

    size_t GetEstimatedSize() const override {
        return sizeof(TCodecSig) + DataSize + Mask.MaxSize();
    }

    void Seal() override {
        Mask.Seal(Output);
    }

protected:
    void DoAddData(const char* data, size_t size) override {
        Y_ABORT_UNLESS(size == Size, "Size mismatch.");
        Mask.AddNonNull();
        DataSize += Size;
        Output.Append(data, size);
    }

    void DoAddNull() override {
        Mask.AddNull();
        static char zero[Size ? Size : 1];
        Output.Append(zero, Size);
        // TODO: Consider using succinct Rank structure instead.
    }

private:
    size_t DataSize;
    TCoderMask<IsNullable> Mask;
    TBuffer& Output;
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TVarLenCoder : public TChunkCoderBase<ui16(TCodecType::VarLen), IsNullable> {
public:
    TVarLenCoder(TBuffer& output)
        : DataSize(0)
        , Output(output)
    {
        AppendPOD(output, TVarLenCoder::Sig());
    }

    size_t GetEstimatedSize() const override {
        return sizeof(TCodecSig) + DataSize + Offsets.size() * sizeof(ui32) + Mask.MaxSize() + sizeof(ui32);
    }

    void Seal() override {
        Output.Append((const char*)Offsets.data(), Offsets.size() * sizeof(ui32));
        Mask.Seal(Output);
    }

protected:
    void DoAddData(const char* data, size_t size) override {
        Mask.AddNonNull();
        DataSize += size;
        Offsets.push_back(DataSize);
        Output.Append(data, size);
    }

    void DoAddNull() override {
        Mask.AddNull();
        Offsets.push_back(DataSize);
    }

private:
    ui32 DataSize;
    TVector<ui32> Offsets;
    TCoderMask<IsNullable> Mask;
    TBuffer& Output;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TIntType>
class TVarIntValueCoder {
public:
    using TType = TIntType;
    using TSigned = std::make_signed_t<TType>;

    inline size_t Save(TBuffer& output, TType value) {
        const auto outValue = static_cast<i64>(value); // TODO: fix out_long(i32)
        char varIntOut[sizeof(outValue) + 1];
        auto bytes = out_long(outValue, varIntOut);
        output.Append(varIntOut, bytes);
        return bytes;
    }
};

template <typename TIntType>
class TZigZagValueCoder {
public:
    using TType = TIntType;
    using TSigned = std::make_signed_t<TType>;

    inline size_t Save(TBuffer& output, TType value) {
        const auto zigZagged = static_cast<i64>(ZigZagEncode(value));
        char varIntOut[sizeof(zigZagged) + 1];
        auto bytes = out_long(zigZagged, varIntOut);
        output.Append(varIntOut, bytes);
        return bytes;
    }
};

template <typename TValueCoder, bool Rev = false>
class TDeltaValueCoder : public TValueCoder {
public:
    using TType = typename TValueCoder::TType;

    inline size_t Save(TBuffer& output, TType value) {
        auto delta = Rev ? Last - value : value - Last;
        Last = value;
        return TValueCoder::Save(output, delta);
    }

private:
    TType Last = 0;
};

template <typename TValueCoder, ui16 CodecType, bool IsNullable>
class TByteAlignedIntCoder : public TChunkCoderBase<CodecType, IsNullable> {
public:
    using TType = typename TValueCoder::TType;

    TByteAlignedIntCoder(TBuffer& output)
        : DataSize(0)
        , Output(output)
    {
        AppendPOD(output, TByteAlignedIntCoder::Sig());
    }

    size_t GetEstimatedSize() const override {
        return sizeof(TCodecSig) + DataSize + Mask.MaxSize();
    }

    void Seal() override {
        Mask.Seal(Output);
    }

protected:
    void DoAddData(const char* data, size_t size) override {
        Y_ABORT_UNLESS(size == sizeof(TType));
        Mask.AddNonNull();
        DataSize += ValueCoder.Save(Output, ReadUnaligned<TType>(data));
    }

    void DoAddNull() override {
        Mask.AddNull();
    }

private:
    size_t DataSize;
    TCoderMask<IsNullable> Mask;
    TBuffer& Output;
    TValueCoder ValueCoder;
};

template <typename TIntType, bool IsNullable>
class TVarIntCoder : public TByteAlignedIntCoder<TVarIntValueCoder<TIntType>, ui16(TCodecType::VarInt), IsNullable> {
public:
    TVarIntCoder(TBuffer& output)
        : TByteAlignedIntCoder<TVarIntValueCoder<TIntType>, ui16(TCodecType::VarInt), IsNullable>(output)
    { }
};

template <typename TIntType, bool IsNullable>
class TZigZagCoder : public TByteAlignedIntCoder<TZigZagValueCoder<TIntType>, ui16(TCodecType::ZigZag), IsNullable> {
public:
    TZigZagCoder(TBuffer& output)
        : TByteAlignedIntCoder<TZigZagValueCoder<TIntType>, ui16(TCodecType::ZigZag), IsNullable>(output)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaVarIntCoder : public TByteAlignedIntCoder<TDeltaValueCoder<TVarIntValueCoder<TIntType>>, ui16(TCodecType::DeltaVarInt), IsNullable> {
public:
    TDeltaVarIntCoder(TBuffer& output)
        : TByteAlignedIntCoder<TDeltaValueCoder<TVarIntValueCoder<TIntType>>, ui16(TCodecType::DeltaVarInt), IsNullable>(output)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaRevVarIntCoder : public TByteAlignedIntCoder<TDeltaValueCoder<TVarIntValueCoder<TIntType>, true>, ui16(TCodecType::DeltaRevVarInt), IsNullable> {
public:
    TDeltaRevVarIntCoder(TBuffer& output)
        : TByteAlignedIntCoder<TDeltaValueCoder<TVarIntValueCoder<TIntType>, true>, ui16(TCodecType::DeltaRevVarInt), IsNullable>(output)
    { }
};

template <typename TIntType, bool IsNullable>
class TDeltaZigZagCoder : public TByteAlignedIntCoder<TDeltaValueCoder<TZigZagValueCoder<TIntType>>, ui16(TCodecType::DeltaZigZag), IsNullable> {
public:
    TDeltaZigZagCoder(TBuffer& output)
        : TByteAlignedIntCoder<TDeltaValueCoder<TZigZagValueCoder<TIntType>>, ui16(TCodecType::DeltaZigZag), IsNullable>(output)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsNullable>
class TBoolCoder : public TChunkCoderBase<ui16(TCodecType::Bool), IsNullable> {
public:
    TBoolCoder(TBuffer& output)
        : Output(output)
    {
        AppendPOD(output, TBoolCoder::Sig());
    }

    size_t GetEstimatedSize() const override {
        return sizeof(TCodecSig) + Mask.Words() * sizeof(TMask::TWord);
    }

    void Seal() override {
        Output.Append((const char*)Mask.Data(), Mask.Words() * sizeof(TMask::TWord));
    }

protected:
    void DoAddData(const char* data, size_t size) override {
        Y_DEBUG_ABORT_UNLESS(size == sizeof(bool));
        if (IsNullable)
            Mask.Append(1, 1);
        Mask.Append(*(const bool*)data, 1);
    }

    void DoAddNull() override {
        Y_ABORT_UNLESS(IsNullable, "Null values are not supported.");
        Mask.Append(0, 2);
    }

protected:
    using TMask = TBitVector<ui16>;
    TMask Mask;
    TBuffer& Output;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheme
} // namespace NKikimr

