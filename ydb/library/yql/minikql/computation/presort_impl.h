#pragma once
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/utils/swap_bytes.h>

#include <util/generic/vector.h>

namespace NKikimr {
namespace NMiniKQL {

namespace NDetail {

    using NYql::SwapBytes;

    Y_FORCE_INLINE
        void EnsureInputSize(TStringBuf& input, size_t size) {
        MKQL_ENSURE(input.size() >= size, "premature end of input");
    }

    template <bool Desc>
    Y_FORCE_INLINE
        void EncodeBool(TVector<ui8>& output, bool value) {
        output.push_back(Desc ? 0xFF ^ ui8(value) : ui8(value));
    }

    template <bool Desc>
    Y_FORCE_INLINE
        bool DecodeBool(TStringBuf& input) {
        EnsureInputSize(input, 1);
        auto result = Desc ? bool(0xFF ^ ui8(input[0])) : bool(input[0]);
        input.Skip(1);
        return result;
    }

    template <typename TUnsigned, bool Desc>
    Y_FORCE_INLINE
        void EncodeUnsigned(TVector<ui8>& output, TUnsigned value) {
        constexpr size_t size = sizeof(TUnsigned);

        if (Desc) {
            value = ~value;
        }

        output.resize(output.size() + size);
        WriteUnaligned<TUnsigned>(output.end() - size, SwapBytes(value));
    }

    template <typename TUnsigned, bool Desc>
    Y_FORCE_INLINE
        TUnsigned DecodeUnsigned(TStringBuf& input) {
        constexpr size_t size = sizeof(TUnsigned);

        EnsureInputSize(input, size);
        auto value = ReadUnaligned<TUnsigned>(input.data());
        input.Skip(size);

        value = SwapBytes(value);
        if (Desc) {
            value = ~value;
        }
        return value;
    }

    template <typename TSigned, bool Desc>
    Y_FORCE_INLINE
        void EncodeSigned(TVector<ui8>& output, TSigned value) {
        using TUnsigned = std::make_unsigned_t<TSigned>;
        constexpr size_t size = sizeof(TUnsigned);
        constexpr TUnsigned shift = TUnsigned(1) << (size * 8 - 1);

        EncodeUnsigned<TUnsigned, Desc>(output, TUnsigned(value) + shift);
    }

    template <typename TSigned, bool Desc>
    Y_FORCE_INLINE
        TSigned DecodeSigned(TStringBuf& input) {
        using TUnsigned = std::make_unsigned_t<TSigned>;
        constexpr size_t size = sizeof(TUnsigned);
        constexpr TUnsigned shift = TUnsigned(1) << (size * 8 - 1);

        return TSigned(DecodeUnsigned<TUnsigned, Desc>(input) - shift);
    }

    enum class EFPCode : ui8 {
        NegInf = 0,
        Neg = 1,
        Zero = 2,
        Pos = 3,
        PosInf = 4,
        Nan = 5
    };

    template <typename TFloat>
    struct TFloatToInteger {};

    template <>
    struct TFloatToInteger<float> {
        using TType = ui32;
    };

    template <>
    struct TFloatToInteger<double> {
        using TType = ui64;
    };

    static_assert(std::numeric_limits<float>::is_iec559, "float type is not iec559(ieee754)");
    static_assert(std::numeric_limits<double>::is_iec559, "double type is not iec559(ieee754)");

    template <typename TFloat, bool Desc>
    Y_FORCE_INLINE
        void EncodeFloating(TVector<ui8>& output, TFloat value) {
        using TInteger = typename TFloatToInteger<TFloat>::TType;
        EFPCode code;

        switch (std::fpclassify(value)) {
        case FP_NORMAL:
        case FP_SUBNORMAL: {
            auto integer = ReadUnaligned<TInteger>(&value);
            if (value < 0) {
                integer = ~integer;
                code = EFPCode::Neg;
            }
            else {
                code = EFPCode::Pos;
            }
            output.push_back(Desc ? 0xFF ^ ui8(code) : ui8(code));
            EncodeUnsigned<TInteger, Desc>(output, integer);
            return;
        }
        case FP_ZERO:
            code = EFPCode::Zero;
            break;
        case FP_INFINITE:
            code = value < 0 ? EFPCode::NegInf : EFPCode::PosInf;
            break;
        default:
            code = EFPCode::Nan;
            break;
        }
        output.push_back(Desc ? 0xFF ^ ui8(code) : ui8(code));
    }

    template <typename TFloat, bool Desc>
    Y_FORCE_INLINE
        TFloat DecodeFloating(TStringBuf& input) {
        using TInteger = typename TFloatToInteger<TFloat>::TType;

        EnsureInputSize(input, 1);
        auto code = EFPCode(Desc ? 0xFF ^ input[0] : input[0]);
        input.Skip(1);

        bool negative;
        switch (code) {
        case EFPCode::Zero:
            return 0;
        case EFPCode::NegInf:
            return -std::numeric_limits<TFloat>::infinity();
        case EFPCode::PosInf:
            return std::numeric_limits<TFloat>::infinity();
        case EFPCode::Nan:
            return std::numeric_limits<TFloat>::quiet_NaN();
        case EFPCode::Neg:
            negative = true;
            break;
        case EFPCode::Pos:
            negative = false;
            break;
        default:
            MKQL_ENSURE(false, "floating point data is corrupted");
        }

        auto integer = DecodeUnsigned<TInteger, Desc>(input);
        if (negative) {
            integer = ~integer;
        }

        return ReadUnaligned<TFloat>(&integer);
    }

    constexpr ui8 BlockCode = 0x1F;
    constexpr size_t BlockSize = 15;
    constexpr size_t BlockSizeUi64 = BlockSize / 8 + 1;

    template <bool Desc>
    Y_FORCE_INLINE
        void EncodeString(TVector<ui8>& output, TStringBuf value) {
        size_t part = 0;

        while (!value.empty()) {
            union {
                ui8 buffer[BlockSize + 1];
                ui64 buffer64[BlockSizeUi64];
            };

            part = std::min(value.size(), BlockSize);
            if (part == BlockSize) {
                std::memcpy(buffer + 1, value.data(), BlockSize);
            }
            else {
                for (size_t i = 0; i < BlockSizeUi64; ++i) {
                    buffer64[i] = 0;
                }
                std::memcpy(buffer + 1, value.data(), part);
            }
            value.Skip(part);

            buffer[0] = BlockCode;

            if (Desc) {
                for (size_t i = 0; i < BlockSizeUi64; ++i) {
                    buffer64[i] ^= std::numeric_limits<ui64>::max();
                }
            }

            output.insert(output.end(), buffer, buffer + BlockSize + 1);
        }

        auto lastLength = ui8(part);
        output.push_back(Desc ? 0xFF ^ lastLength : lastLength);
    }

    template <bool Desc>
    Y_FORCE_INLINE
        TStringBuf DecodeString(TStringBuf& input, TVector<ui8>& value) {
        EnsureInputSize(input, 1);
        ui8 code = Desc ? 0xFF ^ input[0] : input[0];
        input.Skip(1);

        if (code != BlockCode) {
            MKQL_ENSURE(code == 0, TStringBuilder() << "unknown string block code: " << code);
            return TStringBuf();
        }

        while (code == BlockCode) {
            union {
                ui8 buffer[BlockSize + 1];
                ui64 buffer64[BlockSizeUi64];
            };

            EnsureInputSize(input, BlockSize + 1);
            std::memcpy(buffer, input.data(), BlockSize + 1);
            input.Skip(BlockSize + 1);

            if (Desc) {
                for (size_t i = 0; i < BlockSizeUi64; ++i) {
                    buffer64[i] ^= std::numeric_limits<ui64>::max();
                }
            }

            value.insert(value.end(), buffer, buffer + BlockSize);
            code = buffer[BlockSize];
        }

        auto begin = (const char*)value.begin();
        auto end = (const char*)value.end() - BlockSize + code;
        return TStringBuf(begin, end - begin);
    }
}

} // NMiniKQL
} // NKikimr
