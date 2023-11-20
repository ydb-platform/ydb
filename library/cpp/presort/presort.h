#pragma once

#include <util/generic/bitops.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

#include <cfloat>
#include <cmath>

// TODO: remove when switched to c++11 stl
#if _LIBCPP_STD_VER >= 11
#include <limits>
#else
#define PRESORT_FP_DISABLED
#endif

/*
    Serialization PREServing ORder of Tuples of numbers, strings and optional numbers or strings
    Lexicographic ordering of serialized tuples will be the same as of non-serialized
    Descending order is supported
*/

namespace NPresort {
    namespace NImpl {
        enum ECode {
            StringEnd = 0x00,
            StringPart = 0x10,
            IntNeg = 0x20,
            IntNonNeg = 0x30,
            Unsigned = 0x40,
            Float = 0x50,
            Double = 0x60,
            Extension = 0x70,
            Descending = 0x80,
        };

        static const ui8 CodeMask = 0xf0;
        static const ui8 LengthMask = 0x0f;
        static const ui8 Optional = 0x01;
        static const ui8 OptionalFilled = 0x02;

        enum EFPCode {
            NegInf = 0x00,
            NegFar = 0x01,
            NegNear = 0x02,
            NegSub = 0x03,
            Zero = 0x04,
            PosSub = 0x05,
            PosNear = 0x06,
            PosFar = 0x07,
            PosInf = 0x08,
            Nan = 0x09,
            Disabled = 0x0f
        };

        static const ui8 FPCodeMask = 0x0f;

        static const size_t BlockLength = 15;
        static const size_t BufferLength = BlockLength + 1;

        static const float FloatSignificandBase = pow((float)FLT_RADIX, FLT_MANT_DIG);
        static const double DoubleSignificandBase = pow((double)FLT_RADIX, DBL_MANT_DIG);

        template <typename T>
        struct TSignificandBase {
            static double Value() {
                return DoubleSignificandBase;
            }
        };

        template <>
        struct TSignificandBase<float> {
            static float Value() {
                return FloatSignificandBase;
            }
        };

        struct TDecodeContext {
            ECode Code;
            TString Err;
            TString Str;
            ui32 StrBlocks = 0;
            i64 SignedVal = 0;
            ui64 UnsignedVal = 0;
            float FloatVal = 0;
            double DoubleVal = 0;
            bool Optional = false;
            bool Filled = false;
        };

        class TBlock {
        public:
            TBlock()
                : Len(0)
            {
                memset(Buf, 0, BufferLength);
            }

            void Put(IOutputStream& out) const {
                out.Write(Buf, Len);
            }

            ui8 GetLen() const {
                return Len;
            }

            void EncodeSignedInt(i64 val, bool desc) {
                const bool neg = val < 0;
                const ui8 bytes = val ? EncodeInt(neg ? -val : val) : 0;
                Set(neg ? ((~IntNeg) & CodeMask) : IntNonNeg, bytes, neg != desc);
            }

            void EncodeUnsignedInt(ui64 val, bool desc, bool end = false) {
                const ui8 bytes = val ? EncodeInt(val) : 0;
                Set(end ? StringEnd : Unsigned, bytes, desc);
            }

            bool EncodeFloating(float val, bool desc) {
                const EFPCode code = FPCode(val);
                Set(Float | code, 0, desc);
                return FPNeedEncodeValue(code);
            }

            bool EncodeFloating(double val, bool desc) {
                const EFPCode code = FPCode(val);
                Set(Double | code, 0, desc);
                return FPNeedEncodeValue(code);
            }

            void EncodeString(TStringBuf str, bool desc) {
                memcpy(Buf + 1, str.data(), str.size());
                Set(StringPart, BlockLength, desc);
            }

            void EncodeOptional(bool filled, bool desc) {
                Set(Extension | Optional | (filled ? OptionalFilled : 0), 0, desc);
            }

            bool Decode(TDecodeContext& ctx, TStringBuf str) {
                if (str.empty()) {
                    ctx.Err = "No data";
                    return false;
                }
                Len = 1;
                bool desc = false;
                ui8 byte = str[0];
                ui8 code = byte & CodeMask;
                if (code >= Descending) {
                    desc = true;
                    byte = ~byte;
                    code = byte & CodeMask;
                }
                switch (code) {
                    case StringPart:
                        if (!Init(ctx, str, byte, desc)) {
                            return false;
                        }
                        ctx.Str.append((const char*)Buf + 1, Len - 1);
                        ++ctx.StrBlocks;
                        break;
                    case StringEnd: {
                        if (!Init(ctx, str, byte, desc)) {
                            return false;
                        }
                        const ui64 val = DecodeInt();
                        if (val) {
                            if (!ctx.StrBlocks) {
                                ctx.Err = "Unexpected end of string";
                                return false;
                            }
                            if (val > BlockLength) {
                                ctx.Err = "Invalid string terminal";
                                return false;
                            }
                            ctx.Str.erase(BlockLength * (ctx.StrBlocks - 1) + val);
                        }
                        ctx.StrBlocks = 0;
                        break;
                    }
                    case IntNeg:
                        if (!Init(ctx, str, ~byte, !desc)) {
                            return false;
                        }
                        ctx.SignedVal = -(i64)DecodeInt();
                        break;
                    case IntNonNeg:
                        if (!Init(ctx, str, byte, desc)) {
                            return false;
                        }
                        ctx.SignedVal = DecodeInt();
                        break;
                    case Unsigned:
                        if (!Init(ctx, str, byte, desc)) {
                            return false;
                        }
                        ctx.UnsignedVal = DecodeInt();
                        break;
                    case Float:
                        if (!DecodeFloating((EFPCode)(byte & FPCodeMask), ctx.FloatVal, ctx, str.Skip(Len))) {
                            return false;
                        }
                        break;
                    case Double:
                        if (!DecodeFloating((EFPCode)(byte & FPCodeMask), ctx.DoubleVal, ctx, str.Skip(Len))) {
                            return false;
                        }
                        break;
                    case Extension:
                        ctx.Optional = byte & Optional;
                        ctx.Filled = byte & OptionalFilled;
                        break;
                    default:
                        Y_ABORT("Invalid record code: %d", (int)code);
                }
                ctx.Code = (ECode)code;
                return true;
            }

        private:
            bool Init(TDecodeContext& ctx, TStringBuf str, ui8 byte, bool invert) {
                Len = (byte & LengthMask) + 1;
                if (Len > BufferLength) {
                    ctx.Err = "Invalid block length";
                    return false;
                }
                if (Len > str.size()) {
                    ctx.Err = "Unexpected end of data";
                    return false;
                }
                memcpy(Buf, str.data(), Len);
                if (invert) {
                    Invert();
                }
                return true;
            }

            ui64 DecodeInt() const {
                ui64 val = 0;
                for (ui8 b = 1; b < Len; ++b) {
                    const ui8 shift = Len - b - 1;
                    if (shift < sizeof(ui64)) {
                        val |= ((ui64)Buf[b]) << (8 * shift);
                    }
                }
                return val;
            }

            ui8 EncodeInt(ui64 val) {
                const ui8 bytes = GetValueBitCount(val) / 8 + 1;
                for (ui8 b = 1; b <= bytes; ++b) {
                    const ui8 shift = bytes - b;
                    if (shift < sizeof(ui64)) {
                        Buf[b] = val >> (8 * shift);
                    }
                }
                return bytes;
            }

            static bool FPNeedEncodeValue(EFPCode code) {
                return code != Nan && code != Zero && code != NegInf && code != PosInf && code != Disabled;
            }

            template <typename T>
            static EFPCode FPCode(T val) {
#ifdef PRESORT_FP_DISABLED
                Y_UNUSED(val);
                return Disabled;
#else
                switch (std::fpclassify(val)) {
                    case FP_INFINITE:
                        return val < 0 ? NegInf : PosInf;
                    case FP_NAN:
                        return Nan;
                    case FP_ZERO:
                        return Zero;
                    case FP_SUBNORMAL:
                        return val < 0 ? NegSub : PosSub;
                    case FP_NORMAL:
                        break;
                }
                if (val < 0) {
                    return val > -1 ? NegNear : NegFar;
                }
                return val < 1 ? PosNear : PosFar;
#endif
            }

            template <typename T>
            bool DecodeFloating(EFPCode code, T& val, TDecodeContext& ctx, TStringBuf data) {
#ifdef PRESORT_FP_DISABLED
                Y_UNUSED(code);
                Y_UNUSED(val);
                Y_UNUSED(data);
                ctx.Err = "Floating point numbers support is disabled";
                return false;
#else
                switch (code) {
                    case Zero:
                        val = 0;
                        return true;
                    case NegInf:
                        val = -std::numeric_limits<T>::infinity();
                        return true;
                    case PosInf:
                        val = std::numeric_limits<T>::infinity();
                        return true;
                    case Nan:
                        val = std::numeric_limits<T>::quiet_NaN();
                        return true;
                    case Disabled:
                        ctx.Err = "Floating point numbers support was disabled on encoding";
                        return false;
                    default:
                        break;
                }
                i64 exp = 0;
                i64 sig = 0;
                if (!DecodeFloatingPart(exp, ctx, data) || !DecodeFloatingPart(sig, ctx, data)) {
                    return false;
                }
                val = ldexp(sig / TSignificandBase<T>::Value(), exp);
                return true;
#endif
            }

            bool DecodeFloatingPart(i64& val, TDecodeContext& ctx, TStringBuf& data) {
                TBlock block;
                if (!block.Decode(ctx, data)) {
                    return false;
                }
                if (ctx.Code != IntNeg && ctx.Code != IntNonNeg) {
                    ctx.Err = "Invalid floating part";
                    return false;
                }
                val = ctx.SignedVal;
                ctx.SignedVal = 0;
                data.Skip(block.GetLen());
                Len += block.GetLen();
                return true;
            }

            void Set(ui8 code, ui8 size, bool invert) {
                Y_ASSERT(size <= BlockLength);
                Buf[0] = code | size;
                Len = size + 1;
                if (invert) {
                    Invert();
                }
            }

            void Invert() {
                Y_ASSERT(Len <= BufferLength);
                for (ui8 b = 0; b < Len; ++b) {
                    Buf[b] = ~Buf[b];
                }
            }

        private:
            ui8 Buf[BufferLength];
            ui8 Len;
        };
    }

    inline void EncodeSignedInt(IOutputStream& out, i64 val, bool desc = false) {
        NImpl::TBlock block;
        block.EncodeSignedInt(val, desc);
        block.Put(out);
    }

    inline void EncodeUnsignedInt(IOutputStream& out, ui64 val, bool desc = false) {
        NImpl::TBlock block;
        block.EncodeUnsignedInt(val, desc);
        block.Put(out);
    }

    template <typename T>
    inline void EncodeFloating(IOutputStream& out, T val, bool desc = false) {
        NImpl::TBlock head;
        const bool encodeValue = head.EncodeFloating(val, desc);
        head.Put(out);

        if (encodeValue) {
            int exponent = 0;
            i64 significand = 0;
            significand = frexp(val, &exponent) * NImpl::TSignificandBase<T>::Value();

            NImpl::TBlock exp;
            exp.EncodeSignedInt(exponent, desc);
            exp.Put(out);

            NImpl::TBlock sig;
            sig.EncodeSignedInt(significand, desc);
            sig.Put(out);
        }
    }

    inline void EncodeString(IOutputStream& out, TStringBuf str, bool desc = false) {
        size_t part = 0;
        while (!str.empty()) {
            part = Min(str.size(), NImpl::BlockLength);
            NImpl::TBlock block;
            block.EncodeString(str.Head(part), desc);
            block.Put(out);
            str.Skip(part);
        }
        // Encode string end token
        NImpl::TBlock end;
        end.EncodeUnsignedInt(part, desc, true);
        end.Put(out);
    }

    template <bool Signed>
    struct TEncodeInt {
        static void Do(IOutputStream& out, i64 val, bool desc) {
            EncodeSignedInt(out, val, desc);
        }
    };

    template <>
    struct TEncodeInt<false> {
        static void Do(IOutputStream& out, ui64 val, bool desc) {
            EncodeUnsignedInt(out, val, desc);
        }
    };

    template <typename T, bool Integral>
    struct TEncodeNumber {
        static void Do(IOutputStream& out, const T& val, bool desc) {
            TEncodeInt<std::is_signed<T>::value>::Do(out, val, desc);
        }
    };

    template <typename T>
    struct TEncodeNumber<T, false> {
        static void Do(IOutputStream& out, const T& val, bool desc) {
            EncodeFloating(out, val, desc);
        }
    };

    template <typename T, bool Arithmetic>
    struct TEncodeValue {
        static void Do(IOutputStream& out, const T& val, bool desc) {
            TEncodeNumber<T, std::is_integral<T>::value>::Do(out, val, desc);
        }
    };

    template <typename T>
    struct TEncodeValue<T, false> {
        static void Do(IOutputStream& out, TStringBuf str, bool desc) {
            EncodeString(out, str, desc);
        }
    };

    template <typename T>
    static void Encode(IOutputStream& out, const T& val, bool desc = false) {
        TEncodeValue<T, std::is_arithmetic<T>::value>::Do(out, val, desc);
    }

    template <typename T>
    inline void EncodeOptional(IOutputStream& out, const TMaybe<T>& val, bool desc = false) {
        NImpl::TBlock block;
        block.EncodeOptional(val.Defined(), desc);
        block.Put(out);
        if (val.Defined()) {
            Encode(out, *val, desc);
        }
    }

    template <typename T>
    static void Encode(IOutputStream& out, const TMaybe<T>& val, bool desc = false) {
        EncodeOptional(out, val, desc);
    }

    struct TResultOps {
        void SetError(const TString&) {
            return;
        }
        void SetSignedInt(i64) {
            return;
        }
        void SetUnsignedInt(ui64) {
            return;
        }
        void SetFloat(float) {
            return;
        }
        void SetDouble(double) {
            return;
        }
        void SetString(const TString&) {
            return;
        }
        void SetOptional(bool) {
            return;
        }
    };

    template <typename TResult>
    bool Decode(TResult& res, TStringBuf data) {
        static_assert(std::is_base_of<TResultOps, TResult>::value, "Result must be derived from NPresort::TResultOps");

        using namespace NImpl;

        TDecodeContext ctx;
        while (!data.empty()) {
            TBlock block;
            if (!block.Decode(ctx, data)) {
                res.SetError(ctx.Err);
                return false;
            }
            if (ctx.StrBlocks && ctx.Code != StringPart) {
                res.SetError("Unexpected integer");
                return false;
            }
            switch (ctx.Code) {
                case StringEnd:
                    res.SetString(ctx.Str);
                    ctx.Str = TString();
                    break;
                case IntNeg:
                case IntNonNeg:
                    res.SetSignedInt(ctx.SignedVal);
                    ctx.SignedVal = 0;
                    break;
                case Unsigned:
                    res.SetUnsignedInt(ctx.UnsignedVal);
                    ctx.UnsignedVal = 0;
                    break;
                case Float:
                    res.SetFloat(ctx.FloatVal);
                    ctx.FloatVal = 0;
                    break;
                case Double:
                    res.SetDouble(ctx.DoubleVal);
                    ctx.DoubleVal = 0;
                    break;
                case Extension:
                    if (ctx.Optional) {
                        res.SetOptional(ctx.Filled);
                        ctx.Optional = false;
                        ctx.Filled = false;
                    }
                    break;
                default:
                    break;
            }
            data.Skip(block.GetLen());
        }
        return true;
    }
}
