#pragma once

#include "knn-defines.h"

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>
#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/stream/format.h>

#include <bit>

namespace {

    template <typename TTo>
    inline TArrayRef<const TTo> GetArray(const TStringBuf& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (Y_UNLIKELY(len % sizeof(TTo) != 0)) {
            return {};
        }

        const ui32 count = len / sizeof(TTo);

        return {reinterpret_cast<const TTo*>(buf), count};
    }

    struct TBitArray {
        const ui64* data = nullptr;
        ui64 bitLen = 0;
    };

    inline TBitArray GetBitArray(const TStringBuf& str) {
        if (Y_UNLIKELY(str.Size() < 2)) {
            return {};
        }
        const char* buf = str.Data();
        const ui64 len = 8 * (str.Size() - HeaderLen - 1) - static_cast<ui8>(buf[str.Size() - HeaderLen - 1]);
        return {reinterpret_cast<const ui64*>(buf), len};
    }

} // namespace

template <typename TRes = float>
class KnnDistance {
public:
    using TDistanceResult = std::optional<TRes>;

private:
    static void BitVectorHandleShort(ui64 byteLen, const ui64* v1, const ui64* v2, auto&& op) {
        Y_ASSERT(0 < byteLen);
        Y_ASSERT(byteLen < sizeof(ui64));
        ui64 d1 = 0;
        ui64 d2 = 0;
        // TODO manual switch for [1..7]?
        std::memcpy(&d1, v1, byteLen);
        std::memcpy(&d2, v2, byteLen);
        op(d1, d2);
    }

    static void BitVectorHandleTail(ui64 byteLen, const ui64* v1, const ui64* v2, auto&& op) {
        if (Y_LIKELY(byteLen == 0)) { // fast-path for aligned case
            return;
        }
        Y_ASSERT(byteLen < sizeof(ui64));
        const auto unneededBytes = sizeof(ui64) - byteLen;
        const auto* r1 = reinterpret_cast<const char*>(v1) - unneededBytes;
        const auto* r2 = reinterpret_cast<const char*>(v2) - unneededBytes;
        ui64 d1, d2; // unaligned loads
        std::memcpy(&d1, r1, sizeof(ui64));
        std::memcpy(&d2, r2, sizeof(ui64));
        ui64 mask = 0;
        // big    endian: 0 1 2 3 4 5 6 7 | 0 1 2 3 | 0 1 | 0 | 0 => needs to zero high bits
        // little endian: 7 6 5 4 3 2 1 0 | 3 2 1 0 | 1 0 | 0 | 0 => needs to zero low  bits
        static_assert(std::endian::native == std::endian::little, "BitVectors only support little endian");
        mask = ~((ui64{1} << (unneededBytes * 8)) - 1);
        op(d1 & mask, d2 & mask);
    }

    static void BitVectorHandleOp(ui64 bitLen, const ui64* v1, const ui64* v2, auto&& op) {
        if (Y_UNLIKELY(bitLen == 0)) {
            return;
        }
        auto byteLen = (bitLen + 7) / 8;
        const auto wordLen = byteLen / sizeof(ui64);
        if (Y_LIKELY(wordLen == 0)) { // fast-path for short case
            return BitVectorHandleShort(byteLen, v1, v2, op);
        }
        byteLen %= sizeof(ui64);
        for (const auto* end = v1 + wordLen; v1 != end; ++v1, ++v2) {
            op(*v1, *v2);
        }
        BitVectorHandleTail(byteLen, v1, v2, op);
    }

    template <typename Func>
    static TDistanceResult VectorFuncImpl(const auto* v1, const auto* v2, auto len1, auto len2, Func&& func) {
        if (Y_UNLIKELY(len1 != len2)) {
            return {};
        }
        return {func(v1, v2, len1)};
    }

    template <typename T, typename Func>
    static auto VectorFunc(const TStringBuf& str1, const TStringBuf& str2, Func&& func) {
        const TArrayRef<const T> v1 = GetArray<T>(str1);
        const TArrayRef<const T> v2 = GetArray<T>(str2);
        return VectorFuncImpl(v1.data(), v2.data(), v1.size(), v2.size(), std::forward<Func>(func));
    }

    template <typename Func>
    static auto BitVectorFunc(const TStringBuf& str1, const TStringBuf& str2, Func&& func) {
        auto [v1, bitLen1] = GetBitArray(str1);
        auto [v2, bitLen2] = GetBitArray(str2);
        return VectorFuncImpl(v1, v2, bitLen1, bitLen2, std::forward<Func>(func));
    }

public:
    static TDistanceResult ManhattanDistance(const TStringBuf& str1, const TStringBuf& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];
        if (Y_UNLIKELY(format1 != format2)) {
            return {};
        }

        switch (format1) {
            case EFormat::FloatVector:
                return VectorFunc<float>(str1, str2, [](const float* v1, const float* v2, size_t len) {
                    return ::L1Distance(v1, v2, len);
                });
            case EFormat::Int8Vector:
                return VectorFunc<i8>(str1, str2, [](const i8* v1, const i8* v2, size_t len) {
                    return ::L1Distance(v1, v2, len);
                });
            case EFormat::Uint8Vector:
                return VectorFunc<ui8>(str1, str2, [](const ui8* v1, const ui8* v2, size_t len) {
                    return ::L1Distance(v1, v2, len);
                });
            case EFormat::BitVector:
                return BitVectorFunc(str1, str2, [](const ui64* v1, const ui64* v2, ui64 bitLen) {
                    ui64 ret = 0;
                    BitVectorHandleOp(bitLen, v1, v2, [&](ui64 d1, ui64 d2) {
                        ret += std::popcount(d1 ^ d2);
                    });
                    return ret;
                });
            default:
                return {};
        }
    }

    static TDistanceResult EuclideanDistance(const TStringBuf& str1, const TStringBuf& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];
        if (Y_UNLIKELY(format1 != format2)) {
            return {};
        }

        switch (format1) {
            case EFormat::FloatVector:
                return VectorFunc<float>(str1, str2, [](const float* v1, const float* v2, size_t len) {
                    return ::L2Distance(v1, v2, len);
                });
            case EFormat::Int8Vector:
                return VectorFunc<i8>(str1, str2, [](const i8* v1, const i8* v2, size_t len) {
                    return ::L2Distance(v1, v2, len);
                });
            case EFormat::Uint8Vector:
                return VectorFunc<ui8>(str1, str2, [](const ui8* v1, const ui8* v2, size_t len) {
                    return ::L2Distance(v1, v2, len);
                });
            case EFormat::BitVector:
                return BitVectorFunc(str1, str2, [](const ui64* v1, const ui64* v2, ui64 bitLen) {
                    ui64 ret = 0;
                    BitVectorHandleOp(bitLen, v1, v2, [&](ui64 d1, ui64 d2) {
                        ret += std::popcount(d1 ^ d2);
                    });
                    return NPrivate::NL2Distance::L2DistanceSqrt(ret);
                });
            default:
                return {};
        }
    }

    static TDistanceResult DotProduct(const TStringBuf& str1, const TStringBuf& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];
        if (Y_UNLIKELY(format1 != format2)) {
            return {};
        }

        switch (format1) {
            case EFormat::FloatVector:
                return VectorFunc<float>(str1, str2, [](const float* v1, const float* v2, size_t len) {
                    return ::DotProduct(v1, v2, len);
                });
            case EFormat::Int8Vector:
                return VectorFunc<i8>(str1, str2, [](const i8* v1, const i8* v2, size_t len) {
                    return ::DotProduct(v1, v2, len);
                });
            case EFormat::Uint8Vector:
                return VectorFunc<ui8>(str1, str2, [](const ui8* v1, const ui8* v2, size_t len) {
                    return ::DotProduct(v1, v2, len);
                });
            case EFormat::BitVector:
                return BitVectorFunc(str1, str2, [](const ui64* v1, const ui64* v2, ui64 bitLen) {
                    ui64 ret = 0;
                    BitVectorHandleOp(bitLen, v1, v2, [&](ui64 d1, ui64 d2) {
                        ret += std::popcount(d1 & d2);
                    });
                    return ret;
                });
            default:
                return {};
        }
    }

    static TDistanceResult CosineSimilarity(const TStringBuf& str1, const TStringBuf& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];
        if (Y_UNLIKELY(format1 != format2)) {
            return {};
        }

        auto compute = [](TRes ll, TRes lr, TRes rr) {
            const auto norm = std::sqrt(ll * rr);
            const TRes cosine = norm != 0 ? lr / static_cast<TRes>(norm) : 1;
            return cosine;
        };

        switch (format1) {
            case EFormat::FloatVector:
                return VectorFunc<float>(str1, str2, [&](const float* v1, const float* v2, size_t len) {
                    const auto res = ::TriWayDotProduct(v1, v2, len);
                    return compute(res.LL, res.LR, res.RR);
                });
            case EFormat::Int8Vector:
                return VectorFunc<i8>(str1, str2, [&](const i8* v1, const i8* v2, size_t len) {
                    // TODO We can optimize it if we will iterate over both vector at the same time, look to the float implementation
                    const i64 ll = ::DotProduct(v1, v1, len);
                    const i64 lr = ::DotProduct(v1, v2, len);
                    const i64 rr = ::DotProduct(v2, v2, len);
                    return compute(ll, lr, rr);
                });
            case EFormat::Uint8Vector:
                return VectorFunc<ui8>(str1, str2, [&](const ui8* v1, const ui8* v2, size_t len) {
                    // TODO We can optimize it if we will iterate over both vector at the same time, look to the float implementation
                    const ui64 ll = ::DotProduct(v1, v1, len);
                    const ui64 lr = ::DotProduct(v1, v2, len);
                    const ui64 rr = ::DotProduct(v2, v2, len);
                    return compute(ll, lr, rr);
                });
            case EFormat::BitVector:
                return BitVectorFunc(str1, str2, [&](const ui64* v1, const ui64* v2, ui64 bitLen) {
                    ui64 ll = 0;
                    ui64 rr = 0;
                    ui64 lr = 0;
                    BitVectorHandleOp(bitLen, v1, v2, [&](ui64 d1, ui64 d2) {
                        ll += std::popcount(d1);
                        rr += std::popcount(d2);
                        lr += std::popcount(d1 & d2);
                    });
                    return compute(ll, lr, rr);
                });
            default:
                return {};
        }
    }
};
