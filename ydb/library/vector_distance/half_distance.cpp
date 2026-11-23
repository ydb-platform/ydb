#include "half_distance.h"
#include "half_distance_avx2.h"
#include "half_distance_simple.h"
#include "half_distance_sse.h"

#include <library/cpp/sse/sse.h>

#include <util/system/cpu_id.h>

#include <cmath>

namespace NVectorDistance {
    namespace {
        float (*L1Float16Impl)(const TFloat16*, const TFloat16*, size_t) noexcept = &NSimple::L1<TFloat16>;
        float (*L2SqrFloat16Impl)(const TFloat16*, const TFloat16*, size_t) noexcept = &NSimple::L2Sqr<TFloat16>;
        float (*DotFloat16Impl)(const TFloat16*, const TFloat16*, size_t) noexcept = &NSimple::Dot<TFloat16>;
        TTriWayDotProduct<float> (*TriWayFloat16Impl)(const TFloat16*, const TFloat16*, size_t) noexcept = &NSimple::TriWay<TFloat16>;

        float (*L1BFloat16Impl)(const TBFloat16*, const TBFloat16*, size_t) noexcept = &NSimple::L1<TBFloat16>;
        float (*L2SqrBFloat16Impl)(const TBFloat16*, const TBFloat16*, size_t) noexcept = &NSimple::L2Sqr<TBFloat16>;
        float (*DotBFloat16Impl)(const TBFloat16*, const TBFloat16*, size_t) noexcept = &NSimple::Dot<TBFloat16>;
        TTriWayDotProduct<float> (*TriWayBFloat16Impl)(const TBFloat16*, const TBFloat16*, size_t) noexcept = &NSimple::TriWay<TBFloat16>;

        [[maybe_unused]] const int Init = [] {
#ifdef ARCADIA_SSE
            L1Float16Impl = &L1DistanceSse;
            L2SqrFloat16Impl = &L2SqrDistanceSse;
            DotFloat16Impl = &DotProductSse;
            TriWayFloat16Impl = &TriWayDotProductSse;
            L1BFloat16Impl = &L1DistanceSse;
            L2SqrBFloat16Impl = &L2SqrDistanceSse;
            DotBFloat16Impl = &DotProductSse;
            TriWayBFloat16Impl = &TriWayDotProductSse;
#endif
            if (NX86::HaveAVX2()) {
                L1BFloat16Impl = &L1DistanceAvx2;
                L2SqrBFloat16Impl = &L2SqrDistanceAvx2;
                DotBFloat16Impl = &DotProductAvx2;
                TriWayBFloat16Impl = &TriWayDotProductAvx2;
                if (NX86::HaveF16C()) {
                    L1Float16Impl = &L1DistanceAvx2;
                    L2SqrFloat16Impl = &L2SqrDistanceAvx2;
                    DotFloat16Impl = &DotProductAvx2;
                    TriWayFloat16Impl = &TriWayDotProductAvx2;
                }
            }
            return 0;
        }();
    } // namespace

    float L1Distance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
        return L1Float16Impl(lhs, rhs, length);
    }
    float L2SqrDistance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
        return L2SqrFloat16Impl(lhs, rhs, length);
    }
    float L2Distance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
        return std::sqrt(L2SqrDistance(lhs, rhs, length));
    }
    float DotProduct(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
        return DotFloat16Impl(lhs, rhs, length);
    }
    TTriWayDotProduct<float> TriWayDotProduct(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
        return TriWayFloat16Impl(lhs, rhs, length);
    }

    float L1Distance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
        return L1BFloat16Impl(lhs, rhs, length);
    }
    float L2SqrDistance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
        return L2SqrBFloat16Impl(lhs, rhs, length);
    }
    float L2Distance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
        return std::sqrt(L2SqrDistance(lhs, rhs, length));
    }
    float DotProduct(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
        return DotBFloat16Impl(lhs, rhs, length);
    }
    TTriWayDotProduct<float> TriWayDotProduct(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
        return TriWayBFloat16Impl(lhs, rhs, length);
    }
}
