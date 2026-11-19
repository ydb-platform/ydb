#include "float8_distance.h"
#include "float8_distance_avx2.h"
#include "float8_distance_sse.h"
#include "half_distance_simple.h"

#include <library/cpp/sse/sse.h>
#include <util/system/cpu_id.h>

#include <cmath>

namespace NVectorDistance {
    namespace {
        template <typename T>
        struct TDispatch {
            float (*L1)(const T*, const T*, size_t) noexcept = &NSimple::L1<T>;
            float (*L2Sqr)(const T*, const T*, size_t) noexcept = &NSimple::L2Sqr<T>;
            float (*Dot)(const T*, const T*, size_t) noexcept = &NSimple::Dot<T>;
            TTriWayDotProduct<float> (*TriWay)(const T*, const T*, size_t) noexcept = &NSimple::TriWay<T>;
        };

        template <typename T>
        const TDispatch<T>& GetDispatch() {
            static const TDispatch<T> dispatch = [] {
                TDispatch<T> result;
#ifdef ARCADIA_SSE
                result = {&NFloat8::NSse::L1Distance, &NFloat8::NSse::L2SqrDistance,
                    &NFloat8::NSse::DotProduct, &NFloat8::NSse::TriWayDotProduct};
#endif
                if (NX86::HaveAVX2() && NX86::HaveF16C()) {
                    result = {&NFloat8::NAvx2::L1Distance, &NFloat8::NAvx2::L2SqrDistance,
                        &NFloat8::NAvx2::DotProduct, &NFloat8::NAvx2::TriWayDotProduct};
                }
                return result;
            }();
            return dispatch;
        }
    } // namespace

    float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E4M3>().L1(lhs, rhs, length);
    }
    float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E4M3>().L2Sqr(lhs, rhs, length);
    }
    float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E4M3>().Dot(lhs, rhs, length);
    }
    TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E4M3>().TriWay(lhs, rhs, length);
    }
    float L2Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
        return std::sqrt(L2SqrDistance(lhs, rhs, length));
    }

    float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E5M2>().L1(lhs, rhs, length);
    }
    float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E5M2>().L2Sqr(lhs, rhs, length);
    }
    float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E5M2>().Dot(lhs, rhs, length);
    }
    TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
        return GetDispatch<TFloat8E5M2>().TriWay(lhs, rhs, length);
    }
    float L2Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
        return std::sqrt(L2SqrDistance(lhs, rhs, length));
    }
}
