#pragma once

#include "half_distance_simple.h"

namespace NVectorDistance::NFloat8 {

    enum class EMetric {
        L1,
        L2Sqr,
        Dot,
    };

    template <EMetric Metric, typename T, typename TOps>
    float Distance(const T* lhs, const T* rhs, size_t length) noexcept {
        auto sum0 = TOps::Zero();
        auto sum1 = TOps::Zero();
        auto accumulate = [](auto sum, auto a, auto b) {
            if constexpr (Metric == EMetric::L1) {
                return TOps::Add(sum, TOps::Abs(TOps::Sub(a, b)));
            } else if constexpr (Metric == EMetric::L2Sqr) {
                const auto d = TOps::Sub(a, b);
                return TOps::Add(sum, TOps::Mul(d, d));
            } else {
                return TOps::Add(sum, TOps::Mul(a, b));
            }
        };
        while (length >= 2 * TOps::Width) {
            sum0 = accumulate(sum0, TOps::Load(lhs), TOps::Load(rhs));
            sum1 = accumulate(sum1, TOps::Load(lhs + TOps::Width), TOps::Load(rhs + TOps::Width));
            lhs += 2 * TOps::Width;
            rhs += 2 * TOps::Width;
            length -= 2 * TOps::Width;
        }
        if (length >= TOps::Width) {
            sum0 = accumulate(sum0, TOps::Load(lhs), TOps::Load(rhs));
            lhs += TOps::Width;
            rhs += TOps::Width;
            length -= TOps::Width;
        }
        const float sum = TOps::Sum(TOps::Add(sum0, sum1));
        if constexpr (Metric == EMetric::L1) {
            return sum + NSimple::L1(lhs, rhs, length);
        } else if constexpr (Metric == EMetric::L2Sqr) {
            return sum + NSimple::L2Sqr(lhs, rhs, length);
        } else {
            return sum + NSimple::Dot(lhs, rhs, length);
        }
    }

    template <typename T, typename TOps>
    TTriWayDotProduct<float> TriWay(const T* lhs, const T* rhs, size_t length) noexcept {
        auto ll0 = TOps::Zero();
        auto ll1 = TOps::Zero();
        auto lr0 = TOps::Zero();
        auto lr1 = TOps::Zero();
        auto rr0 = TOps::Zero();
        auto rr1 = TOps::Zero();
        auto accumulate = [](auto& ll, auto& lr, auto& rr, auto a, auto b) {
            ll = TOps::Add(ll, TOps::Mul(a, a));
            lr = TOps::Add(lr, TOps::Mul(a, b));
            rr = TOps::Add(rr, TOps::Mul(b, b));
        };
        while (length >= 2 * TOps::Width) {
            accumulate(ll0, lr0, rr0, TOps::Load(lhs), TOps::Load(rhs));
            accumulate(ll1, lr1, rr1, TOps::Load(lhs + TOps::Width), TOps::Load(rhs + TOps::Width));
            lhs += 2 * TOps::Width;
            rhs += 2 * TOps::Width;
            length -= 2 * TOps::Width;
        }
        if (length >= TOps::Width) {
            accumulate(ll0, lr0, rr0, TOps::Load(lhs), TOps::Load(rhs));
            lhs += TOps::Width;
            rhs += TOps::Width;
            length -= TOps::Width;
        }
        auto result = NSimple::TriWay(lhs, rhs, length);
        result.LL += TOps::Sum(TOps::Add(ll0, ll1));
        result.LR += TOps::Sum(TOps::Add(lr0, lr1));
        result.RR += TOps::Sum(TOps::Add(rr0, rr1));
        return result;
    }

} // namespace NVectorDistance::NFloat8
