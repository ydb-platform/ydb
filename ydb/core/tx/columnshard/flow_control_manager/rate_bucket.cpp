#include "rate_bucket.h"

#include <util/generic/utility.h>

#include <cmath>
#include <limits>

namespace NKikimr::NColumnShard::NFlowControl {

double TRateBucket::EffectiveMin() const {
    return RMin > 0.0 ? RMin : ZeroMinFloor;
}

double TRateBucket::EffectiveMax() const {
    return RMax > 0.0 ? RMax : std::numeric_limits<double>::infinity();
}

double TRateBucket::SoftCap(double extraFloor) const {
    return Max(Max(1.0, std::ceil(Rate)), extraFloor);
}

void TRateBucket::Seed(double rMin, double rMax, double rate) {
    RMin = rMin;
    RMax = rMax;
    Rate = rate;
    // Start in the probe phase at the seed rate: there is no cut to recover from yet.
    Wmax = rate;
    CubicC = 0.0;
    Tokens = SoftCap();
    LastRefillAt = TInstant::Zero();
}

void TRateBucket::SetBounds(double rMin, double rMax) {
    RMin = rMin;
    RMax = rMax;
    Rate = Min(EffectiveMax(), Max(EffectiveMin(), Rate));
}

void TRateBucket::SetRate(double rate) {
    Rate = rate;
}

void TRateBucket::ResetTokens(double tokens) {
    Tokens = tokens;
}

void TRateBucket::CapTokens(double cap) {
    Tokens = Min(Tokens, cap);
}

void TRateBucket::RefillTokens(TInstant now, double extraFloor) {
    if (LastRefillAt == TInstant::Zero()) {
        LastRefillAt = now;
        return;
    }
    const double dt = (now - LastRefillAt).SecondsFloat();
    if (dt <= 0.0) {
        return;
    }
    Tokens = Min(SoftCap(extraFloor), Tokens + Rate * dt);
    LastRefillAt = now;
}

void TRateBucket::ResetRefillClock(TInstant now) {
    LastRefillAt = now;
}

bool TRateBucket::CanPay(double amount) const {
    return Tokens >= amount;
}

void TRateBucket::Pay(double amount) {
    Tokens -= amount;
}

void TRateBucket::Refund(double amount) {
    // No burst cap here: the soft cap in RefillTokens bounds accrual over time, and a refund only
    // returns budget that was already reserved.
    Tokens += amount;
}

double TRateBucket::ScaleRate(double factor) {
    const double prev = Rate;
    Rate = Max(EffectiveMin(), Rate * factor);
    return prev;
}

void TRateBucket::PinWmaxToRate() {
    Wmax = Min(Wmax, Rate);
}

void TRateBucket::ResetWmaxToRate() {
    Wmax = Rate;
    CubicC = 0.0;
}

void TRateBucket::StartCubicEpoch(double prevRate, double kTarget) {
    Wmax = prevRate;
    const double k3 = kTarget * kTarget * kTarget;
    CubicC = k3 > 0.0 ? Max(0.0, Wmax - Rate) / k3 : 0.0;
}

void TRateBucket::EnterProbePhase() {
    Wmax = Max(Wmax, Rate);
    CubicC = 0.0;
}

bool TRateBucket::Grow(double tSec, double kTarget, double probePercent, double cap) {
    if (Rate >= cap) {
        return false;
    }
    const double prev = Rate;
    if (tSec < kTarget && CubicC > 0.0) {
        Rate = Min(cap, Max(EffectiveMin(), CubicW(CubicC, Wmax, tSec, kTarget)));
    } else {
        // Past the recovery window (or no curve at all): lift to Wmax, then probe above it. Wmax
        // itself is not raised here — it stays the last loss peak until the next meaningful cut.
        if (Wmax <= 0.0) {
            Wmax = Rate;
        }
        Rate = Max(Rate, Min(cap, Wmax));
        if (probePercent > 0.0) {
            Rate = Min(cap, Rate + probePercent / 100.0 * Wmax);
        }
    }
    return Rate > prev;
}

double TRateBucket::CubicW(double c, double wmax, double tSec, double kTarget) {
    const double dt = tSec - kTarget;
    return c * dt * dt * dt + wmax;
}

}   // namespace NKikimr::NColumnShard::NFlowControl
