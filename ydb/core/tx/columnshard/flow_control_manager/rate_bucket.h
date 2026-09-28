#pragma once

#include <util/datetime/base.h>

namespace NKikimr::NColumnShard::NFlowControl {

// One token bucket plus the CUBIC state that moves its rate, for a single dimension:
// admits/sec for the count bucket, bytes/sec for the bytes bucket.
//
// The two buckets differ only in their units and in the floor used when the configured minimum
// is 0, so keeping the arithmetic here means every cut, decay and growth step is written once
// instead of once per dimension. Everything is expressed against an explicit `now`, so the whole
// class is testable without an actor system.
//
// The CUBIC epoch *origin* is deliberately not stored here: both buckets share one wall-clock
// origin, otherwise the count and bytes rates would recover along independently shifted curves.
// The owner (TDrainRateController) holds it and passes the elapsed time into Grow().
class TRateBucket {
    double Rate = 0.0;
    double Tokens = 0.0;
    double RMin = 0.0;
    double RMax = 0.0;   // 0 => no limit => EffectiveMax() is +inf
    // Last known-good peak and the curvature that returns Rate to it over KTarget seconds.
    double Wmax = 0.0;
    double CubicC = 0.0;
    TInstant LastRefillAt;
    // Floor applied when RMin is 0 (UT-only): a cut must never be able to freeze the queue.
    double ZeroMinFloor = 1.0;

public:
    explicit TRateBucket(double zeroMinFloor)
        : ZeroMinFloor(zeroMinFloor)
    {
    }

    double GetRate() const {
        return Rate;
    }

    double GetTokens() const {
        return Tokens;
    }

    double GetWmax() const {
        return Wmax;
    }

    double GetCubicC() const {
        return CubicC;
    }

    // RMin of 0 (UT-only) keeps a tiny floor; RMax of 0 means no limit, matching DrainRateMax /
    // DrainRateMaxBytes in config.
    double EffectiveMin() const;
    double EffectiveMax() const;

    // Soft cap on accrued tokens: one second of budget, but never below extraFloor. For the bytes
    // bucket the caller passes the FIFO head's batch size, otherwise a single request larger than
    // the rate could never accumulate enough tokens and would stall the queue forever.
    double SoftCap(double extraFloor = 0.0) const;

    // Initial seed: bounds, rate and a full soft cap of tokens, with no CUBIC recovery curve yet.
    void Seed(double rMin, double rMax, double rate);
    // Re-read bounds from config and clamp the live rate back into them.
    void SetBounds(double rMin, double rMax);
    void SetRate(double rate);
    void ResetTokens(double tokens);
    void CapTokens(double cap);

    void RefillTokens(TInstant now, double extraFloor);
    // Forget the accrual interval, so time already spent gated is not credited back as tokens.
    void ResetRefillClock(TInstant now);

    bool CanPay(double amount) const;
    void Pay(double amount);
    void Refund(double amount);

    // Multiplicative change of the rate (cut on overload, decay while hot). Returns the previous
    // rate so the caller can tell whether the change was meaningful.
    double ScaleRate(double factor);
    // Keep Wmax at or below the current rate: the previous peak is known-bad under this pressure.
    void PinWmaxToRate();
    // Pin the peak to the current rate and drop the recovery curve, so the next clean window
    // probes from here rather than climbing back to a peak that caused overload.
    void ResetWmaxToRate();

    // Begin a recovery epoch after a cut: C is derived from the actual drop, so W(0) is the
    // post-cut rate and W(KTarget) is the pre-cut peak even for partial cuts.
    void StartCubicEpoch(double prevRate, double kTarget);
    // No recovery curve yet: treat the current rate as the peak and start in the probe region.
    void EnterProbePhase();

    // One CUBIC step: follow W(t) while inside the recovery window, otherwise lift to Wmax and
    // add probePercent of it. Never exceeds cap. Returns true if the rate actually rose.
    bool Grow(double tSec, double kTarget, double probePercent, double cap);

    // W(t) = C * (t - K)^3 + Wmax — the standard CUBIC window function.
    static double CubicW(double c, double wmax, double tSec, double kTarget);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
