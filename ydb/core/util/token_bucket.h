#pragma once

#include <util/datetime/base.h>

#include <cmath>

namespace NKikimr {

template<typename TTime>
class TTokenBucketBase {
    double Tokens = 0.0; // tokens currenty in bucket
    double Rate = 0.0; // tokens filling rate [tokens/sec]
    double Capacity = 0.0; // maximum amount of tokens allowed in bucket
    TTime LastFill;

public:
    // Create unlimited bucket
    // NOTE: any bucket is created fully filled
    TTokenBucketBase() {
        SetUnlimited();
    }

    // Reset bucket into unlimited mode
    void SetUnlimited() {
        Tokens = std::numeric_limits<double>::infinity(); // tokens currenty in bucket
        Rate = 0.0; // tokens fillig rate [tokens/sec]
        Capacity = std::numeric_limits<double>::infinity(); // maximum amount of tokens allowed in bucket
    }

    bool IsUnlimited() const {
        return Tokens == std::numeric_limits<double>::infinity() &&
            Rate == 0.0 &&
            Capacity == std::numeric_limits<double>::infinity();
    }

    // Reset rate
    void SetRate(double rate) {
        Rate = rate;
    }

    // Reset capacity
    void SetCapacity(double capacity) {
        Capacity = capacity;
        if (Tokens > Capacity) {
            Tokens = Capacity;
        }
    }

    // Fill bucket with tokens, should be done just before Take()
    void Fill(TTime now) {
        // NOTE: LastFill is allowed to be zero, the following code will work OK
        TDuration elapsed = now - LastFill;
        Tokens += elapsed.SecondsFloat() * Rate;
        if (Tokens > Capacity) {
            Tokens = Capacity;
        }
        LastFill = now;
    }

    // Use accumulated tokens
    void Take(double amount) {
        Tokens -= amount;
    }

    // Fill and take if available, returns taken amount
    double FillAndTryTake(TTime now, double amount) {
        Fill(now);
        amount = Min(amount, Tokens);
        Take(amount);
        return amount;
    }

    // don't forget to Fill or use FillAndNextAvailableDelay() instead
    TDuration NextAvailableDelay() const {
        if (IsUnlimited() || Available() >= 0) {
            return TDuration::Zero();
        }

        return TDuration::MicroSeconds(std::ceil(Available() * -1000000.0 / Rate));
    }

    TDuration FillAndNextAvailableDelay(TTime now) {
        Fill(now);
        return NextAvailableDelay();
    }

    // Amount of accumulated tokens
    double Available() const {
        return Tokens;
    }

    double GetRate() const {
        return Rate;
    }

    double GetCapacity() const {
        return Capacity;
    }

    TInstant GetLastFill() const {
        return LastFill;
    }
};

using TTokenBucket = TTokenBucketBase<TInstant>;

}   // namespace NKikimr
