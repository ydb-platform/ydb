#pragma once

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

// Manages time-windowed sparkline history with fixed interval collection
// - Adds new data points at fixed intervals (default 5 seconds)
// - Updates the latest point on faster refreshes (no duplicate points)
// - Uses window-based max for scaling (rescales when peaks leave window)
class TSparklineHistory {
public:
    explicit TSparklineHistory(size_t maxPoints = 60, TDuration interval = TDuration::Seconds(5))
        : LastAddTime_(TInstant::Zero())
        , MaxPoints_(maxPoints)
        , Interval_(interval)
    {}
    
    // Update with new value
    // - If interval elapsed since last add: append new point
    // - Otherwise: update the latest point (in-place refresh)
    void Update(double value) {
        auto now = TInstant::Now();
        
        if (Values_.empty()) {
            // First value
            Values_.push_back(value);
            LastAddTime_ = now;
        } else if (now - LastAddTime_ >= Interval_) {
            // Interval elapsed - add new point
            Values_.push_back(value);
            LastAddTime_ = now;
            
            // Trim to max size
            while (Values_.size() > MaxPoints_) {
                Values_.erase(Values_.begin());
            }
        } else {
            // Update latest point (faster refresh)
            Values_.back() = value;
        }
    }
    
    // Get values for rendering
    const TVector<double>& GetValues() const {
        return Values_;
    }
    
    // Get max from visible window for proper scaling
    // Recalculates each time, so scale adjusts when peaks leave the window
    double GetMax() const {
        if (Values_.empty()) {
            return 1.0;
        }
        double maxVal = 0.0;
        for (double v : Values_) {
            if (v > maxVal) maxVal = v;
        }
        return maxVal > 0.0 ? maxVal : 1.0;
    }
    
    // Check if empty
    bool Empty() const {
        return Values_.empty();
    }
    
    // Clear history
    void Clear() {
        Values_.clear();
        LastAddTime_ = TInstant::Zero();
    }
    
private:
    TVector<double> Values_;
    TInstant LastAddTime_;
    size_t MaxPoints_;
    TDuration Interval_;
};

} // namespace NYdb::NConsoleClient
