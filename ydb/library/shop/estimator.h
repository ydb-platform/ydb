#pragma once

#include "probes.h"
#include <util/string/builder.h>

namespace NShop {

template <int ReactionNom = 1, int ReactionDenom = 100>
class TMovingAverageEstimator {
    static constexpr double NewFactor = double(ReactionNom) / double(ReactionDenom);
    static constexpr double OldFactor = 1 - NewFactor;
protected:
    double GoalMean;
public:
    explicit TMovingAverageEstimator(double average)
        : GoalMean(average)
    {}

    double GetAverage() const
    {
        return GoalMean;
    }

    void Update(double goal)
    {
        GoalMean = OldFactor * GoalMean + NewFactor * goal;
    }

    TString ToString() const
    {
        return TStringBuilder() << "{ GoalMean:" << GoalMean << "}";
    }
};

template <int ReactionNom = 1, int ReactionDenom = 100>
class TMovingSlrEstimator : public TMovingAverageEstimator<ReactionNom, ReactionDenom> {
protected:
    static constexpr double NewFactor = double(ReactionNom) / double(ReactionDenom);
    static constexpr double OldFactor = 1 - NewFactor;
    double FeatureMean;
    double FeatureSqMean;
    double GoalFeatureMean;
public:
    TMovingSlrEstimator(double goal1, double feature1, double goal2, double feature2)
        : TMovingAverageEstimator<ReactionNom, ReactionDenom>(0.5 * (goal1 + goal2))
        , FeatureMean(0.5 * (feature1 + feature2))
        , FeatureSqMean(0.5 * (feature1 * feature1 + feature2 * feature2))
        , GoalFeatureMean(0.5 * (goal1 * feature1 + goal2 * feature2))
    {}

    double GetEstimation(double feature) const
    {
        return this->GoalMean + GetSlope() * (feature - FeatureMean);
    }

    double GetEstimationAndSlope(double feature, double& slope) const
    {
        slope = GetSlope();
        return this->GoalMean + slope * (feature - FeatureMean);
    }

    double GetSlope() const
    {
        double disp = FeatureSqMean - FeatureMean * FeatureMean;
        if (disp > 1e-10) {
            return (GoalFeatureMean - this->GoalMean * FeatureMean) / disp;
        } else {
            return this->GetAverage();
        }
    }

    using TMovingAverageEstimator<ReactionNom, ReactionDenom>::Update;

    void Update(double goal, double feature)
    {
        Update(goal);
        FeatureMean = OldFactor * FeatureMean + NewFactor * feature;
        FeatureSqMean = OldFactor * FeatureSqMean + NewFactor * feature * feature;
        GoalFeatureMean = OldFactor * GoalFeatureMean + NewFactor * goal * feature;
    }

    TString ToString() const
    {
        return TStringBuilder()
            << "{ GoalMean:" << this->GoalMean
            << ", FeatureMean:" << FeatureMean
            << ", FeatureSqMean:" << FeatureSqMean
            << ", GoalFeatureMean:" << GoalFeatureMean
            << " }";
    }
};

struct TAnomalyFilter {
    // New value is classified as anomaly if it is `MaxRelativeError' times larger or smaller
    float MinRelativeError;
    float MaxRelativeError;

    // Token bucket for anomaly filter (1 token = 1 anomaly allowed)
    float MaxAnomaliesBurst; // Total bucket capacity
    float MaxAnomaliesRate; // Bucket fill rate (number of tokens generated per one Update() calls)

    void Disable()
    {
        MaxAnomaliesBurst = 0.0f;
    }

    TAnomalyFilter(float minRelativeError, float maxRelativeError, float maxAnomaliesBurst, float maxAnomaliesRate)
        : MinRelativeError(minRelativeError)
        , MaxRelativeError(maxRelativeError)
        , MaxAnomaliesBurst(maxAnomaliesBurst)
        , MaxAnomaliesRate(maxAnomaliesRate)
    {}
};

template <int ReactionNom = 1, int ReactionDenom = 100>
class TFilteredMovingSlrEstimator {
protected:
    TMovingSlrEstimator<ReactionNom, ReactionDenom> Estimator;
    float AnomaliesAllowed; // Current token count
public:
    TFilteredMovingSlrEstimator(double goal1, double feature1, double goal2, double feature2, ui64 anomaliesAllowed = 0)
        : Estimator(goal1, feature1, goal2, feature2)
        , AnomaliesAllowed(anomaliesAllowed)
    {}

    double GetAverage() const
    {
        return Estimator.GetAverage();
    }

    double GetEstimation(double feature) const
    {
        return Estimator.GetEstimation(feature);
    }

    double GetEstimationAndSlope(double feature, double& slope) const
    {
        return Estimator.GetEstimationAndSlope(feature, slope);
    }

    double GetSlope() const
    {
        return Estimator.GetSlope();
    }

    void Update(double goal, double estimation, const TAnomalyFilter& filter)
    {
        if (Filter(goal, estimation, filter)) {
            Estimator.Update(goal);
        }
    }

    void Update(double goal, double estimation, double feature, const TAnomalyFilter& filter)
    {
        if (Filter(goal, estimation, filter)) {
            Estimator.Update(goal, feature);
        }
    }

    TString ToString() const
    {
        return TStringBuilder()
            << "{ Estimator:" << Estimator.ToString()
            << ", AnomaliesAllowed:" << AnomaliesAllowed
            << " }";
    }
private:
    bool Filter(double goal, double estimation, const TAnomalyFilter& filter)
    {
        // Shortcut for turned off mode
        if (filter.MaxAnomaliesBurst <= 0.0f || estimation < 1e-3) {
            return true;
        }

        // Fill bucket
        AnomaliesAllowed += filter.MaxAnomaliesRate;
        if (AnomaliesAllowed > filter.MaxAnomaliesBurst) {
            AnomaliesAllowed = filter.MaxAnomaliesBurst;
        }

        // Apply filter
        float error = goal / estimation;
        if ((error < filter.MinRelativeError || filter.MaxRelativeError < error) && AnomaliesAllowed >= 1.0f) {
            AnomaliesAllowed--; // Use 1 token on anomaly that we ignore
            return false;
        } else {
            return true;
        }
    }
};

}
