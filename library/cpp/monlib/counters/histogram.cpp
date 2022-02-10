#include "histogram.h"

namespace NMonitoring {
    void THistogramSnapshot::Print(IOutputStream* out) const {
        (*out) << "mean: " << Mean
               << ", stddev: " << StdDeviation
               << ", min: " << Min
               << ", max: " << Max
               << ", 50%: " << Percentile50
               << ", 75%: " << Percentile75
               << ", 90%: " << Percentile90
               << ", 95%: " << Percentile95
               << ", 98%: " << Percentile98
               << ", 99%: " << Percentile99
               << ", 99.9%: " << Percentile999 
               << ", count: " << TotalCount; 
    }

    void THdrHistogram::TakeSnaphot(THistogramSnapshot* snapshot) {
        with_lock (Lock_) {
            // TODO: get data with single traverse
            snapshot->Mean = Data_.GetMean();
            snapshot->StdDeviation = Data_.GetStdDeviation();
            snapshot->Min = Data_.GetMin();
            snapshot->Max = Data_.GetMax();
            snapshot->Percentile50 = Data_.GetValueAtPercentile(50.0);
            snapshot->Percentile75 = Data_.GetValueAtPercentile(75.0);
            snapshot->Percentile90 = Data_.GetValueAtPercentile(90.0);
            snapshot->Percentile95 = Data_.GetValueAtPercentile(95.0);
            snapshot->Percentile98 = Data_.GetValueAtPercentile(98.0);
            snapshot->Percentile99 = Data_.GetValueAtPercentile(99.0);
            snapshot->Percentile999 = Data_.GetValueAtPercentile(99.9);
            snapshot->TotalCount = Data_.GetTotalCount(); 

            // cleanup histogram data
            Data_.Reset();
        }
    }

}
