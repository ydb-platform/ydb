#include <util/datetime/base.h>
#include <ydb/library/kll_median/sketch.h>

#include <deque>

namespace NKikimr::NPQ {

struct TPartitioningKeysManager {
    TPartitioningKeysManager(size_t numSketches, TDuration windowSize)
        : NumSketches(numSketches)
        , WindowSize(windowSize) {
        
    }

    void Add(const TString& /*key*/, ui64 /*msgSize*/) {
        
    }
    
    TString GetQuantile(long double /*phi*/) const {
        return {};
    }

private:
    struct KllSketchWrapper {
        NKll::TKllSketch<TString> Sketch;
        TInstant StartTime;
    };

    std::deque<KllSketchWrapper> Sketches;
    [[maybe_unused]] const ui64 NumSketches;
    const TDuration WindowSize;
};

} // namespace NKikimr::NPQ