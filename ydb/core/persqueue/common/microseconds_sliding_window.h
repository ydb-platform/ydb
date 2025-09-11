#pragma once

#include "util/datetime/base.h"
#include "util/generic/vector.h"

namespace NKikimr::NPQ {

class TMicrosecondsSlidingWindow {
    public:
        TMicrosecondsSlidingWindow(size_t partsNum, TDuration length);
        
        ui32 Update(ui32 val, TInstant t);
        ui32 Update(TInstant t);
        ui32 GetValue();

    private:
        void LocateFirstNotZeroElement();
        void AdjustFirstNonZeroElement();
        void UpdateBucket(size_t index, ui32 newVal);
        void ClearBuckets(ui32 bucketsToClear);
        void UpdateCurrentBucket(ui32 val);
        void AdvanceTime(const TInstant& time);

    private:
        TVector<ui32> Buckets;
        ui32 WindowValue = 0;
        size_t FirstElem = 0;
        i32 FirstNotZeroElem = -1;
        TInstant PeriodStart;
        TDuration Length;
        ui32 MicroSecondsPerBucket;
};


}// NKikimr::NPQ
