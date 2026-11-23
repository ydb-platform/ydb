#pragma once

#include "throttled.h"

class TBufferedThrottledFileOutputStream: public TThrottledOutputStream {
public:
    TBufferedThrottledFileOutputStream(const TString& filename, TThrottle::TOptions option);
    TBufferedThrottledFileOutputStream(const TString& filename, ui32 maxBytesPerSecond, TDuration samplingInterval = TDuration::Seconds(1));
};

