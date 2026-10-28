#include "buffered_throttled_file.h"
#include <util/stream/direct_io.h>

TBufferedThrottledFileOutputStream::TBufferedThrottledFileOutputStream(const TString& filename, TThrottle::TOptions option)
    : TThrottledOutputStream(new TBufferedFileOutputEx(filename, CreateAlways | WrOnly | Seq | Direct), option)
{
}

TBufferedThrottledFileOutputStream::TBufferedThrottledFileOutputStream(const TString& filename, ui32 maxBytesPerSecond, TDuration samplingInterval)
    : TThrottledOutputStream(new TBufferedFileOutputEx(filename, CreateAlways | WrOnly | Seq | Direct), maxBytesPerSecond, samplingInterval)
{
}
