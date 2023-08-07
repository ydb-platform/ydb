#include "stream_output.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

TFixedBufferFileOutput::TFixedBufferFileOutput(
    TFile file,
    size_t bufferSize)
    : Underlying_(bufferSize, file)
{
    Underlying_.SetFinishPropagateMode(true);
}

void TFixedBufferFileOutput::DoWrite(const void* buf, size_t len)
{
    Underlying_.Write(buf, len);
}

void TFixedBufferFileOutput::DoFlush()
{
    Underlying_.Flush();
}

void TFixedBufferFileOutput::DoFinish()
{
    Underlying_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
