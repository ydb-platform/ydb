#include "serialize.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(
    IZeroCopyInput* input,
    TRowBufferPtr rowBuffer)
    : NPhoenix::TLoadContext(input)
    , RowBuffer_(std::move(rowBuffer))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
