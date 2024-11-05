#include "serialize.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(
    IZeroCopyInput* input,
    TRowBufferPtr rowBuffer)
    : NPhoenix2::TLoadContext(input)
    , RowBuffer_(std::move(rowBuffer))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
