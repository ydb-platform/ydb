#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/phoenix.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NPhoenix::TSaveContext
{
public:
    using NPhoenix::TSaveContext::TSaveContext;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NPhoenix::TLoadContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TRowBufferPtr, RowBuffer);

public:
    TLoadContext(
        IZeroCopyInput* input,
        TRowBufferPtr rowBuffer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
