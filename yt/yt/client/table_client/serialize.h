#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/phoenix/context.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NPhoenix2::TSaveContext
{
public:
    using NPhoenix2::TSaveContext::TSaveContext;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NPhoenix2::TLoadContext
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
