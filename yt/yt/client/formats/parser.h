#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct IParser
    : public TNonCopyable
{
    virtual ~IParser()
    { }

    virtual void Read(TStringBuf data) = 0;
    virtual void Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void Parse(IInputStream* input, IParser* parser);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
