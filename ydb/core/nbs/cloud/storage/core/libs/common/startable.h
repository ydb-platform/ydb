#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IStartable
{
    virtual ~IStartable() = default;

    virtual void Start() = 0;
    virtual void Stop() = 0;
};

}   // namespace NCloud
