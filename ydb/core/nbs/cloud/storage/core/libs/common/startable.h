#pragma once

#include "public.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IStartable
{
    virtual ~IStartable() = default;

    virtual void Start() = 0;
    virtual void Stop() = 0;
};

}   // namespace NYdb::NBS
