#pragma once

#include "public.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDumpable
{
    virtual ~IDumpable() = default;

    virtual void Dump(IOutputStream& out) const = 0;
    virtual void DumpHtml(IOutputStream& out) const = 0;
};

}   // namespace NYdb::NBS::NBlockStore
