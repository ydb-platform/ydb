#pragma once

#include "public.h"

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct IRequestExecutor
{
    virtual ~IRequestExecutor() = default;

    virtual void Run() = 0;
    virtual TString Print() = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
