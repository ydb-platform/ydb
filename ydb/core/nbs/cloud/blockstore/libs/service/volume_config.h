#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeConfig
{
    const TString DiskId;
    const ui32 BlockSize = 0;
    const ui64 BlockCount = 0;
    const ui64 BlocksPerStripe = 0;
};

using TVolumeConfigPtr = std::shared_ptr<TVolumeConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
