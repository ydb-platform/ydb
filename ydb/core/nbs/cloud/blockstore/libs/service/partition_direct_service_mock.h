#pragma once

#include "partition_direct_service.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectServiceMock: public IPartitionDirectService
{
    TVolumeConfigPtr VolumeConfig;

    [[nodiscard]] TVolumeConfigPtr GetVolumeConfig() const override
    {
        return VolumeConfig;
    }

    NWilson::TSpan CreteRootSpan(TStringBuf name) override
    {
        Y_UNUSED(name);
        return {};
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
