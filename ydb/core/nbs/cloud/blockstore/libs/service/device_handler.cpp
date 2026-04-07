#include "device_handler.h"

#include "aligned_device_handler.h"
#include "unaligned_device_handler.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxZeroBlocksSubRequestSize = 2048_MB;

////////////////////////////////////////////////////////////////////////////////

struct TDefaultDeviceHandlerFactory final: public IDeviceHandlerFactory
{
    const ui32 MaxSubRequestSize;

    explicit TDefaultDeviceHandlerFactory(ui32 maxSubRequestSize)
        : MaxSubRequestSize(maxSubRequestSize)
    {}

    IDeviceHandlerPtr CreateDeviceHandler(TDeviceHandlerParams params) override
    {
        if (params.MaxZeroBlocksSubRequestSize != 0) {
            params.MaxZeroBlocksSubRequestSize = std::min(
                MaxZeroBlocksSubRequestSize,
                params.MaxZeroBlocksSubRequestSize);
        } else {
            params.MaxZeroBlocksSubRequestSize = MaxSubRequestSize;
        }

        if (params.UnalignedRequestsDisabled) {
            return std::make_shared<TAlignedDeviceHandler>(
                std::move(params),
                MaxSubRequestSize);
        }

        return std::make_shared<TUnalignedDeviceHandler>(
            std::move(params),
            MaxSubRequestSize);

        return nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory()
{
    return std::make_shared<TDefaultDeviceHandlerFactory>(MaxSubRequestSize);
}

IDeviceHandlerFactoryPtr CreateDeviceHandlerFactoryForTesting(
    ui32 maxSubRequestSize)
{
    return std::make_shared<TDefaultDeviceHandlerFactory>(maxSubRequestSize);
}

}   // namespace NYdb::NBS::NBlockStore
