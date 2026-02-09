#pragma once

#include "public.h"

#include "request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceHandler
{
    virtual ~IDeviceHandler() = default;

    virtual NThreading::TFuture<TReadBlocksLocalResponse> Read(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) = 0;

    virtual NThreading::TFuture<TWriteBlocksLocalResponse> Write(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) = 0;

    virtual NThreading::TFuture<TZeroBlocksLocalResponse>
    Zero(TCallContextPtr callContext, ui64 from, ui64 length) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TDeviceHandlerParams
{
    IStoragePtr Storage;
    TString DiskId;
    TString ClientId;
    ui32 BlockSize = 0;
    ui32 MaxZeroBlocksSubRequestSize = 0;
    bool UnalignedRequestsDisabled = false;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;
};

struct IDeviceHandlerFactory
{
    virtual ~IDeviceHandlerFactory() = default;

    virtual IDeviceHandlerPtr CreateDeviceHandler(
        TDeviceHandlerParams params) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory();
IDeviceHandlerFactoryPtr CreateDeviceHandlerFactoryForTesting(
    ui32 maxSubRequestSize);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
