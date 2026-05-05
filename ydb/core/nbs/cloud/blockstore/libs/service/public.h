#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext;
using TCallContextPtr = TIntrusivePtr<TCallContext>;

struct IStorage;
using IStoragePtr = std::shared_ptr<IStorage>;

struct IStorageProvider;
using IStorageProviderPtr = std::shared_ptr<IStorageProvider>;

struct IDeviceHandler;
using IDeviceHandlerPtr = std::shared_ptr<IDeviceHandler>;

struct IDeviceHandlerFactory;
using IDeviceHandlerFactoryPtr = std::shared_ptr<IDeviceHandlerFactory>;

struct IPartitionDirectService;
using IPartitionDirectServicePtr = std::shared_ptr<IPartitionDirectService>;

struct TVolumeConfig;
using TVolumeConfigPtr = std::shared_ptr<TVolumeConfig>;

using TStorageBuffer = std::shared_ptr<char>;

struct TReadBlocksLocalRequest;
struct TReadBlocksLocalResponse;

struct TWriteBlocksLocalRequest;
struct TWriteBlocksLocalResponse;

struct TZeroBlocksLocalRequest;
struct TZeroBlocksLocalResponse;

}   // namespace NYdb::NBS::NBlockStore
