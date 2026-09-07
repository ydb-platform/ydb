#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext;
using TCallContextPtr = TIntrusivePtr<TCallContext>;

struct IBlockStore;
using IBlockStorePtr = std::shared_ptr<IBlockStore>;

struct ISwitchableBlockStore;
using ISwitchableBlockStorePtr = std::shared_ptr<ISwitchableBlockStore>;

struct ISessionSwitcher;
using ISessionSwitcherWeakPtr = std::weak_ptr<ISessionSwitcher>;

struct IAuthProvider;
using IAuthProviderPtr = std::shared_ptr<IAuthProvider>;

struct IStorage;
using IStoragePtr = std::shared_ptr<IStorage>;

struct IStorageProvider;
using IStorageProviderPtr = std::shared_ptr<IStorageProvider>;

struct IDeviceHandler;
using IDeviceHandlerPtr = std::shared_ptr<IDeviceHandler>;

struct IDeviceHandlerFactory;
using IDeviceHandlerFactoryPtr = std::shared_ptr<IDeviceHandlerFactory>;

using TStorageBuffer = std::shared_ptr<char>;

}   // namespace NCloud::NBlockStore
