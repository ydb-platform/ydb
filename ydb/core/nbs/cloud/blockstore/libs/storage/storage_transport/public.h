#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

class IStorageTransport;
using TStorageTransportPtr = std::shared_ptr<IStorageTransport>;

class IChaosInjectorControl;
using IChaosInjectorControlPtr = std::shared_ptr<IChaosInjectorControl>;

class ITransportWithChaosInjectorControl;
using TTransportWithChaosInjectorControlPtr =
    std::shared_ptr<ITransportWithChaosInjectorControl>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
