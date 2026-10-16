#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/uds/client_storage.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IClientStorageFactory
{
    virtual ~IClientStorageFactory() = default;

    virtual NYdb::NBS::NServer::IClientStoragePtr CreateClientStorage(
        IBlockStorePtr service) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientStorageFactoryPtr CreateClientStorageFactoryStub();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NServer
