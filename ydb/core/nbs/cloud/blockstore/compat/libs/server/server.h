#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/incomplete_requests.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/public.h>

#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/headers.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/public.h>

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NServer {

using NYdb::NBS::ICertificateProviderPtr;

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public NYdb::NBS::IStartable
    , public IIncompleteRequestProvider
{
    virtual IClientStorageFactoryPtr GetClientStorageFactory() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TServerOptions
{
    TString CellId;
};

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

void PrepareRequestHeaders(
    NCloud::NProto::ERequestSource source,
    TStringBuf peer,
    TStringBuf authToken,
    NProto::THeaders& headers);

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerAppConfigPtr config,
    NYdb::NBS::ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IBlockStorePtr udsService,
    TServerOptions options,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NBlockStore::NServer
