#pragma once

#include "client_storage.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/protos/request_source.pb.h>

#include <util/generic/string.h>

namespace NYdb::NBS::NServer {

////////////////////////////////////////////////////////////////////////////////

class TEndpointPoller
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TEndpointPoller();
    ~TEndpointPoller();

    void Start();
    void Stop();

    NYdb::NBS::NProto::TError StartListenEndpoint(
        const TString& unixSocketPath,
        ui32 backlog,
        int accessMode,
        bool multiClient,
        NProto::ERequestSource source,
        IClientStoragePtr clientStorage);

    NYdb::NBS::NProto::TError StopListenEndpoint(const TString& unixSocketPath);
};

}   // namespace NYdb::NBS::NServer
