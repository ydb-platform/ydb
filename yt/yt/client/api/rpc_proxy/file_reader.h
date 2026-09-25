#pragma once

#include "api_service_proxy.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> CreateFileReader(
    TApiServiceProxy::TReqReadFilePtr request);

TFuture<IFileReaderPtr> CreateFilePartitionReader(
    TApiServiceProxy::TReqReadFilePartitionPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

