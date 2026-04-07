#pragma once

#include "api_service_proxy.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IFileWriterPtr CreateFileWriter(TApiServiceProxy::TReqWriteFilePtr request);

////////////////////////////////////////////////////////////////////////////////

IFileFragmentWriterPtr CreateFileFragmentWriter(TApiServiceProxy::TReqWriteFileFragmentPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
