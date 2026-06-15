#pragma once

#include <yt/yt/client/ypath/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TPtr>
void SetReadTableRequestInfo(
    TPtr& target,
    const NYPath::TRichYPath& path,
    const NRpcProxy::NProto::TReqReadTable& req);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define REQUEST_INFO_INL_H_
#include "request_info-inl.h"
#undef REQUEST_INFO_INL_H_
