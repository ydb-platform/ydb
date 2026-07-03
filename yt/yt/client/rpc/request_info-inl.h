#ifndef REQUEST_INFO_INL_H_
#error "Direct inclusion of this file is not allowed, include request_info.h"
// For the sake of sane code completion.
#include "request_info.h"
#endif

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TPtr>
void SetReadTableRequestInfo(
    TPtr& target,
    const NYPath::TRichYPath& path,
    const NRpcProxy::NProto::TReqReadTable& req)
{
    target->SetRequestInfo(
        "Path: %v, Unordered: %v, OmitInaccessibleColumns: %v, OmitInaccessibleRows: %v, "
        "DesiredRowsetFormat: %v, ArrowFallbackRowsetFormat: %v",
        path,
        req.unordered(),
        req.omit_inaccessible_columns(),
        req.omit_inaccessible_rows(),
        NApi::NRpcProxy::NProto::ERowsetFormat_Name(req.desired_rowset_format()),
        NApi::NRpcProxy::NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
