#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>

#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NPQ {

inline bool BasicCheck(const NKikimrClient::TResponse& response, TString& error, bool mustHaveResponse = true) {
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        error = TStringBuilder() << "Status is not ok"
            << ": status# " << static_cast<ui32>(response.GetStatus());
        return false;
    }

    if (response.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        error = TStringBuilder() << "Error code is not ok"
            << ": code# " << static_cast<ui32>(response.GetErrorCode());
        return false;
    }

    if (mustHaveResponse && !response.HasPartitionResponse()) {
        error = "Absent partition response";
        return false;
    }

    return true;
}


} // namespace NKikimr::NPQ

