#pragma once

#include <ydb/library/ycloud/api/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

namespace NFq {

template <typename TEv, ui32 TEventType, typename TProtoMessage>
void SetYdbRequestToken(NCloud::TEvGrpcProtoRequest<TEv, TEventType, TProtoMessage>& event, const TString& token) {
    if (token) {
        event.Token = token;
        event.Headers.emplace(NYdb::YDB_AUTH_TICKET_HEADER, token);
    }
}

}  // namespace NFq
