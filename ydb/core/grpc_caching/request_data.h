#pragma once

#include <util/generic/guid.h>

namespace NKikimr {

    constexpr TDuration DEFAULT_CACHED_GRPC_REQUEST_TIMEOUT = TDuration::Seconds(10);
    const TString REQUEST_ID_METADATA_NAME = "x-request-id";

    struct TRequestIdData {
        TString RequestId;

        NYdbGrpc::TCallMeta FillCallMeta() const {
            NYdbGrpc::TCallMeta callMeta;
            callMeta.Timeout = DEFAULT_CACHED_GRPC_REQUEST_TIMEOUT;
            callMeta.Aux.emplace_back(REQUEST_ID_METADATA_NAME, RequestId);
            return callMeta;
        }

        void FillCachedRequestData(TMaybe<TRequestIdData> cached) {
            if (RequestId.empty()) {
                RequestId = cached.Defined() ? cached->RequestId : CreateGuidAsString();
            }
        }

    };

    struct TTicketData {
        TString Ticket;

        NYdbGrpc::TCallMeta FillCallMeta() const {
            NYdbGrpc::TCallMeta callMeta;
            callMeta.Timeout = DEFAULT_CACHED_GRPC_REQUEST_TIMEOUT;
            callMeta.Aux.emplace_back("authorization", "Bearer " + Ticket);
            return callMeta;
        }

        void FillCachedRequestData(TMaybe<TTicketData> cached) {
            Y_UNUSED(cached);
        }

    };

}