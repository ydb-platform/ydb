#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/iam/operation_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/operation/operation.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvOperationService {
        enum EEv {
            // requests
            EvGetOperationRequest = EventSpaceBegin(TKikimrEvents::ES_OPERATION_SERVICE),

            // replies
            EvGetOperationResponse = EventSpaceBegin(TKikimrEvents::ES_OPERATION_SERVICE) + 1024,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_OPERATION_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_OPERATION_SERVICE)");

        struct TEvGetOperationRequest : TEvGrpcProtoRequest<TEvGetOperationRequest, EvGetOperationRequest, yandex::cloud::priv::iam::v1::GetOperationRequest> {};
        struct TEvGetOperationResponse : TEvGrpcProtoResponse<TEvGetOperationResponse, EvGetOperationResponse, ydb::yc::priv::operation::Operation> {};
    };
}
