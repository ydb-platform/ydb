#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/iam/user_account_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvUserAccountService {
        enum EEv {
            // requests
            EvGetUserAccountRequest = EventSpaceBegin(TKikimrEvents::ES_USER_ACCOUNT_SERVICE),

            // replies
            EvGetUserAccountResponse = EventSpaceBegin(TKikimrEvents::ES_USER_ACCOUNT_SERVICE) + 512,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_USER_ACCOUNT_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_USER_ACCOUNT_SERVICE)");

        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/identity/proto/iam/v1/user_account.proto
        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/identity/proto/iam/v1/user_account_service.proto

        struct TEvGetUserAccountRequest : TEvGrpcProtoRequest<TEvGetUserAccountRequest, EvGetUserAccountRequest, yandex::cloud::priv::iam::v1::GetUserAccountRequest> {};
        struct TEvGetUserAccountResponse : TEvGrpcProtoResponse<TEvGetUserAccountResponse, EvGetUserAccountResponse, yandex::cloud::priv::iam::v1::UserAccount> {};
    };
}
