#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/tx/iam_delegation/protos/iam_delegation.pb.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NIamDelegation {

struct TEvIamDelegationTablet {
    enum EEv : ui32 {
        EvRequest = EventSpaceBegin(TKikimrEvents::ES_IAM_DELEGATION_TABLET),
        EvResponse,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_IAM_DELEGATION_TABLET));

    struct TEvRequest : NActors::TEventPB<TEvRequest, NKikimrIamDelegation::TRequest, EvRequest> {};
    struct TEvResponse : NActors::TEventPB<TEvResponse, NKikimrIamDelegation::TResponse, EvResponse> {};
};

} // namespace NKikimr::NIamDelegation
