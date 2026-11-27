#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/grpc_services/cancelation/protos/event.pb.h>

namespace NKikimr {
namespace NGRpcService {

enum EServiceId {
    EvSubscribeGrpcCancel = EventSpaceBegin(TKikimrEvents::ES_GRPC_CANCELATION),
    EvClientLost
};

struct TEvSubscribeGrpcCancel : public TEventPB<TEvSubscribeGrpcCancel, NKikimrGRpcService::TEvSubscribeGrpcCancel, EvSubscribeGrpcCancel> {
    TEvSubscribeGrpcCancel() = default;
    TEvSubscribeGrpcCancel(const NActors::TActorId& subscriber, ui64 requestId = 0) {
        ActorIdToProto(subscriber, Record.MutableSubscriber());
        Record.SetRequestId(requestId);
    }
};

struct TEvClientLost : public TEventPB<TEvClientLost, NKikimrGRpcService::TEvClientLost, EvClientLost> {
    TEvClientLost() = default;
    explicit TEvClientLost(ui64 requestId) {
        Record.SetRequestId(requestId);
    }
};

}
}

