#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/grpc_services/cancelation/protos/event.pb.h>

namespace NKikimr {
namespace NGRpcService {

enum EServiceId {
    EvSubscribeGrpcCancel = EventSpaceBegin(TKikimrEvents::ES_GRPC_CANCELATION),
};

struct TEvSubscribeGrpcCancel : public TEventPB<TEvSubscribeGrpcCancel, NKikimrGRpcService::TEvSubscribeGrpcCancel, EvSubscribeGrpcCancel> {
    TEvSubscribeGrpcCancel() = default;
    TEvSubscribeGrpcCancel(const NActors::TActorId& subscriber, ui64 wakeupTag) {
        ActorIdToProto(subscriber, Record.MutableSubscriber());
        Record.SetWakeupTag(wakeupTag);
    }
};

}
}

