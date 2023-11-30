#pragma once
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/event_pb.h>
#include <util/generic/ptr.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvProcessResponse: public TEventPB<TEvProcessResponse, NKikimrKqp::TEvProcessResponse,
    TKqpEvents::EvProcessResponse>
{
};

} // namespace NKikimr::NKqp::NPrivateEvents
