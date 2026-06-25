#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NKqp::NEvKqpExecuter {

struct TEvTxRequest : public NActors::TEventPB<TEvTxRequest, NKikimrKqp::TEvExecuterTxRequest,
    TKqpExecuterEvents::EvTxRequest> {};

} // namespace NKikimr::NKqp::NEvKqpExecuter
