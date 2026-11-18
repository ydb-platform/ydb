#pragma once

#include "kqp_event_ids.h"
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NKqp {

struct TEvKqpExecuterTxRequest : public TEventPB<TEvKqpExecuterTxRequest,
    NKikimrKqp::TEvExecuterTxRequest, TKqpExecuterEvents::EvTxRequest> {};

} // namespace NKikimr::NKqp
