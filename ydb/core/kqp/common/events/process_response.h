#pragma once
#include <ydb/core/protos/kqp.pb.h>
#include <library/cpp/actors/core/event_pb.h>
#include <util/generic/ptr.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvProcessResponse: public TEventPB<TEvProcessResponse, NKikimrKqp::TEvProcessResponse,
    TKqpEvents::EvProcessResponse> {
    static THolder<TEvProcessResponse> Error(Ydb::StatusIds::StatusCode ydbStatus, const TString& error);
    static THolder<TEvProcessResponse> Success();
};

} // namespace NKikimr::NKqp::NPrivateEvents
