#pragma once
#include <util/generic/hash_set.h>
#include <util/string/join.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "appdata.h"

namespace NMVP {

class THandlerActorHttpSensors : public NActors::TActor<THandlerActorHttpSensors> {
public:
    using TBase = NActors::TActor<THandlerActorHttpSensors>;

    THandlerActorHttpSensors()
        : TBase(&THandlerActorHttpSensors::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        TMVPAppData* appData = MVPAppData();
        TStringStream out;
        NMonitoring::IMetricEncoderPtr encoder = NMonitoring::EncoderJson(&out);
        appData->MetricRegistry->Accept(TInstant::Zero(), encoder.Get());
        auto response = event->Get()->Request->CreateResponseOK(out.Str(), "application/json");
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
