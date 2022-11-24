#pragma once

#include "json_proxy.h"

namespace NKikimr {
namespace NCms {

class TJsonProxySentinel : public TJsonProxyCms<TEvCms::TEvGetSentinelStateRequest, TEvCms::TEvGetSentinelStateResponse> {
private:

public:
    TJsonProxySentinel(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyCms<TEvCms::TEvGetSentinelStateRequest, TEvCms::TEvGetSentinelStateResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override
    {
        TAutoPtr<TRequest> request = new TRequest;

        return request;
    }
};


} // NCms
} // NKikimr
