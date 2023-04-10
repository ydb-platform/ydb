#pragma once

#include "json_proxy.h"

namespace NKikimr::NCms {

class TJsonProxyLog : public TJsonProxyCms<TEvCms::TEvGetLogTailRequest, TEvCms::TEvGetLogTailResponse> {
public:
    TJsonProxyLog(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyCms<TEvCms::TEvGetLogTailRequest, TEvCms::TEvGetLogTailResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        const TCgiParameters& cgi = RequestEvent->Get()->Request.GetParams();

        if (cgi.Has("limit")) {
            ui32 limit = 0;
            if (TryFromString<ui32>(cgi.Get("limit"), limit))
                request->Record.MutableLogFilter()->SetLimit(limit);
        }
        if (cgi.Has("from")) {
            ui64 val = 0;
            if (TryFromString<ui64>(cgi.Get("from"), val))
                request->Record.MutableLogFilter()->SetMinTimestamp(val);
        }
        if (cgi.Has("to")) {
            ui64 val = 0;
            if (TryFromString<ui64>(cgi.Get("to"), val))
                request->Record.MutableLogFilter()->SetMaxTimestamp(val);
        }
        if (cgi.Has("detailed")) {
            if (cgi.Get("detailed") == "1")
                request->Record.SetTextFormat(NKikimrCms::TEXT_FORMAT_DETAILED);
        }
        if (cgi.Has("data")) {
            if (cgi.Get("data") == "1")
                request->Record.SetIncludeData(true);
        }

        return request;
    }
};

} // namespace NKikimr::NCms
