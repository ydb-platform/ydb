#pragma once

#include "json_proxy.h"

#include <util/string/split.h>

namespace NKikimr::NCms {

class TJsonProxyToggleConfigValidator : public TJsonProxyConsole<NConsole::TEvConsole::TEvToggleConfigValidatorRequest,
                                                                 NConsole::TEvConsole::TEvToggleConfigValidatorResponse>
{
public:
    TJsonProxyToggleConfigValidator(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyConsole<NConsole::TEvConsole::TEvToggleConfigValidatorRequest,
                            NConsole::TEvConsole::TEvToggleConfigValidatorResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        const TCgiParameters& cgi = RequestEvent->Get()->Request.GetParams();

        if (!cgi.Has("name"))
            return nullptr;

        request->Record.SetName(cgi.Get("name"));
        if (cgi.Has("enable"))
            request->Record.SetDisable(cgi.Get("enable") == "0");
        if (cgi.Has("disable"))
            request->Record.SetDisable(cgi.Get("disable") != "0");

        return request;
    }
};

} // namespace NKikimr::NCms
