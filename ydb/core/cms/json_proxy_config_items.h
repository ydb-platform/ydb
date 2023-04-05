#pragma once

#include "json_proxy.h"

#include <util/string/split.h>

namespace NKikimr::NCms {

class TJsonProxyConfigItems : public TJsonProxyConsole<NConsole::TEvConsole::TEvGetConfigItemsRequest,
                                                       NConsole::TEvConsole::TEvGetConfigItemsResponse>
{
public:
    TJsonProxyConfigItems(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyConsole<NConsole::TEvConsole::TEvGetConfigItemsRequest,
                            NConsole::TEvConsole::TEvGetConfigItemsResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        const TCgiParameters& cgi = RequestEvent->Get()->Request.GetParams();

        if (cgi.Has("ids")) {
            TVector<TString> ids = StringSplitter(cgi.Get("ids")).Split(',').ToList<TString>();
            for (auto &idStr : ids) {
                ui64 id = 0;
                if (TryFromString<ui64>(idStr, id))
                    request->Record.AddItemIds(id);
            }
        }

        return request;
    }
};

} // NKikimr::NCms
