#pragma once

#include "json_proxy.h"

#include <util/string/split.h>

namespace NKikimr::NCms {

class TJsonProxyConfigUpdates : public TJsonProxyConsole<NConsole::TEvConsole::TEvCheckConfigUpdatesRequest,
                                                         NConsole::TEvConsole::TEvCheckConfigUpdatesResponse>
{
public:
    TJsonProxyConfigUpdates(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyConsole<NConsole::TEvConsole::TEvCheckConfigUpdatesRequest,
                            NConsole::TEvConsole::TEvCheckConfigUpdatesResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        const TCgiParameters& cgi = RequestEvent->Get()->Request.GetParams();

        if (cgi.Has("base")) {
            TVector<TString> ids = StringSplitter(cgi.Get("base")).Split(',').ToList<TString>();
            for (auto &item : ids) {
                TVector<TString> parts = StringSplitter(item).Split('.').ToList<TString>();
                ui64 id = 0;
                ui64 generation = 0;
                if (parts.size() == 2
                    && TryFromString<ui64>(parts[0], id)
                    && TryFromString<ui64>(parts[1], generation)) {
                    auto &rec = *request->Record.AddBaseItemIds();
                    rec.SetId(id);
                    rec.SetGeneration(generation);
                }
            }
        }

        return request;
    }
};

} // namespace NKikimr::NCms
