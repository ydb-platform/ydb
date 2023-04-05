#pragma once

#include "json_proxy.h"

namespace NKikimr::NCms {

using namespace NConsole;

class TJsonProxyConsoleLog : public TJsonProxyConsole<TEvConsole::TEvGetLogTailRequest, TEvConsole::TEvGetLogTailResponse> {
public:
    TJsonProxyConsoleLog(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyConsole<TEvConsole::TEvGetLogTailRequest, TEvConsole::TEvGetLogTailResponse>(event)
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

        if (cgi.Has("from-timestamp")) {
            ui64 val = 0;
            if (TryFromString<ui64>(cgi.Get("from-timestamp"), val))
                request->Record.MutableLogFilter()->SetFromTimestamp(val * 1000);
        } else if (cgi.Has("from-id")) {
            ui64 val = 0;
            if (TryFromString<ui64>(cgi.Get("from-id"), val))
                request->Record.MutableLogFilter()->SetFromId(val);
        }

        if (cgi.Has("reverse")) {
            bool reverse = false;
            if (TryFromString<bool>(cgi.Get("reverse"), reverse))
                request->Record.MutableLogFilter()->SetReverse(reverse);
        }

        if (cgi.Has("users")) {
            TVector<TString> users;
            StringSplitter(cgi.Get("users")).Split(',').Collect(&users);
            for (auto& user : users) {
                request->Record.MutableLogFilter()->AddUsers(user);
            }
        } else if (cgi.Has("exclude-users")) {
            TVector<TString> users;
            StringSplitter(cgi.Get("exclude-users")).Split(',').Collect(&users);
            for (auto& user : users) {
                request->Record.MutableLogFilter()->AddExcludeUsers(user);
            }
        }

        if (cgi.Has("affected-kinds")) {
            TVector<TString> affectedStrs;
            StringSplitter(cgi.Get("affected-kinds")).Split(',').Collect(&affectedStrs);
            for (auto& affectedStr : affectedStrs) {
                ui32 kind = 0;
                if (TryFromString<ui32>(affectedStr, kind))
                    request->Record.MutableLogFilter()->AddAffectedKinds(kind);
            }
        }

        return request;
    }
};

} // namespace NKikimr::NCms
