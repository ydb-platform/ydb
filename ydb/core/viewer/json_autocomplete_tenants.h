#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "query_autocomplete_helper.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonQueryAutocompleteTenants : public TViewerPipeClient<TJsonQueryAutocompleteTenants> {
    using TBase = TViewerPipeClient<TJsonQueryAutocompleteTenants>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TVector<TString> Tenants;
    TString Prefix;
    ui32 Limit = 10;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonQueryAutocompleteTenants(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Prefix = params.Get("prefix");
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), Limit);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        Tenants.emplace_back("/" + domain->Name);

        RequestConsoleListTenants();
        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            Tenants.emplace_back(path);
        }
        RequestDone();
    }

    void ReplyAndPassAway() {
        NKikimrViewer::TQueryAutocomplete Result;

        auto fuzzy = FuzzySearcher<TString>(Tenants);
        auto autocomplete = fuzzy.Search(Prefix, Limit);
        for (TString& name: autocomplete) {
            Result.MutableResult()->MutableDatabasesResult()->AddEntities()->set_name(name);
        }
        Result.set_success(true);
        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonQueryAutocompleteTenants> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TQueryAutocomplete>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonQueryAutocompleteTenants> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"prefix","in":"query","description":"known part of the word","required":false,"type":"string"},
                      {"name":"limit","in":"query","description":"limit of entities","required":false,"type":"integer"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonQueryAutocompleteTenants> {
    static TString GetSummary() {
        return "\"Tenant info (brief)\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonQueryAutocompleteTenants> {
    static TString GetDescription() {
        return "\"Returns list of tenants\"";
    }
};

}
}
