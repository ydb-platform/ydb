#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include "wb_aggregate.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonTenants : public TViewerPipeClient {
    using TThis = TJsonTenants;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NKikimrViewer::TTenants Result;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    bool State = true;
    THashMap<TString, NKikimrViewer::TTenant*> TenantIndex;

public:
    TJsonTenants(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        State = FromStringWithDefault<bool>(params.Get("state"), true);
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        NKikimrViewer::TTenant& tenant = *Result.AddTenants();
        tenant.SetName("/" + domain->Name);
        if (State) {
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::State::GetDatabaseStatusResult_State_RUNNING);
        }
        RequestConsoleListTenants();
        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetTenantStatusResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            NKikimrViewer::TTenant& tenant = *Result.AddTenants();
            tenant.SetName(path);
            TenantIndex[path] = &tenant;
            if (State) {
                RequestConsoleGetTenantStatus(path);
            }
        }
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr& ev) {
        Ydb::Cms::GetDatabaseStatusResult getTenantStatusResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&getTenantStatusResult);
        auto itTenant = TenantIndex.find(getTenantStatusResult.path());
        if (itTenant != TenantIndex.end()) {
            NKikimrViewer::TTenant& tenant = *itTenant->second;
            tenant.SetState(getTenantStatusResult.state());
        }
        RequestDone();
    }

    void ReplyAndPassAway() override {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonTenants> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NConsole::TEvConsole::TEvListTenantsResponse::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonTenants> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: state
              in: query
              description: return tenant state
              required: false
              type: boolean
              default: true
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
              )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonTenants> {
    static TString GetSummary() {
        return "Tenant info (brief)";
    }
};

template <>
struct TJsonRequestDescription<TJsonTenants> {
    static TString GetDescription() {
        return "Returns list of tenants";
    }
};

}
}
