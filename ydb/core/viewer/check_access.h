#pragma once
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using NSchemeShard::TEvSchemeShard;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TCheckAccess : public TViewerPipeClient<TCheckAccess> {
    using TBase = TViewerPipeClient<TCheckAccess>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;
    TVector<TString> Permissions;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TCheckAccess(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        ui32 timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        TString database;
        if (params.Has("database")) {
            database = params.Get("database");
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'database' is required"));
        }
        if (database && database != AppData()->TenantName) {
            BLOG_TRACE("Requesting StateStorageEndpointsLookup for " << database);
            RequestStateStorageEndpointsLookup(database); // to find some dynamic node and redirect query there
        } else {
            if (params.Has("permissions")) {
                Split(params.Get("permissions"), ",", Permissions);
            } else {
                return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'permissions' is required"));
            }
            if (params.Has("path")) {
                RequestSchemeCacheNavigate(params.Get("path"));
            } else {
                return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'path' is required"));
            }
        }
        Become(&TThis::StateRequestedNavigate, TDuration::MilliSeconds(timeout), new TEvents::TEvWakeup());
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        ReplyAndPassAway(Viewer->MakeForward(Event->Get(), GetNodesFromBoardReply(ev)));
    }

    STATEFN(StateRequestedNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        CacheResult = ev->Release();
        RequestDone();
    }

    ui32 GetAccessType(const TString& permission) {
        TACLAttrs attrs(0);
        try {
            attrs = ConvertYdbPermissionNameToACLAttrs(permission);
        }
        catch (const std::exception&) {
        }
        return attrs.AccessMask;
    }

    bool CheckAccessPermission(const NACLib::TSecurityObject* object, const NACLib::TUserToken* token, const TString& permission) {
        const auto& kikimrRunConfig = Viewer->GetKikimrRunConfig();
        const auto& securityConfig = kikimrRunConfig.AppConfig.GetDomainsConfig().GetSecurityConfig();
        if (!securityConfig.GetEnforceUserTokenRequirement()) {
            if (!securityConfig.GetEnforceUserTokenCheckRequirement() || token == nullptr) {
                return true;
            }
        }
        if (token == nullptr) {
            return false;
        }
        if (object == nullptr) {
            return false;
        }
        ui32 access = GetAccessType(permission);
        if (access == 0) {
            return false;
        }
        return object->CheckAccess(access, *token);
    }

    void ReplyAndPassAway() {
        std::unique_ptr<NACLib::TUserToken> token;
        if (Event->Get()->UserToken) {
            token = std::make_unique<NACLib::TUserToken>(Event->Get()->UserToken);
        }
        if (CacheResult == nullptr) {
            return ReplyAndPassAway(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "no SchemeCache response"));
        }
        if (CacheResult->Request == nullptr) {
            return ReplyAndPassAway(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "wrong SchemeCache response"));
        }
        if (CacheResult->Request.Get()->ResultSet.empty()) {
            return ReplyAndPassAway(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "SchemeCache response is empty"));
        }
        if (CacheResult->Request.Get()->ErrorCount != 0) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", TStringBuilder() << "SchemeCache response error " << static_cast<int>(CacheResult->Request.Get()->ResultSet.front().Status)));
        }


        auto object = CacheResult->Request.Get()->ResultSet.front().SecurityObject;

        NJson::TJsonValue json(NJson::JSON_MAP);

        for (const TString& permission : Permissions) {
            json[permission] = CheckAccessPermission(object.Get(), token.get(), permission);
        }

        ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json, false)));
    }

    void HandleTimeout() {
        ReplyAndPassAway(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), "text/plain", "Timeout receiving SchemeCache response"));
    }

    void ReplyAndPassAway(TString data) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
YAML::Node TJsonRequestSwagger<TCheckAccess>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        get:
          tags:
          - viewer
          summary: Check access
          description: Check access to the specified path
          parameters:
          - name: database
            in: query
            description: database name
            type: string
            required: true
          - name: path
            in: query
            description: path to check access
            type: string
            required: true
          - name: permissions
            in: query
            description: permissions to check
            required: true
            type: array
            items:
              type: string
              enum:
               - ydb.database.connect
               - ydb.tables.modify
               - ydb.tables.read
               - ydb.generic.list
               - ydb.generic.read
               - ydb.generic.write
               - ydb.generic.use_legacy
               - ydb.generic.use
               - ydb.generic.manage
               - ydb.generic.full_legacy
               - ydb.generic.full
               - ydb.database.create
               - ydb.database.drop
               - ydb.access.grant
               - ydb.granular.select_row
               - ydb.granular.update_row
               - ydb.granular.erase_row
               - ydb.granular.read_attributes
               - ydb.granular.write_attributes
               - ydb.granular.create_directory
               - ydb.granular.create_table
               - ydb.granular.create_queue
               - ydb.granular.remove_schema
               - ydb.granular.describe_schema
               - ydb.granular.alter_schema
          - name: timeout
            in: query
            description: timeout in ms
            required: false
            type: integer
          responses:
            200:
              description: OK
              content:
                application/json:
                  schema: {}
            400:
              description: Bad Request
            403:
              description: Forbidden
            504:
              description: Gateway Timeout
        )___");

    return node;
}

}
}

