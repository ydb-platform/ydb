#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr::NViewer {

using namespace NActors;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TCheckAccess : public TViewerPipeClient {
    using TThis = TCheckAccess;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;
    TVector<TString> Permissions;

public:
    TCheckAccess(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        ui32 timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        TString database;
        if (params.Has("database")) {
            database = params.Get("database");
        } else {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'database' is required"));
        }
        if (database && database != AppData()->TenantName) {
            BLOG_TRACE("Requesting StateStorageEndpointsLookup for " << database);
            RequestStateStorageEndpointsLookup(database); // to find some dynamic node and redirect query there
        } else {
            if (params.Has("permissions")) {
                Split(params.Get("permissions"), ",", Permissions);
            } else {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'permissions' is required"));
            }
            if (params.Has("path")) {
                RequestSchemeCacheNavigate(params.Get("path"));
            } else {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'path' is required"));
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

    void ReplyAndPassAway() override {
        std::unique_ptr<NACLib::TUserToken> token;
        if (Event->Get()->UserToken) {
            token = std::make_unique<NACLib::TUserToken>(Event->Get()->UserToken);
        }
        if (CacheResult == nullptr) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "no SchemeCache response"));
        }
        if (CacheResult->Request == nullptr) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "wrong SchemeCache response"));
        }
        if (CacheResult->Request.Get()->ResultSet.empty()) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "SchemeCache response is empty"));
        }
        if (CacheResult->Request.Get()->ErrorCount != 0) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", TStringBuilder() << "SchemeCache response error " << static_cast<int>(CacheResult->Request.Get()->ResultSet.front().Status)));
        }


        auto object = CacheResult->Request.Get()->ResultSet.front().SecurityObject;

        NJson::TJsonValue json(NJson::JSON_MAP);

        for (const TString& permission : Permissions) {
            json[permission] = CheckAccessPermission(object.Get(), token.get(), permission);
        }

        ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    void HandleTimeout() {
        ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT("text/plain", "Timeout receiving SchemeCache response"));
    }

    static YAML::Node GetSwagger() {
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
};

}
