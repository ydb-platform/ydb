#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonACL : public TViewerPipeClient {
    using TThis = TJsonACL;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;
    TJsonSettings JsonSettings;
    bool MergeRules = false;
    ui32 Timeout = 0;

public:
    TJsonACL(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        TString database;
        if (params.Has("database")) {
            database = params.Get("database");
        }
        bool direct = false;;
        if (params.Has("direct")) {
            direct = FromStringWithDefault<bool>(params.Get("direct"), direct);
        }
        direct |= !TBase::Event->Get()->Request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
        direct |= (database == AppData()->TenantName); // we're already on the right node or don't use database filter
        if (database && !direct) {
            return RedirectToDatabase(database); // to find some dynamic node and redirect query there
        } else {
            if (params.Has("path")) {
                RequestSchemeCacheNavigate(params.Get("path"));
            } else {
                return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'path' is required"));
            }
            MergeRules = FromStringWithDefault<bool>(params.Get("merge_rules"), MergeRules);
        }

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        CacheResult = ev->Release();
        RequestDone();
    }

    static bool Has(ui32 accessRights, ui32 mask) {
        return (accessRights & mask) == mask;
    }

    void FillACE(const NACLibProto::TACE& ace, NKikimrViewer::TMetaCommonInfo::TACE& pbAce) {
        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Deny) {
            pbAce.SetAccessType("Deny");
        }
        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Allow) {
            pbAce.SetAccessType("Allow");
        }

        auto ar = ace.GetAccessRight();

        static std::pair<ui32, TString> accessRules[] = {
            {NACLib::EAccessRights::GenericFull, "Full"},
            {NACLib::EAccessRights::GenericFullLegacy, "FullLegacy"},
            {NACLib::EAccessRights::GenericManage, "Manage"},
            {NACLib::EAccessRights::GenericUse, "Use"},
            {NACLib::EAccessRights::GenericUseLegacy, "UseLegacy"},
            {NACLib::EAccessRights::GenericWrite, "Write"},
            {NACLib::EAccessRights::GenericRead, "Read"},
            {NACLib::EAccessRights::GenericList, "List"},
        };
        if (MergeRules) {
            for (const auto& [rule, name] : accessRules) {
                if (Has(ar, rule)) {
                    pbAce.AddAccessRules(name);
                    ar &= ~rule;
                }
            }
        }

        static std::pair<ui32, TString> accessRights[] = {
            {NACLib::EAccessRights::SelectRow, "SelectRow"},
            {NACLib::EAccessRights::UpdateRow, "UpdateRow"},
            {NACLib::EAccessRights::EraseRow, "EraseRow"},
            {NACLib::EAccessRights::ReadAttributes, "ReadAttributes"},
            {NACLib::EAccessRights::WriteAttributes, "WriteAttributes"},
            {NACLib::EAccessRights::CreateDirectory, "CreateDirectory"},
            {NACLib::EAccessRights::CreateTable, "CreateTable"},
            {NACLib::EAccessRights::CreateQueue, "CreateQueue"},
            {NACLib::EAccessRights::RemoveSchema, "RemoveSchema"},
            {NACLib::EAccessRights::DescribeSchema, "DescribeSchema"},
            {NACLib::EAccessRights::AlterSchema, "AlterSchema"},
            {NACLib::EAccessRights::CreateDatabase, "CreateDatabase"},
            {NACLib::EAccessRights::DropDatabase, "DropDatabase"},
            {NACLib::EAccessRights::GrantAccessRights, "GrantAccessRights"},
            {NACLib::EAccessRights::WriteUserAttributes, "WriteUserAttributes"},
            {NACLib::EAccessRights::ConnectDatabase, "ConnectDatabase"},
            {NACLib::EAccessRights::ReadStream, "ReadStream"},
            {NACLib::EAccessRights::WriteStream, "WriteStream"},
            {NACLib::EAccessRights::ReadTopic, "ReadTopic"},
            {NACLib::EAccessRights::WriteTopic, "WriteTopic"}
        };
        for (const auto& [right, name] : accessRights) {
            if (Has(ar, right)) {
                pbAce.AddAccessRights(name);
                ar &= ~right;
            }
        }

        if (ar != 0) {
            pbAce.AddAccessRights(NACLib::AccessRightsToString(ar));
        }

        pbAce.SetSubject(ace.GetSID());

        auto inht = ace.GetInheritanceType();
        if ((inht & NACLib::EInheritanceType::InheritObject) != 0) {
            pbAce.AddInheritanceType("Object");
        }
        if ((inht & NACLib::EInheritanceType::InheritContainer) != 0) {
            pbAce.AddInheritanceType("Container");
        }
        if ((inht & NACLib::EInheritanceType::InheritOnly) != 0) {
            pbAce.AddInheritanceType("Only");
        }
    }

    void ReplyAndPassAway() override {
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
        const auto& entry = CacheResult->Request.Get()->ResultSet.front();
        NKikimrViewer::TMetaInfo metaInfo;
        NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
        pbCommon.SetPath(CanonizePath(entry.Path));
        if (entry.Self) {
            pbCommon.SetOwner(entry.Self->Info.GetOwner());
            if (entry.Self->Info.HasACL()) {
                NACLib::TACL acl(entry.Self->Info.GetACL());
                for (const NACLibProto::TACE& ace : acl.GetACE()) {
                    auto& pbAce = *pbCommon.AddACL();
                    FillACE(ace, pbAce);
                }
                if (acl.GetInterruptInheritance()) {
                    pbCommon.SetInterruptInheritance(true);
                }
            }
            if (entry.Self->Info.HasEffectiveACL()) {
                NACLib::TACL acl(entry.Self->Info.GetEffectiveACL());
                for (const NACLibProto::TACE& ace : acl.GetACE()) {
                    auto& pbAce = *pbCommon.AddEffectiveACL();
                    FillACE(ace, pbAce);
                }
            }
        }

        TStringStream json;
        TProtoToJson::ProtoToJson(json, metaInfo, JsonSettings);

        ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - viewer
            summary: ACL information
            description: Returns information about ACL of an object
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: path
                in: query
                description: schema path
                required: true
                type: string
              - name: merge_rules
                in: query
                description: merge access rights into access rules
                type: boolean
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
                            schema:
                                type: object
                                properties:
                                    Common:
                                        type: object
                                        properties:
                                            Path:
                                                type: string
                                            Owner:
                                                type: string
                                            ACL:
                                                type: array
                                                items:
                                                    type: object
                                                    properties:
                                                        AccessType:
                                                            type: string
                                                        Subject:
                                                            type: string
                                                        AccessRules:
                                                            type: array
                                                            items:
                                                                type: string
                                                        AccessRights:
                                                            type: array
                                                            items:
                                                                type: string
                                                        InheritanceType:
                                                            type: array
                                                            items:
                                                                type: string
                                            InterruptInheritance:
                                                type: boolean
                                            EffectiveACL:
                                                type: array
                                                items:
                                                    type: object
                                                properties:
                                                    AccessType:
                                                        type: string
                                                    Subject:
                                                        type: string
                                                    AccessRules:
                                                        type: array
                                                        items:
                                                            type: string
                                                    AccessRights:
                                                        type: array
                                                        items:
                                                            type: string
                                                    InheritanceType:
                                                        type: array
                                                        items:
                                                            type: string
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
