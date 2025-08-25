#pragma once
#include "json_pipe_req.h"
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonACL : public TViewerPipeClient {
    using TThis = TJsonACL;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;
    TRequestResponse<TEvTxUserProxy::TEvProposeTransactionStatus> ProposeStatus;
    TRequestResponse<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult> NotifyTxCompletionResult;
    bool MergeRules = false;

    struct TDialect {
        std::vector<std::pair<ui32, TString>> AccessRules;
        std::vector<std::pair<ui32, TString>> AccessRights;
        std::vector<std::pair<ui32, TString>> InheritanceTypes;
        std::unordered_map<TString, ui32> AccessMap;

        void BuildAccessMap() {
            for (const auto& [mask, name] : AccessRules) {
                AccessMap[name] = mask;
            }
            for (const auto& [mask, name] : AccessRights) {
                AccessMap[name] = mask;
            }
            for (const auto& [mask, name] : InheritanceTypes) {
                AccessMap[name] = mask;
            }
        }
    };

    struct TKikimrDialect : TDialect {
        TKikimrDialect() {
            AccessRules = {
                {NACLib::EAccessRights::GenericFull, "Full"},
                {NACLib::EAccessRights::GenericFullLegacy, "FullLegacy"},
                {NACLib::EAccessRights::GenericManage, "Manage"},
                {NACLib::EAccessRights::GenericUse, "Use"},
                {NACLib::EAccessRights::GenericUseLegacy, "UseLegacy"},
                {NACLib::EAccessRights::GenericWrite, "Write"},
                {NACLib::EAccessRights::GenericRead, "Read"},
                {NACLib::EAccessRights::GenericList, "List"},
            };
            AccessRights = {
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
            InheritanceTypes = {
                {NACLib::EInheritanceType::InheritObject + NACLib::EInheritanceType::InheritContainer, "Inherit"},
                {NACLib::EInheritanceType::InheritObject, "Object"},
                {NACLib::EInheritanceType::InheritContainer, "Container"},
                {NACLib::EInheritanceType::InheritOnly, "Only"},
                {NACLib::EInheritanceType::InheritNone, "None"},
            };
            BuildAccessMap();
        }
    };

    struct TYdbShortDialect : TDialect {
        TYdbShortDialect() {
            AccessRules = {
                {NACLib::EAccessRights::GenericFull, "full"},
                {NACLib::EAccessRights::GenericFullLegacy, "full_legacy"},
                {NACLib::EAccessRights::GenericManage, "manage"},
                {NACLib::EAccessRights::GenericUse, "use"},
                {NACLib::EAccessRights::GenericUseLegacy, "use_legacy"},
                {NACLib::EAccessRights::GenericWrite, "insert"},
                {NACLib::EAccessRights::GenericRead, "select"},
                {NACLib::EAccessRights::GenericList, "list"},
            };
            AccessRights = {
                {NACLib::EAccessRights::SelectRow, "select_row"},
                {NACLib::EAccessRights::UpdateRow, "update_row"},
                {NACLib::EAccessRights::EraseRow, "erase_row"},
                {NACLib::EAccessRights::ReadAttributes, "select_attributes"},
                {NACLib::EAccessRights::WriteAttributes, "modify_attributes"},
                {NACLib::EAccessRights::CreateDirectory, "create_directory"},
                {NACLib::EAccessRights::CreateTable, "create_table"},
                {NACLib::EAccessRights::CreateQueue, "create_queue"},
                {NACLib::EAccessRights::RemoveSchema, "remove_schema"},
                {NACLib::EAccessRights::DescribeSchema, "describe_schema"},
                {NACLib::EAccessRights::AlterSchema, "alter_schema"},
                {NACLib::EAccessRights::CreateDatabase, "create_database"},
                {NACLib::EAccessRights::DropDatabase, "drop_database"},
                {NACLib::EAccessRights::GrantAccessRights, "grant"},
                {NACLib::EAccessRights::ConnectDatabase, "connect"},
            };
            InheritanceTypes = {
                {NACLib::EInheritanceType::InheritObject + NACLib::EInheritanceType::InheritContainer, "inherit"},
                {NACLib::EInheritanceType::InheritObject, "object"},
                {NACLib::EInheritanceType::InheritContainer, "container"},
                {NACLib::EInheritanceType::InheritOnly, "only"},
                {NACLib::EInheritanceType::InheritNone, "none"},
            };
            BuildAccessMap();
            AccessMap["create"] = NACLib::EAccessRights::CreateDatabase;
            AccessMap["drop"] = NACLib::EAccessRights::DropDatabase;
        }
    };

    struct TYdbDialect : TDialect {
        TYdbDialect() {
            AccessRules = {
                {NACLib::EAccessRights::GenericFull, "ydb.generic.full"},
                {NACLib::EAccessRights::GenericFullLegacy, "ydb.generic.full_legacy"},
                {NACLib::EAccessRights::GenericManage, "ydb.generic.manage"},
                {NACLib::EAccessRights::GenericUse, "ydb.generic.use"},
                {NACLib::EAccessRights::GenericUseLegacy, "ydb.generic.use_legacy"},
                {NACLib::EAccessRights::GenericWrite, "ydb.generic.write"},
                {NACLib::EAccessRights::GenericRead, "ydb.generic.read"},
                {NACLib::EAccessRights::GenericList, "ydb.generic.list"},
            };
            AccessRights = {
                {NACLib::EAccessRights::UpdateRow + NACLib::EAccessRights::EraseRow, "ydb.tables.modify"},
                {NACLib::EAccessRights::SelectRow + NACLib::EAccessRights::ReadAttributes, "ydb.tables.read"},
                {NACLib::EAccessRights::SelectRow, "ydb.granular.select_row"},
                {NACLib::EAccessRights::UpdateRow, "ydb.granular.update_row"},
                {NACLib::EAccessRights::EraseRow, "ydb.granular.erase_row"},
                {NACLib::EAccessRights::ReadAttributes, "ydb.granular.read_attributes"},
                {NACLib::EAccessRights::WriteAttributes, "ydb.granular.write_attributes"},
                {NACLib::EAccessRights::CreateDirectory, "ydb.granular.create_directory"},
                {NACLib::EAccessRights::CreateTable, "ydb.granular.create_table"},
                {NACLib::EAccessRights::CreateQueue, "ydb.granular.create_queue"},
                {NACLib::EAccessRights::RemoveSchema, "ydb.granular.remove_schema"},
                {NACLib::EAccessRights::DescribeSchema, "ydb.granular.describe_schema"},
                {NACLib::EAccessRights::AlterSchema, "ydb.granular.alter_schema"},
                {NACLib::EAccessRights::CreateDatabase, "ydb.database.create"},
                {NACLib::EAccessRights::DropDatabase, "ydb.database.drop"},
                {NACLib::EAccessRights::GrantAccessRights, "ydb.access.grant"},
                {NACLib::EAccessRights::ConnectDatabase, "ydb.database.connect"},
            };
            InheritanceTypes = {
                {NACLib::EInheritanceType::InheritObject + NACLib::EInheritanceType::InheritContainer, "inherit"},
                {NACLib::EInheritanceType::InheritObject, "object"},
                {NACLib::EInheritanceType::InheritContainer, "container"},
                {NACLib::EInheritanceType::InheritOnly, "only"},
                {NACLib::EInheritanceType::InheritNone, "none"},
            };
            BuildAccessMap();
        }
    };

    struct TYqlDialect : TDialect {
        TYqlDialect() {
            AccessRules = {
                {NACLib::EAccessRights::GenericFull, "FULL"},
                {NACLib::EAccessRights::GenericFullLegacy, "FULL LEGACY"},
                {NACLib::EAccessRights::GenericManage, "MANAGE"},
                {NACLib::EAccessRights::GenericUse, "USE"},
                {NACLib::EAccessRights::GenericUseLegacy, "USE LEGACY"},
                {NACLib::EAccessRights::GenericWrite, "INSERT"},
                {NACLib::EAccessRights::GenericRead, "SELECT"},
                {NACLib::EAccessRights::GenericList, "LIST"},
            };
            AccessRights = {
                {NACLib::EAccessRights::UpdateRow + NACLib::EAccessRights::EraseRow, "MODIFY TABLES"},
                {NACLib::EAccessRights::SelectRow + NACLib::EAccessRights::ReadAttributes, "SELECT TABLES"},
                {NACLib::EAccessRights::SelectRow, "SELECT ROW"},
                {NACLib::EAccessRights::UpdateRow, "UPDATE ROW"},
                {NACLib::EAccessRights::EraseRow, "ERASE ROW"},
                {NACLib::EAccessRights::CreateDirectory, "CREATE DIRECTORY"},
                {NACLib::EAccessRights::CreateTable, "CREATE TABLE"},
                {NACLib::EAccessRights::CreateQueue, "CREATE QUEUE"},
                {NACLib::EAccessRights::RemoveSchema, "REMOVE SCHEMA"},
                {NACLib::EAccessRights::DescribeSchema, "DESCRIBE SCHEMA"},
                {NACLib::EAccessRights::AlterSchema, "ALTER SCHEMA"},
                {NACLib::EAccessRights::CreateDatabase, "CREATE"},
                {NACLib::EAccessRights::DropDatabase, "DROP"},
                {NACLib::EAccessRights::GrantAccessRights, "GRANT"},
                {NACLib::EAccessRights::ConnectDatabase, "CONNECT"},
            };
            InheritanceTypes = {
                {NACLib::EInheritanceType::InheritObject, "OBJECT"},
                {NACLib::EInheritanceType::InheritContainer, "CONTAINER"},
                {NACLib::EInheritanceType::InheritOnly, "ONLY"},
                {NACLib::EInheritanceType::InheritNone, "NONE"},
            };
            BuildAccessMap();
        }
    };

    static TKikimrDialect KikimrDialect;
    static TYdbShortDialect YdbShortDialect;
    static TYdbDialect YdbDialect;
    static TYqlDialect YqlDialect;
    const TDialect* Dialect = &KikimrDialect;

public:
    TJsonACL(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        MergeRules = FromStringWithDefault<bool>(Params.Get("merge_rules"), MergeRules);
        if (Params.Has("dialect")) {
            static const std::unordered_map<TString, const TDialect*> dialects = {
                {"kikimr", &KikimrDialect},
                {"ydb-short", &YdbShortDialect},
                {"ydb", &YdbDialect},
                {"yql", &YqlDialect},
            };
            const auto& dialect = Params.Get("dialect");
            auto it = dialects.find(dialect);
            if (it != dialects.end()) {
                Dialect = it->second;
            } else {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Unknown dialect"));
            }
        }
        if (FromStringWithDefault<bool>(Params.Get("list_permissions"))) {
            return ReplyWithListAndPassAway();
        } else if (Params.Has("path")) {
            CacheResult = MakeRequestSchemeCacheNavigate(Params.Get("path"));
        } else {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Parameter 'path' is required"));
        }
        Become(&TThis::StateRequestedDescribe, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    NACLibProto::TACE MakeACE(const NJson::TJsonValue& ace) {
        NACLibProto::TACE aceObj;
        if (ace.Has("Subject")) {
            aceObj.SetSID(ace["Subject"].GetStringRobust());
        } else {
            throw yexception() << "Subject is required";
        }
        NACLib::EAccessType accessType = NACLib::EAccessType::Allow;
        if (ace.Has("AccessType")) {
            auto jsonAccessType = ace["AccessType"].GetStringRobust();
            if (jsonAccessType == "Deny") {
                accessType = NACLib::EAccessType::Deny;
            } else if (jsonAccessType == "Allow") {
                accessType = NACLib::EAccessType::Allow;
            } else {
                throw yexception() << "Invalid access type";
            }
        }
        aceObj.SetAccessType(static_cast<ui32>(accessType));
        ui32 accessRights = 0;
        if (ace.Has("AccessRights")) {
            const auto& jsonAccessRights = ace["AccessRights"].GetArraySafe();
            for (const auto& right : jsonAccessRights) {
                auto accessRight = Dialect->AccessMap.find(right.GetStringRobust());
                if (accessRight != Dialect->AccessMap.end()) {
                    accessRights |= accessRight->second;
                } else {
                    throw yexception() << "Invalid access right \"" << right.GetStringRobust() << "\"";
                }
            }
        }
        aceObj.SetAccessRight(accessRights);
        ui32 inheritanceType = NACLib::EInheritanceType::InheritObject + NACLib::EInheritanceType::InheritContainer;
        if (ace.Has("InheritanceType")) {
            const auto& jsonInheritanceType = ace["InheritanceType"].GetArraySafe();
            for (const auto& inherit : jsonInheritanceType) {
                auto inheritance = Dialect->AccessMap.find(inherit.GetStringRobust());
                if (inheritance != Dialect->AccessMap.end()) {
                    inheritanceType |= inheritance->second;
                } else {
                    throw yexception() << "Invalid inheritance type \"" << inherit.GetStringRobust() << "\"";
                }
            }
        }
        aceObj.SetInheritanceType(inheritanceType);
        return aceObj;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (CacheResult.Set(std::move(ev))) {
            if (CacheResult.IsOk()) {
                if (PostData.IsDefined()) {
                    const auto& entry = CacheResult->Request.Get()->ResultSet.front();
                    if (entry.Path.size() >= 1) {
                        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest(new TEvTxUserProxy::TEvProposeTransaction());
                        auto& record(proposeRequest->Record);
                        record.SetUserToken(GetRequest().GetUserTokenObject());
                        record.SetDatabaseName(Database);
                        record.SetPeerName(GetRequest().GetRemoteAddr());
                        //record.SetRequestType();
                        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
                        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
                        auto path = entry.Path;
                        auto name = path.back();
                        path.pop_back();
                        auto workingDir = path.empty() ? "/" : CanonizePath(path);
                        modifyScheme->SetWorkingDir(workingDir);
                        modifyScheme->MutableModifyACL()->SetName(name);
                        NACLib::TDiffACL acl;
                        try {
                            if (PostData.Has("AddAccess")) {
                                for (const auto& ace : PostData["AddAccess"].GetArraySafe()) {
                                    auto aceObj = MakeACE(ace);
                                    acl.AddAccess(aceObj);
                                }
                            }
                            if (PostData.Has("RemoveAccess")) {
                                for (const auto& ace : PostData["RemoveAccess"].GetArraySafe()) {
                                    auto aceObj = MakeACE(ace);
                                    acl.RemoveAccess(aceObj);
                                }
                            }
                            if (PostData.Has("ChangeOwnership")) {
                                auto newOwner = PostData["ChangeOwnership"]["Subject"];
                                if (newOwner.GetType() != NJson::JSON_STRING) {
                                    throw yexception() << "New owner is required";
                                }
                                modifyScheme->MutableModifyACL()->SetNewOwner(newOwner.GetString());
                            }
                        }
                        catch (const std::exception& e) {
                            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", e.what()));
                        }
                        modifyScheme->MutableModifyACL()->SetDiffACL(acl.SerializeAsString());
                        ProposeStatus = MakeRequest<TEvTxUserProxy::TEvProposeTransactionStatus>(MakeTxProxyID(), proposeRequest.release());
                    } else {
                        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Invalid path"));
                    }
                }
            }
            RequestDone();
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        ProposeStatus.Set(std::move(ev));
        if (ProposeStatus.IsOk()) {
            const auto& record(ProposeStatus->Record);
            if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
                ui64 schemeShardTabletId = record.GetSchemeShardTabletId();
                auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(record.GetTxId());
                NotifyTxCompletionResult = MakeRequestToTablet<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(schemeShardTabletId, request.release());
            }
        }
        RequestDone();
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        if (NotifyTxCompletionResult.Set(std::move(ev))) {
            RequestDone();
        }
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
        if (MergeRules) {
            for (const auto& [rule, name] : Dialect->AccessRules) {
                if (Has(ar, rule)) {
                    pbAce.AddAccessRules(name);
                    ar &= ~rule;
                    if (ar == 0) {
                        break;
                    }
                }
            }
        }
        for (const auto& [right, name] : Dialect->AccessRights) {
            if (Has(ar, right)) {
                pbAce.AddAccessRights(name);
                ar &= ~right;
                if (ar == 0) {
                    break;
                }
            }
        }
        if (ar != 0) {
            pbAce.AddAccessRights(std::to_string(ar));
        }
        pbAce.SetSubject(ace.GetSID());
        auto inht = ace.GetInheritanceType();
        for (const auto& [inherit, name] : Dialect->InheritanceTypes) {
            if (Has(inht, inherit)) {
                pbAce.AddInheritanceType(name);
                inht &= ~inherit;
                if (inht == 0) {
                    break;
                }
            }
        }
        if (inht != 0) {
            pbAce.AddInheritanceType(std::to_string(inht));
        }
    }

    static bool IsLegacy(TString name) {
        name.to_lower();
        return name.find("legacy") != TString::npos;
    }

    void ReplyWithListAndPassAway() {
        NJson::TJsonValue jsonRoot(NJson::JSON_MAP);
        NJson::TJsonValue& root = jsonRoot["AvailablePermissions"];
        NJson::TJsonValue& accessRules = root["AccessRules"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (auto itAccessRule = Dialect->AccessRules.begin(); itAccessRule != Dialect->AccessRules.end(); ++itAccessRule) {
            auto [mask, name] = *itAccessRule;
            NJson::TJsonValue& accessRule = accessRules.AppendValue({});
            accessRule["Name"] = name;
            accessRule["Mask"] = mask;
            NJson::TJsonValue& subAccessRules = (accessRule["AccessRules"] = NJson::TJsonValue(NJson::JSON_ARRAY));
            for (auto itSubAccessRule = std::next(itAccessRule); itSubAccessRule != Dialect->AccessRules.end(); ++itSubAccessRule) {
                const auto& [subMask, subName] = *itSubAccessRule;
                if (IsLegacy(subName)) {
                    continue; // skip legacy rules
                }
                if ((mask & subMask) == subMask) {
                    subAccessRules.AppendValue(subName);
                    mask &= ~subMask; // remove submask from mask
                }
            }
            NJson::TJsonValue& subAccessRights = (accessRule["AccessRights"] = NJson::TJsonValue(NJson::JSON_ARRAY));
            for (auto itSubAccessRight = Dialect->AccessRights.begin(); itSubAccessRight != Dialect->AccessRights.end(); ++itSubAccessRight) {
                const auto& [subMask, subName] = *itSubAccessRight;
                if (IsLegacy(subName)) {
                    continue; // skip legacy rights
                }
                if ((mask & subMask) == subMask) {
                    subAccessRights.AppendValue(subName);
                    mask &= ~subMask; // remove submask from mask
                }
            }
        }
        NJson::TJsonValue& accessRights = root["AccessRights"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (const auto& [mask, name] : Dialect->AccessRights) {
            NJson::TJsonValue& accessRight = accessRights.AppendValue({});
            accessRight["Name"] = name;
            accessRight["Mask"] = mask;
        }
        NJson::TJsonValue& inheritanceTypes = root["InheritanceTypes"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (const auto& [mask, name] : Dialect->InheritanceTypes) {
            NJson::TJsonValue& inheritanceType = inheritanceTypes.AppendValue({});
            inheritanceType["Name"] = name;
            inheritanceType["Mask"] = mask;
        }
        ReplyAndPassAway(GetHTTPOKJSON(jsonRoot));
    }

    void ReplyAndPassAway() override {
        NKikimrViewer::TMetaInfo metaInfo;
        if (CacheResult.IsError()) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", CacheResult.GetError()));
        }
        if (ProposeStatus) {
            if (ProposeStatus.IsError()) {
                return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", ProposeStatus.GetError()));
            }
        } else {
            if (CacheResult.IsOk()) {
                const auto& entry = CacheResult->Request.Get()->ResultSet.front();
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
            }
        }
        ReplyAndPassAway(GetHTTPOKJSON(metaInfo));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - viewer
              - acl
            summary: Reads ACL information
            description: Returns access rights of schema object
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: true
              - name: path
                in: query
                description: path to schema object
                required: true
                type: string
              - name: merge_rules
                in: query
                description: merge access rights into access rules
                type: boolean
              - name: dialect
                in: query
                description: dialect to use for access rights
                type: string
                enum:
                  - kikimr
                  - ydb-short
                  - ydb
                  - yql
              - name: list_permissions
                in: query
                description: lists all available permissions in specified dialect
                required: false
                type: boolean
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
        post:
            tags:
              - viewer
              - acl
            summary: Writes ACL information
            description: Updates access rights of schema object
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: true
              - name: path
                in: query
                description: path to schema object
                required: true
                type: string
              - name: merge_rules
                in: query
                description: merge access rights into access rules
                type: boolean
              - name: dialect
                in: query
                description: dialect to use for access rights
                type: string
                enum:
                  - kikimr
                  - ydb-short
                  - ydb
                  - yql
              - name: list_permissions
                in: query
                description: lists all available permissions in specified dialect
                required: false
                type: boolean
            requestBody:
                description: Access rights changes
                required: true
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                AddAccess:
                                    type: array
                                    items:
                                        type: object
                                        properties:
                                            AccessType:
                                                type: string
                                            Subject:
                                                type: string
                                            AccessRights:
                                                type: array
                                                items:
                                                    type: string
                                            InheritanceType:
                                                type: array
                                                items:
                                                    type: string
                                RemoveAccess:
                                    type: array
                                    items:
                                        type: object
                                        properties:
                                            AccessType:
                                                type: string
                                            Subject:
                                                type: string
                                            AccessRights:
                                                type: array
                                                items:
                                                    type: string
                                            InheritanceType:
                                                type: array
                                                items:
                                                    type: string
                                ChangeOwnership:
                                    type: object
                                    properties:
                                        Subject:
                                            type: string
            responses:
                200:
                    description: OK
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
