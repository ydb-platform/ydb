#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include <ydb/core/external_sources/external_source_factory.h>

namespace NKikimr::NViewer {

using namespace NActors;
namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TJsonDescribe : public TViewerPipeClient {
    using TThis = TJsonDescribe;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult> SchemeShardResult;
    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;
    bool ExpandSubElements = true;

    enum class EAskSchemeCache {
        First,
        Second,
        Never,
        Only,
    };

    EAskSchemeCache AskSchemeCache = EAskSchemeCache::Second;

public:
    TJsonDescribe(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    TTabletId GetSchemeShardId() {
        if (Params.Has("schemeshard_id")) {
            return FromStringWithDefault<TTabletId>(Params.Get("schemeshard_id"));
        }
        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            auto self = DatabaseNavigateResponse->Get()->Request->ResultSet.front().Self;
            if (self && self->Info.GetSchemeshardId()) {
                return self->Info.GetSchemeshardId();
            }
        }
        // i'm not sure we actually need this branch
        if (ResourceNavigateResponse && ResourceNavigateResponse->IsOk()) {
            auto self = ResourceNavigateResponse->Get()->Request->ResultSet.front().Self;
            if (self && self->Info.GetSchemeshardId()) {
                return self->Info.GetSchemeshardId();
            }
        }
        return {};
    }

    void FillParams(NKikimrSchemeOp::TDescribePath& record) {
        if (Params.Has("path")) {
            record.SetPath(Params.Get("path"));
        }
        if (Params.Has("path_id")) {
            record.SetPathId(FromStringWithDefault<ui64>(Params.Get("path_id")));
            record.SetSchemeshardId(GetSchemeShardId());
        }
        record.MutableOptions()->SetBackupInfo(FromStringWithDefault<bool>(Params.Get("backup"), true));
        record.MutableOptions()->SetShowPrivateTable(FromStringWithDefault<bool>(Params.Get("private"), true));
        record.MutableOptions()->SetReturnChildren(FromStringWithDefault<bool>(Params.Get("children"), true));
        record.MutableOptions()->SetReturnBoundaries(FromStringWithDefault<bool>(Params.Get("boundaries"), false));
        record.MutableOptions()->SetReturnPartitionConfig(FromStringWithDefault<bool>(Params.Get("partition_config"), true));
        record.MutableOptions()->SetReturnPartitionStats(FromStringWithDefault<bool>(Params.Get("partition_stats"), false));
        record.MutableOptions()->SetReturnPartitioningInfo(FromStringWithDefault<bool>(Params.Get("partitioning_info"), true));
    }

    void RequestSchemeShard() {
        NKikimrSchemeOp::TDescribePath options;
        FillParams(options);
        if (options.GetSchemeshardId()) {
            THolder<TEvSchemeShard::TEvDescribeScheme> request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>();
            request->Record = options;
            SchemeShardResult = MakeRequestToTablet<TEvSchemeShard::TEvDescribeSchemeResult>(request->Record.GetSchemeshardId(), request.Release());
        } else {
            THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->CopyFrom(options);
            auto tokenObj = GetRequest().GetUserTokenObject();
            if (tokenObj) {
                request->Record.SetUserToken(tokenObj);
            }
            SchemeShardResult = MakeRequest<TEvSchemeShard::TEvDescribeSchemeResult>(MakeTxProxyID(), request.Release());
        }
    }

    void RequestSchemeCache() {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.ShowPrivatePath = FromStringWithDefault<bool>(Params.Get("private"), true);
        entry.SyncVersion = false;
        if (Params.Has("path")) {
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.Path = SplitPath(Params.Get("path"));
        } else if (Params.Has("path_id")) {
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.TableId = TTableId(GetSchemeShardId(), FromStringWithDefault<ui64>(Params.Get("path_id")));
        }
        request->ResultSet.emplace_back(entry);
        auto tokenObj = GetRequest().GetUserTokenObject();
        if (tokenObj) {
            request->UserToken = new NACLib::TUserToken(tokenObj);
        }
        CacheResult = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
    }

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        if (Params.Has("path_id")) {
            if (!Viewer->CheckAccessMonitoring(GetRequest())) {
                // it's dangerous because we don't check access to specific path here
                ReplyAndPassAway(GetHTTPFORBIDDEN("text/html", "<html><body><h1>403 Forbidden</h1></body></html>"), "Access denied");
                return;
            }
        }
        if (Params.Has("path_id") && !Params.Has("schemeshard_id") && (!DatabaseNavigateResponse || !DatabaseNavigateResponse->IsOk())) {
            // path_id is not enough to describe path, we need schemeshard_id, we try to get it from database
            ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "schemeshard_id is required for non-database requests when path_id is specified"));
            return;
        }
        // for describe we keep old behavior where enums is false by default - for compatibility reasons
        if (FromStringWithDefault<bool>(Params.Get("enums"), false)) {
            Proto2JsonConfig.EnumMode = TProto2JsonConfig::EnumValueMode::EnumName;
        } else {
            Proto2JsonConfig.EnumMode = TProto2JsonConfig::EnumValueMode::EnumNumber;
        }
        ExpandSubElements = FromStringWithDefault<ui32>(Params.Get("subs"), ExpandSubElements);
        auto askSchemeCache = Params.Get("ask_scheme_cache");
        if (askSchemeCache == "first") {
            AskSchemeCache = EAskSchemeCache::First;
        } else if (askSchemeCache == "never") {
            AskSchemeCache = EAskSchemeCache::Never;
        } else if (askSchemeCache == "only") {
            AskSchemeCache = EAskSchemeCache::Only;
        } else {
            AskSchemeCache = EAskSchemeCache::Second;
        }
        if (AskSchemeCache == EAskSchemeCache::First || AskSchemeCache == EAskSchemeCache::Only) {
            RequestSchemeCache();
        } else {
            RequestSchemeShard();
        }
        Become(&TThis::StateRequestedDescribe, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (SchemeShardResult.Set(std::move(ev))) {
            if (!SchemeShardResult.IsOk() && AskSchemeCache == EAskSchemeCache::Second) {
                RequestSchemeCache();
            }
            RequestDone();
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (CacheResult.Set(std::move(ev))) {
            if (!CacheResult.IsOk() && AskSchemeCache == EAskSchemeCache::First) {
                RequestSchemeShard();
            }
            RequestDone();
        }
    }

    void FillDescription(NKikimrSchemeOp::TDirEntry* descr, ui64 schemeShardId) {
        descr->SetSchemeshardId(schemeShardId);
        descr->SetCreateFinished(true);
        descr->SetCreateTxId(0);
        descr->SetCreateStep(0);
    }

    NKikimrSchemeOp::EPathType ConvertType(TNavigate::EKind navigate) {
        switch (navigate) {
            case TNavigate::KindSubdomain:
                return NKikimrSchemeOp::EPathTypeSubDomain;
            case TNavigate::KindPath:
                return NKikimrSchemeOp::EPathTypeDir;
            case TNavigate::KindExtSubdomain:
                return NKikimrSchemeOp::EPathTypeExtSubDomain;
            case TNavigate::KindTable:
                return NKikimrSchemeOp::EPathTypeTable;
            case TNavigate::KindOlapStore:
                return NKikimrSchemeOp::EPathTypeColumnStore;
            case TNavigate::KindColumnTable:
                return NKikimrSchemeOp::EPathTypeColumnTable;
            case TNavigate::KindRtmr:
                return NKikimrSchemeOp::EPathTypeRtmrVolume;
            case TNavigate::KindKesus:
                return NKikimrSchemeOp::EPathTypeKesus;
            case TNavigate::KindSolomon:
                return NKikimrSchemeOp::EPathTypeSolomonVolume;
            case TNavigate::KindTopic:
                return NKikimrSchemeOp::EPathTypePersQueueGroup;
            case TNavigate::KindCdcStream:
                return NKikimrSchemeOp::EPathTypeCdcStream;
            case TNavigate::KindSequence:
                return NKikimrSchemeOp::EPathTypeSequence;
            case TNavigate::KindReplication:
                return NKikimrSchemeOp::EPathTypeReplication;
            case TNavigate::KindTransfer:
                return NKikimrSchemeOp::EPathTypeTransfer;
            case TNavigate::KindBlobDepot:
                return NKikimrSchemeOp::EPathTypeBlobDepot;
            case TNavigate::KindExternalTable:
                return NKikimrSchemeOp::EPathTypeExternalTable;
            case TNavigate::KindExternalDataSource:
                return NKikimrSchemeOp::EPathTypeExternalDataSource;
            case TNavigate::KindBlockStoreVolume:
                return NKikimrSchemeOp::EPathTypeBlockStoreVolume;
            case TNavigate::KindFileStore:
                return NKikimrSchemeOp::EPathTypeFileStore;
            case TNavigate::KindView:
                return NKikimrSchemeOp::EPathTypeView;
            case TNavigate::KindSysView:
                return NKikimrSchemeOp::EPathTypeSysView;
            case TNavigate::KindResourcePool:
                return NKikimrSchemeOp::EPathTypeResourcePool;
            case TNavigate::KindBackupCollection:
                return NKikimrSchemeOp::EPathTypeBackupCollection;
            case TNavigate::KindSecret:
                return NKikimrSchemeOp::EPathTypeSecret;
            case TNavigate::KindStreamingQuery:
                return NKikimrSchemeOp::EPathTypeStreamingQuery;
            case TNavigate::KindIndex:
                return NKikimrSchemeOp::EPathTypeTableIndex;
            case TNavigate::KindUnknown:
                return NKikimrSchemeOp::EPathTypeDir;
        }
    }

    TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo> GetSchemeShardDescribeSchemeInfo() {
        TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo> result(new NKikimrViewer::TEvDescribeSchemeInfo());
        auto& record = SchemeShardResult->GetRecord();
        const auto *descriptor = NKikimrScheme::EStatus_descriptor();
        result->SetStatus(descriptor->FindValueByNumber(record.GetStatus())->name());
        result->SetReason(record.GetReason());
        result->SetPath(record.GetPath());
        result->MutablePathDescription()->CopyFrom(record.GetPathDescription());
        if (record.GetPathId() != 0 && record.GetPathId() != InvalidLocalPathId) {
            result->SetPathId(record.GetPathId());
        }
        if (result->MutablePathDescription()->GetSelf().GetPathId() == 0 || result->MutablePathDescription()->GetSelf().GetPathId() == InvalidLocalPathId) {
            result->MutablePathDescription()->MutableSelf()->ClearPathId();
        }
        if (result->MutablePathDescription()->GetSelf().GetParentPathId() == 0 || result->MutablePathDescription()->GetSelf().GetParentPathId() == InvalidLocalPathId) {
            result->MutablePathDescription()->MutableSelf()->ClearParentPathId();
        }
        result->SetLastExistedPrefixPath(record.GetLastExistedPrefixPath());
        result->SetLastExistedPrefixPathId(record.GetLastExistedPrefixPathId());
        result->MutableLastExistedPrefixDescription()->CopyFrom(record.GetLastExistedPrefixDescription());
        if (record.GetPathOwnerId() != 0 && record.GetPathOwnerId() != InvalidOwnerId) {
            result->SetPathOwnerId(record.GetPathOwnerId());
        }
        result->SetSource(NKikimrViewer::TEvDescribeSchemeInfo::SchemeShard);

        return result;
    }

    TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo> GetCacheDescribeSchemeInfo() {
        const auto& entry = CacheResult->Request.Get()->ResultSet.front();
        TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo> result(new NKikimrViewer::TEvDescribeSchemeInfo());
        result->SetPath(CanonizePath(entry.Path));
        auto* pathDescription = result->MutablePathDescription();
        auto* self = pathDescription->MutableSelf();
        if (entry.Self) {
            self->CopyFrom(entry.Self->Info);
            if (self->GetPathId() == 0 || self->GetPathId() == InvalidLocalPathId) {
                self->ClearPathId();
            }
            if (self->GetParentPathId() == 0 || self->GetParentPathId() == InvalidLocalPathId) {
                self->ClearParentPathId();
            }
            if (entry.Self->Info.GetPathId() != 0 && entry.Self->Info.GetPathId() != InvalidLocalPathId) {
                result->SetPathId(entry.Self->Info.GetPathId());
            }
            if (entry.Self->Info.GetSchemeshardId() != 0 && entry.Self->Info.GetSchemeshardId() != InvalidOwnerId) {
                result->SetPathOwnerId(entry.Self->Info.GetSchemeshardId());
            }
        }
        if (entry.ListNodeEntry) {
            for (const auto& child : entry.ListNodeEntry->Children) {
                auto descr = pathDescription->AddChildren();
                descr->SetName(child.Name);
                descr->SetPathType(ConvertType(child.Kind));
            }
        };
        if (entry.DomainDescription) {
            pathDescription->MutableDomainDescription()->CopyFrom(entry.DomainDescription->Description);
        }
        result->SetStatus(NKikimrScheme::EStatus_Name(NKikimrScheme::StatusSuccess));
        result->SetSource(NKikimrViewer::TEvDescribeSchemeInfo::Cache);
        return result;
    }

    void ReplyAndPassAway() override {
        TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo> describe;
        if (SchemeShardResult.IsOk()) {
            describe = GetSchemeShardDescribeSchemeInfo();
        } else if (CacheResult.IsOk()) {
            describe = GetCacheDescribeSchemeInfo();
        }
        NJson::TJsonValue json;
        if (describe != nullptr) {
            if (describe->HasPathDescription()) {
                auto& pathDescription = *describe->MutablePathDescription();
                if (pathDescription.HasTable()) {
                    auto& table = *pathDescription.MutableTable();
                    for (auto& column : *table.MutableColumns()) {
                        if (!column.HasFamily()) {
                            column.SetFamily(0);
                        }
                        if (column.GetFamily() == 0 && !column.HasFamilyName()) {
                            column.SetFamilyName("default");
                        }
                    }
                    if (ExpandSubElements) {
                        for (auto& tableIndex : table.GetTableIndexes()) {
                            NKikimrSchemeOp::TDirEntry& child = *pathDescription.AddChildren();
                            child.SetName(tableIndex.GetName());
                            child.SetPathType(NKikimrSchemeOp::EPathType::EPathTypeTableIndex);
                        }
                        for (auto& tableCdc : table.GetCdcStreams()) {
                            NKikimrSchemeOp::TDirEntry& child = *pathDescription.AddChildren();
                            child.SetName(tableCdc.GetName());
                            child.SetPathType(NKikimrSchemeOp::EPathType::EPathTypeCdcStream);
                        }
                    }
                }
            }
            const auto *descriptor = NKikimrScheme::EStatus_descriptor();
            auto accessDeniedStatus = descriptor->FindValueByNumber(NKikimrScheme::StatusAccessDenied)->name();
            if (describe->GetStatus() == accessDeniedStatus) {
                ReplyAndPassAway(GetHTTPFORBIDDEN("text/plain", "Forbidden"));
                return;
            }
            for (auto& child : *describe->MutablePathDescription()->MutableChildren()) {
                if (child.GetPathId() == InvalidLocalPathId) {
                    child.ClearPathId();
                }
                if (child.GetParentPathId() == InvalidLocalPathId) {
                    child.ClearParentPathId();
                }
            }
            Proto2Json(*describe, json);
            DecodeExternalTableContent(describe, json);
        }

        ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    void DecodeExternalTableContent(TAutoPtr<NKikimrViewer::TEvDescribeSchemeInfo>& describe, NJson::TJsonValue& json) const {
        if (!describe) {
            return;
        }

        if (!describe->GetPathDescription().HasExternalTableDescription()) {
            return;
        }

        const auto& content = describe->GetPathDescription().GetExternalTableDescription().GetContent();
        if (!content) {
            return;
        }

        NExternalSource::IExternalSourceFactory::TPtr externalSourceFactory{NExternalSource::CreateExternalSourceFactory({}, nullptr, 50000, nullptr, false, false, true, NYql::GetAllExternalDataSourceTypes())};
        const auto& sourceType = describe->GetPathDescription().GetExternalTableDescription().GetSourceType();
        try {
            json["PathDescription"]["ExternalTableDescription"].EraseValue("Content");
            auto source = externalSourceFactory->GetOrCreate(sourceType);
            auto parameters = source->GetParameters(content);
            for (const auto& [key, items]: parameters) {
                NJson::TJsonValue array{NJson::EJsonValueType::JSON_ARRAY};
                for (const auto& item: items) {
                    array.AppendValue(item);
                }
                json["PathDescription"]["ExternalTableDescription"]["Content"][key] = array;
            }
        } catch (...) {
            BLOG_CRIT("Сan't unpack content for external table: " << sourceType << ", error: " << CurrentExceptionMessage());
        }
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Schema detailed information",
            .Description = "Returns detailed information about schema object"
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "schemeshard_id",
            .Description = "schemeshard identifier (tablet id)",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "path_id",
            .Description = "path id",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "ask_scheme_cache",
            .Description = "how to use scheme cache: first, second (default), never, only",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "backup",
            .Description = "return backup information",
            .Type = "boolean",
            .Default = "true",
        });
        yaml.AddParameter({
            .Name = "private",
            .Description = "return private tables",
            .Type = "boolean",
            .Default = "true",
        });
        yaml.AddParameter({
            .Name = "children",
            .Description = "return children",
            .Type = "boolean",
            .Default = "true",
        });
        yaml.AddParameter({
            .Name = "boundaries",
            .Description = "return boundaries",
            .Type = "boolean",
            .Default = "false",
        });
        yaml.AddParameter({
            .Name = "partition_config",
            .Description = "return partition configuration",
            .Type = "boolean",
            .Default = "true",
        });
        yaml.AddParameter({
            .Name = "partition_stats",
            .Description = "return partitions statistics",
            .Type = "boolean",
            .Default = "false",
        });
        yaml.AddParameter({
            .Name = "partitioning_info",
            .Description = "return partitioning information",
            .Type = "boolean",
            .Default = "true",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TEvDescribeSchemeInfo>());
        return yaml;
    }
};

}
