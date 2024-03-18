#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "query_autocomplete_helper.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonQueryAutocompleteTables : public TViewerPipeClient<TJsonQueryAutocompleteTables> {
    using TBase = TViewerPipeClient<TJsonQueryAutocompleteTables>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;

    struct NavigateChild {
        TString Name;
        NSchemeCache::TSchemeCacheNavigate::EKind Kind;
    };
    THashMap<TString, NavigateChild> Children;
    TString Database;
    TString Prefix;
    TString Path;
    ui32 Limit = 10;
    NKikimrViewer::TQueryAutocomplete Result;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonQueryAutocompleteTables(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Database = params.Get("database");
        Prefix = params.Get("prefix");
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), Limit);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();

        if (Database && !Prefix.StartsWith(Database)) {
            Prefix = Database + "/" + Prefix;
        }
        if (!Prefix.StartsWith("/" + domain->Name)) {
            Prefix = "/" + domain->Name + "/" + Prefix;
        }
        auto splitPos = Prefix.find_last_of('/');
        Path = Prefix.substr(0, splitPos);
        Prefix = Prefix.substr(splitPos + 1);

        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = false;
        entry.Path = SplitPath(Path);
        request->ResultSet.emplace_back(entry);
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    TString ConvertType(TNavigate::EKind navigate) {
        switch (navigate) {
            case TNavigate::KindSubdomain:
                return "Subdomain";
            case TNavigate::KindPath:
                return "Directory";
            case TNavigate::KindExtSubdomain:
                return "ExtSubDomain";
            case TNavigate::KindTable:
                return "Table";
            case TNavigate::KindOlapStore:
                return "ColumnStore";
            case TNavigate::KindColumnTable:
                return "ColumnTable";
            case TNavigate::KindRtmr:
                return "RtmrVolume";
            case TNavigate::KindKesus:
                return "Kesus";
            case TNavigate::KindSolomon:
                return "SolomonVolume";
            case TNavigate::KindTopic:
                return "PersQueueGroup";
            case TNavigate::KindCdcStream:
                return "CdcStream";
            case TNavigate::KindSequence:
                return "Sequence";
            case TNavigate::KindReplication:
                return "Replication";
            case TNavigate::KindBlobDepot:
                return "BlobDepot";
            case TNavigate::KindExternalTable:
                return "ExternalTable";
            case TNavigate::KindExternalDataSource:
                return "ExternalDataSource";
            case TNavigate::KindBlockStoreVolume:
                return "BlockStoreVolume";
            case TNavigate::KindFileStore:
                return "FileStore";
            case TNavigate::KindView:
                return "View";
            default:
                return "Dir";
        }
    }

    void GetTablesDictionaryChildren() {
        const auto& entry = CacheResult->Request.Get()->ResultSet.front();
        if (entry.ListNodeEntry) {
            for (const auto& child : entry.ListNodeEntry->Children) {
                Children[child.Name] = NavigateChild(child.Name, child.Kind);
            }
        };
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        CacheResult = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() {
        if (CacheResult == nullptr) {
            Result.set_success(false);
            Result.add_error("Failed to collect information");
        } else {
            NSchemeCache::TSchemeCacheNavigate *navigate = CacheResult->Request.Get();
            if (navigate->ErrorCount > 0) {
                Result.set_success(false);
                Result.add_error("Inner errors while collected information");
            } else {
                GetTablesDictionaryChildren();
                auto fuzzy = FuzzySearcher(Children);
                auto autocomplete = fuzzy.Search(Prefix, Limit);
                for (NavigateChild& child: autocomplete) {
                    auto entity = Result.MutableResult()->MutableTablesResult()->AddEntities();
                    entity->set_name(child.Name);
                    entity->set_type(ConvertType(child.Kind));
                }
                Result.set_success(true);
            }
        }

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
struct TJsonRequestSchema<TJsonQueryAutocompleteTables> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TQueryAutocomplete>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonQueryAutocompleteTables> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"database","in":"query","description":"database context","required":false,"type":"string"},
                      {"name":"prefix","in":"query","description":"known part of the word","required":false,"type":"string"},
                      {"name":"limit","in":"query","description":"limit of entities","required":false,"type":"integer"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonQueryAutocompleteTables> {
    static TString GetSummary() {
        return "\"Tenant info (brief)\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonQueryAutocompleteTables> {
    static TString GetDescription() {
        return "\"Returns list of tenants\"";
    }
};

}
}
