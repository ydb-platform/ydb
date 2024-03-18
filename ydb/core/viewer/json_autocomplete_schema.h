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
#include "query_autocomplete_helper.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonQueryAutocompleteSchema : public TViewerPipeClient<TJsonQueryAutocompleteSchema> {
    using TBase = TViewerPipeClient<TJsonQueryAutocompleteSchema>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TAutoPtr<TEvTxProxySchemeCache::TEvNavigateKeySetResult> CacheResult;

    struct SchemaWordData {
        TString Name;
        TString Table;
        TString Type;
    };
    THashMap<TString, SchemaWordData> Dictionary;
    TVector<TString> Tables;
    TString Prefix;
    ui32 Limit = 10;
    NKikimrViewer::TQueryAutocomplete Result;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonQueryAutocompleteSchema(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        StringSplitter(params.Get("table")).Split(',').SkipEmpty().Collect(&Tables);
        Prefix = params.Get("prefix");
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), Limit);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);

        if (Tables.size() == 0) {
            Result.set_success(false);
            Result.add_error("There is no any table in request (`table` parameter)");
            ReplyAndPassAway();
            return;
        }
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        for (TString& table: Tables) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            entry.SyncVersion = false;
            entry.Path = SplitPath(table);
            request->ResultSet.emplace_back(entry);
        }
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void FillSchemaDictionary() {
        for (auto& entry: CacheResult->Request.Get()->ResultSet) {
            TString path = CanonizePath(entry.Path);
            for (const auto& [id, column] : entry.Columns) {
                Dictionary[column.Name] = SchemaWordData(column.Name, path, "column");
            }
            for (const auto& index : entry.Indexes) {
                Dictionary[index.GetName()] = SchemaWordData(index.GetName(), path, "index");
            }
            for (const auto& cdcStream : entry.CdcStreams) {
                Dictionary[cdcStream.GetName()] = SchemaWordData(cdcStream.GetName(), path, "cdcstream");
            }
        }
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
                FillSchemaDictionary();
                auto fuzzy = FuzzySearcher(Dictionary);
                auto autocomplete = fuzzy.Search(Prefix, Limit);
                for (SchemaWordData& wordData: autocomplete) {
                    auto entity = Result.MutableResult()->MutableSchemaResult()->AddEntities();
                    entity->SetName(wordData.Name);
                    entity->SetType(wordData.Type);
                    entity->SetTableName(wordData.Table);
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
struct TJsonRequestSchema<TJsonQueryAutocompleteSchema> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TQueryAutocomplete>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonQueryAutocompleteSchema> {
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
struct TJsonRequestSummary<TJsonQueryAutocompleteSchema> {
    static TString GetSummary() {
        return "\"Tenant info (brief)\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonQueryAutocompleteSchema> {
    static TString GetDescription() {
        return "\"Returns list of tenants\"";
    }
};

}
}
