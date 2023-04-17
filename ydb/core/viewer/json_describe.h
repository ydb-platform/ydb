#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "viewer.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using NSchemeShard::TEvSchemeShard;

class TJsonDescribe : public TViewerPipeClient<TJsonDescribe> {
    using TBase = TViewerPipeClient<TJsonDescribe>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    bool ExpandSubElements = true;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonDescribe(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void FillParams(NKikimrSchemeOp::TDescribePath* record, const TCgiParameters& params) {
        if (params.Has("path")) {
            record->SetPath(params.Get("path"));
        }
        if (params.Has("path_id")) {
            record->SetPathId(FromStringWithDefault<ui64>(params.Get("path_id")));
        }
        if (params.Has("schemeshard_id")) {
            record->SetSchemeshardId(FromStringWithDefault<ui64>(params.Get("schemeshard_id")));
        }
        record->MutableOptions()->SetBackupInfo(FromStringWithDefault<bool>(params.Get("backup"), true));
        record->MutableOptions()->SetShowPrivateTable(FromStringWithDefault<bool>(params.Get("private"), true));
        record->MutableOptions()->SetReturnChildren(FromStringWithDefault<bool>(params.Get("children"), true));
        record->MutableOptions()->SetReturnBoundaries(FromStringWithDefault<bool>(params.Get("boundaries"), false));
        record->MutableOptions()->SetReturnPartitionConfig(FromStringWithDefault<bool>(params.Get("partition_config"), true));
        record->MutableOptions()->SetReturnPartitionStats(FromStringWithDefault<bool>(params.Get("partition_stats"), false));
        record->MutableOptions()->SetReturnPartitioningInfo(FromStringWithDefault<bool>(params.Get("partitioning_info"), true));
    }

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        ExpandSubElements = FromStringWithDefault<ui32>(params.Get("subs"), ExpandSubElements);
        InitConfig(params);

        if (params.Has("schemeshard_id")) {
            THolder<TEvSchemeShard::TEvDescribeScheme> request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>();
            FillParams(&request->Record, params);
            ui64 schemeShardId = FromStringWithDefault<ui64>(params.Get("schemeshard_id"));
            SendRequestToPipe(ConnectTabletPipe(schemeShardId), request.Release());
        } else {
            THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            FillParams(request->Record.MutableDescribePath(), params);
            request->Record.SetUserToken(Event->Get()->UserToken);
            SendRequest(MakeTxProxyID(), request.Release());
        }
        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        DescribeResult = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() {
        TStringStream json;
        TString headers = Viewer->GetHTTPOKJSON(Event->Get());
        if (DescribeResult != nullptr) {
            if (ExpandSubElements) {
                NKikimrScheme::TEvDescribeSchemeResult& describe = *(DescribeResult->MutableRecord());
                if (describe.HasPathDescription()) {
                    auto& pathDescription = *describe.MutablePathDescription();
                    if (pathDescription.HasTable()) {
                        auto& table = *pathDescription.MutableTable();
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
            TProtoToJson::ProtoToJson(json, DescribeResult->GetRecord(), JsonSettings);
            switch (DescribeResult->GetRecord().GetStatus()) {
            case NKikimrScheme::StatusAccessDenied:
                headers = HTTPFORBIDDENJSON;
                break;
            default:
                break;
            }
        } else {
            json << "null";
        }

        Send(Event->Sender, new NMon::TEvHttpInfoRes(headers + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonDescribe> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::ProtoRecordType>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonDescribe> {
    static TString GetParameters() {
        return R"___([{"name":"path","in":"query","description":"schema path","required":false,"type":"string"},
                      {"name":"schemeshard_id","in":"query","description":"schemeshard identifier (tablet id)","required":false,"type":"integer"},
                      {"name":"path_id","in":"query","description":"path id","required":false,"type":"integer"},
                      {"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"backup","in":"query","description":"return backup information","required":false,"type":"boolean","default":true},
                      {"name":"private","in":"query","description":"return private tables","required":false,"type":"boolean","default":true},
                      {"name":"children","in":"query","description":"return children","required":false,"type":"boolean","default":true},
                      {"name":"boundaries","in":"query","description":"return boundaries","required":false,"type":"boolean","default":false},
                      {"name":"partition_config","in":"query","description":"return partition configuration","required":false,"type":"boolean","default":true},
                      {"name":"partition_stats","in":"query","description":"return partitions statistics","required":false,"type":"boolean","default":false},
                      {"name":"partitioning_info","in":"query","description":"return partitioning information","required":false,"type":"boolean","default":true},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonDescribe> {
    static TString GetSummary() {
        return "\"Schema detailed information\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonDescribe> {
    static TString GetDescription() {
        return "\"Returns detailed information about schema object\"";
    }
};

}
}
