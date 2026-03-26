#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NViewer {

using namespace NActors;
namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;

class TJsonHotkeys : public TViewerPipeClient {
    using TThis = TJsonHotkeys;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    int LimitKeys = 10;
    ui64 SamplingPeriod = 30000;
    float PollingFactor = 1;
    int LimitShards = 100;
    bool EnableSampling = false;
    bool ReturnActualData = true;
    int ShardsAsked = 0;
    int ShardsCollected = 0;
    TString TableName; // to be filled after describe
    std::vector<TString> KeyNames; // to be filled after collecting statistics
    std::multimap<ui64, TVector<TString>, std::greater<ui64>> Keys;
    std::deque<TRequestResponse<TEvDataShard::TEvGetDataHistogramResponse>> HistogramResponses;

public:
    TJsonHotkeys(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        LimitKeys = FromStringWithDefault(Params.Get("limit_keys"), LimitKeys);
        LimitShards = FromStringWithDefault(Params.Get("limit_shards"), LimitShards);
        PollingFactor = std::clamp(FromStringWithDefault(Params.Get("polling_factor"), PollingFactor), 0.0f, 1.0f);
        EnableSampling = FromStringWithDefault(Params.Get("enable_sampling"), EnableSampling);
        SamplingPeriod = FromStringWithDefault(Params.Get("sampling_period"), SamplingPeriod);
        ReturnActualData = FromStringWithDefault(Params.Get("actual_data"), ReturnActualData);
        THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        if (Params.Has("path")) {
            request->Record.MutableDescribePath()->SetPath(Params.Get("path"));
        } else {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "path is required"));
        }
        request->Record.MutableDescribePath()->MutableOptions()->SetReturnPartitionStats(true);
        request->Record.SetUserToken(GetRequest().GetUserTokenObject());
        DescribeResult = MakeRequest<TEvSchemeShard::TEvDescribeSchemeResult>(MakeTxProxyID(), request.Release());
        Become(&TThis::StateRequestedDescribe, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvDataShard::TEvGetDataHistogramResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (DescribeResult.Set(std::move(ev))) {
            const auto& pbRecord(DescribeResult->GetRecord());
            if (pbRecord.HasPathDescription()) {
                const auto& pathDescription = pbRecord.GetPathDescription();
                TableName = pathDescription.GetSelf().GetName();
                auto securityObject = std::make_unique<TSecurityObject>(pathDescription.GetSelf().GetOwner(), pathDescription.GetSelf().GetEffectiveACL(), false);
                auto tokenObject = GetRequest().GetUserTokenObject();
                if (tokenObject && !securityObject->CheckAccess(NACLib::SelectRow, NACLib::TUserToken(tokenObject))) {
                    return ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", "Read access required"), "Access denied");
                }
                const auto& partitions = pathDescription.GetTablePartitions();
                const auto& metrics = pathDescription.GetTablePartitionMetrics();

                if (!metrics.empty()) {
                    TVector<std::pair<ui64, int>> tabletsOrder;

                    for (int i = 0; i < metrics.size(); ++i) {
                        tabletsOrder.emplace_back(metrics.Get(i).GetCPU(), i);
                    }

                    Sort(tabletsOrder, std::greater<std::pair<ui64, int>>());
                    ShardsAsked = std::clamp<int>(std::min<int>(LimitShards, std::ceil(PollingFactor * tabletsOrder.size())), 1, tabletsOrder.size());

                    for (int i = 0; i < ShardsAsked; ++i) {
                        THolder<TEvDataShard::TEvGetDataHistogramRequest> request = MakeHolder<TEvDataShard::TEvGetDataHistogramRequest>();
                        if (EnableSampling) {
                            request->Record.SetCollectKeySampleMs(SamplingPeriod);
                        }
                        if (ReturnActualData) {
                            request->Record.SetActualData(true);
                        }
                        ui64 datashardId = partitions.Get(tabletsOrder[i].second).GetDatashardId();
                        TRequestResponse<TEvDataShard::TEvGetDataHistogramResponse>& histogramResponse = HistogramResponses.emplace_back();
                        ui64 cookie = HistogramResponses.size(); // to make sure it starts from 1
                        histogramResponse = MakeRequestToTablet<TEvDataShard::TEvGetDataHistogramResponse>(datashardId, request.Release(), cookie);
                    }
                }
            }
            RequestDone();
        }
    }

    void Handle(TEvDataShard::TEvGetDataHistogramResponse::TPtr& ev) {
        const NKikimrTxDataShard::TEvGetDataHistogramResponse* response = nullptr;
        ui64 cookie = ev->Cookie;
        if (cookie == 0 || cookie > HistogramResponses.size()) {
            // invalid cookie / old version
            response = &ev->Get()->Record;
        } else {
            TRequestResponse<TEvDataShard::TEvGetDataHistogramResponse>& histogramResponse = HistogramResponses[cookie - 1];
            if (histogramResponse.Set(std::move(ev))) {
                response = &histogramResponse->Record;
            } else {
                // something went wrong (duplicate response?), fallback to direct response
                response = &ev->Get()->Record;
            }
        }
        for (const auto& histograms : response->GetTableHistograms()) {
            if (histograms.GetTableName() != TableName) {
                continue;
            }
            if (KeyNames.empty()) {
                for (const auto& keyName : histograms.GetKeyNames()) {
                    KeyNames.push_back(keyName);
                }
            }
            if (!histograms.GetKeyAccessSample().GetItems().empty()) {
                ++ShardsCollected;
            }
            for (const auto& keyAccessSample : histograms.GetKeyAccessSample().GetItems()) {
                TVector<TString> keys(keyAccessSample.GetKeyValues().begin(), keyAccessSample.GetKeyValues().end());
                Keys.emplace(keyAccessSample.GetValue(), std::move(keys));
            }
        }
        RequestDone();
    }

    void ReplyAndPassAway() override {
        if (DescribeResult.IsError()) {
            if (DescribeResult.GetError() == "AccessDenied") {
                return ReplyAndPassAway(GETHTTPACCESSDENIED(), "Access denied");
            } else if (DescribeResult.GetError() == "Unknown") {
                return ReplyAndPassAway(GetHTTPINTERNALERROR(), "Unknown error");
            } else {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", DescribeResult.GetError()));
            }
        }
        NJson::TJsonValue root;
        if (DescribeResult.IsOk()) {
            if (!TableName.empty()) {
                root["table"] = TableName;
            }
            if (!KeyNames.empty()) {
                NJson::TJsonValue& keyNames = root["keyNames"];
                for (const auto& keyName : KeyNames) {
                    keyNames.AppendValue(keyName);
                }
            }
            root["shardsAsked"] = ShardsAsked;
            root["shardsCollected"] = ShardsCollected;
            if (!Keys.empty()) {
                NJson::TJsonValue& hotkeys = root["hotkeys"];
                hotkeys.SetType(NJson::JSON_ARRAY);
                int keys = 0;
                for (const auto& [values, key] : Keys) {
                    NJson::TJsonValue& hotkey = hotkeys.AppendValue({});
                    hotkey["accessSample"] = values;
                    NJson::TJsonValue& keyValues = hotkey["keyValues"];
                    keyValues.SetType(NJson::JSON_ARRAY);
                    for (const auto& k : key) {
                        keyValues.AppendValue(k);
                    }
                    ++keys;
                    if (LimitKeys && keys >= LimitKeys) {
                        break;
                    }
                }
            }
        }
        ReplyAndPassAway(GetHTTPOKJSON(root));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Information about current hot keys in a datashard",
            .Description = "Samples and returns information about current hot keys",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "path to the table",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "enable_sampling",
            .Description = "enable sampling",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "sampling_period",
            .Description = "sampling period in ms",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "polling_factor",
            .Description = "polling factor",
            .Type = "float",
        });
        yaml.AddParameter({
            .Name = "limit_shards",
            .Description = "limit of shards to poll",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "limit_keys",
            .Description = "limit of hot keys",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "actual_data",
            .Description = "return actual data instead only (don't return anything when data is old)",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        return yaml;
    }
};

}
