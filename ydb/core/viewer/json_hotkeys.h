#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <util/generic/hash.h>
#include <util/generic/fwd.h>
#include "viewer.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using NSchemeShard::TEvSchemeShard;

class TJsonHotkeys : public TViewerPipeClient {
    static const bool WithRetry = false;
    using TThis = TJsonHotkeys;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    ui32 Timeout = 0;
    ui32 Limit = 0;
    float PollingFactor = 0.0;
    bool EnableSampling = false;

    struct KeysComparator {
        bool operator ()(const std::pair<ui64, TVector<TString>>& a, const std::pair<ui64, TVector<TString>>& b) const {
            return a.first > b.first;
        };
    };

    TMultiSet<std::pair<ui64, TVector<TString>>, KeysComparator> Keys;

public:
    TJsonHotkeys(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void FillParams(NKikimrSchemeOp::TDescribePath* record, const TCgiParameters& params) {
        if (params.Has("path")) {
            record->SetPath(params.Get("path"));
        }
        record->MutableOptions()->SetReturnPartitionStats(true);
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), 10);
        PollingFactor = std::max(0.0f, std::min(FromStringWithDefault<float>(params.Get("polling_factor"), 0.2), 1.0f));
        EnableSampling = FromStringWithDefault<bool>(params.Get("enable_sampling"), false);
        InitConfig(params);

        THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        FillParams(request->Record.MutableDescribePath(), params);
        request->Record.SetUserToken(Event->Get()->UserToken);
        SendRequest(MakeTxProxyID(), request.Release());

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvDataShard::TEvGetDataHistogramResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        DescribeResult = ev->Release();
        const auto& pbRecord(DescribeResult->GetRecord());
        if (pbRecord.HasPathDescription()) {
            const auto& pathDescription = pbRecord.GetPathDescription();
            const auto& partitions = pathDescription.GetTablePartitions();
            const auto& metrics = pathDescription.GetTablePartitionMetrics();
            TVector<std::pair<ui64, int>> tabletsOrder;

            for (int i = 0; i < metrics.size(); ++i) {
                tabletsOrder.emplace_back(metrics.Get(i).GetCPU(), i);
            }

            Sort(tabletsOrder, std::greater<std::pair<ui64, int>>());
            ui32 tablets = (ui32) std::max(1, (int) std::ceil(PollingFactor * tabletsOrder.size()));

            for (ui32 i = 0; i < tablets; ++i) {
                THolder<TEvDataShard::TEvGetDataHistogramRequest> request = MakeHolder<TEvDataShard::TEvGetDataHistogramRequest>();
                if (EnableSampling) {
                    request->Record.SetCollectKeySampleMs(30000); // 30 sec
                }
                request->Record.SetActualData(true);
                ui64 datashardId = partitions.Get(tabletsOrder[i].second).GetDatashardId();
                SendRequestToPipe(ConnectTabletPipe(datashardId), request.Release());
            }
        }

        RequestDone();
    }

    void Handle(TEvDataShard::TEvGetDataHistogramResponse::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        for (const auto& i: rec.GetTableHistograms()) {
            for (const auto& item: i.GetKeyAccessSample().GetItems()) {
                TVector<TString> keys(item.GetKeyValues().begin(), item.GetKeyValues().end());
                Keys.emplace(item.GetValue(), std::move(keys));
                if (Keys.size() > Limit) {
                    Keys.erase(--Keys.end());
                }
            }
        }

        RequestDone();
    }

    NJson::TJsonValue BuildResponse() {
        NJson::TJsonValue root;
        if (DescribeResult != nullptr) {
            NJson::TJsonValue& hotkeys = root["hotkeys"];
            for (const auto &i: Keys) {
                NJson::TJsonValue entry;
                NJson::TJsonValue keyValues;
                for (const auto &j: i.second) {
                    keyValues.AppendValue(j);
                }
                entry["accessSample"] = i.first;
                entry["keyValues"] = std::move(keyValues);
                hotkeys.AppendValue(std::move(entry));
            }
        }
        return root;
    }

    void ReplyAndPassAway() override {
        if (DescribeResult != nullptr) {
            switch (DescribeResult->GetRecord().GetStatus()) {
            case NKikimrScheme::StatusAccessDenied:
                Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPFORBIDDEN(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                PassAway();
                return;
            default:
                break;
            }
        }
        NJson::TJsonValue root = BuildResponse();
        TString json = NJson::WriteJson(root, false);

        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

}
}
