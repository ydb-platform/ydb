#pragma once

#include "json_proxy.h"

#include <util/string/split.h>

namespace NKikimr::NCms {

template <typename TRequestEvent, typename TResponseEvent>
class TJsonProxyDataShard : public TJsonProxy<TRequestEvent, TResponseEvent> {
public:
    TJsonProxyDataShard(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxy<TRequestEvent, TResponseEvent>(event)
    {
    }

    ui64 GetTabletId(const TActorContext &) const override {
        const TCgiParameters &cgi = this->RequestEvent->Get()->Request.GetParams();
        ui64 tabletId = 0;

        if (cgi.Has("tabletid")) {
            TryFromString(cgi.Get("tabletid"), tabletId);
        }

        if (!tabletId && cgi.Has("followerid")) {
            TryFromString(cgi.Get("followerid"), tabletId);
        }

        return tabletId;
    }

    TString GetTabletName() const override {
        return "DataShard";
    }
};

class TJsonProxyDataShardGetOperation : public TJsonProxyDataShard<NEvDataShard::TEvGetOperationRequest,
                                                                   NEvDataShard::TEvGetOperationResponse>
{
public:
    TJsonProxyDataShardGetOperation(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetOperationRequest,
                              NEvDataShard::TEvGetOperationResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetOperationRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new NEvDataShard::TEvGetOperationRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableSinkState : public TJsonProxyDataShard<NEvDataShard::TEvGetReadTableSinkStateRequest,
                                                                            NEvDataShard::TEvGetReadTableSinkStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableSinkState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetReadTableSinkStateRequest,
                              NEvDataShard::TEvGetReadTableSinkStateResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetReadTableSinkStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new NEvDataShard::TEvGetReadTableSinkStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableScanState : public TJsonProxyDataShard<NEvDataShard::TEvGetReadTableScanStateRequest,
                                                                            NEvDataShard::TEvGetReadTableScanStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableScanState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetReadTableScanStateRequest,
                              NEvDataShard::TEvGetReadTableScanStateResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetReadTableScanStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new NEvDataShard::TEvGetReadTableScanStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableStreamState : public TJsonProxyDataShard<NEvDataShard::TEvGetReadTableStreamStateRequest,
                                                                              NEvDataShard::TEvGetReadTableStreamStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableStreamState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetReadTableStreamStateRequest,
                              NEvDataShard::TEvGetReadTableStreamStateResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetReadTableStreamStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new NEvDataShard::TEvGetReadTableStreamStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetSlowOpProfiles : public TJsonProxyDataShard<NEvDataShard::TEvGetSlowOpProfilesRequest,
                                                                        NEvDataShard::TEvGetSlowOpProfilesResponse>
{
public:
    TJsonProxyDataShardGetSlowOpProfiles(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetSlowOpProfilesRequest,
                              NEvDataShard::TEvGetSlowOpProfilesResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetSlowOpProfilesRequest> PrepareRequest(const TActorContext &) override {
        return new NEvDataShard::TEvGetSlowOpProfilesRequest;
    }
};

class TJsonProxyDataShardGetRSInfo : public TJsonProxyDataShard<NEvDataShard::TEvGetRSInfoRequest,
                                                                NEvDataShard::TEvGetRSInfoResponse>
{
public:
    TJsonProxyDataShardGetRSInfo(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetRSInfoRequest,
                              NEvDataShard::TEvGetRSInfoResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetRSInfoRequest> PrepareRequest(const TActorContext &) override {
        return new NEvDataShard::TEvGetRSInfoRequest;
    }
};

class TJsonProxyDataShardGetDataHistogram : public TJsonProxyDataShard<NEvDataShard::TEvGetDataHistogramRequest,
                                                                       NEvDataShard::TEvGetDataHistogramResponse>
{
public:
    TJsonProxyDataShardGetDataHistogram(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<NEvDataShard::TEvGetDataHistogramRequest,
                              NEvDataShard::TEvGetDataHistogramResponse>(event)
    {
    }

    TAutoPtr<NEvDataShard::TEvGetDataHistogramRequest> PrepareRequest(const TActorContext &) override {
        return new NEvDataShard::TEvGetDataHistogramRequest;
    }
};

} // namespace NKikimr::NCms
