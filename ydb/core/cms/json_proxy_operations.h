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

class TJsonProxyDataShardGetOperation : public TJsonProxyDataShard<TEvDataShard::TEvGetOperationRequest,
                                                                   TEvDataShard::TEvGetOperationResponse>
{
public:
    TJsonProxyDataShardGetOperation(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetOperationRequest,
                              TEvDataShard::TEvGetOperationResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetOperationRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new TEvDataShard::TEvGetOperationRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableSinkState : public TJsonProxyDataShard<TEvDataShard::TEvGetReadTableSinkStateRequest,
                                                                            TEvDataShard::TEvGetReadTableSinkStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableSinkState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetReadTableSinkStateRequest,
                              TEvDataShard::TEvGetReadTableSinkStateResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetReadTableSinkStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new TEvDataShard::TEvGetReadTableSinkStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableScanState : public TJsonProxyDataShard<TEvDataShard::TEvGetReadTableScanStateRequest,
                                                                            TEvDataShard::TEvGetReadTableScanStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableScanState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetReadTableScanStateRequest,
                              TEvDataShard::TEvGetReadTableScanStateResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetReadTableScanStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new TEvDataShard::TEvGetReadTableScanStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetReadTableStreamState : public TJsonProxyDataShard<TEvDataShard::TEvGetReadTableStreamStateRequest,
                                                                              TEvDataShard::TEvGetReadTableStreamStateResponse>
{
public:
     TJsonProxyDataShardGetReadTableStreamState(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetReadTableStreamStateRequest,
                              TEvDataShard::TEvGetReadTableStreamStateResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetReadTableStreamStateRequest> PrepareRequest(const TActorContext &) override {
        auto *request = new TEvDataShard::TEvGetReadTableStreamStateRequest;
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();
        if (cgi.Has("opid")) {
            ui64 id;
            if (TryFromString(cgi.Get("opid"), id))
                request->Record.SetTxId(id);
        }
        return request;
    }
};

class TJsonProxyDataShardGetSlowOpProfiles : public TJsonProxyDataShard<TEvDataShard::TEvGetSlowOpProfilesRequest,
                                                                        TEvDataShard::TEvGetSlowOpProfilesResponse>
{
public:
    TJsonProxyDataShardGetSlowOpProfiles(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetSlowOpProfilesRequest,
                              TEvDataShard::TEvGetSlowOpProfilesResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetSlowOpProfilesRequest> PrepareRequest(const TActorContext &) override {
        return new TEvDataShard::TEvGetSlowOpProfilesRequest;
    }
};

class TJsonProxyDataShardGetRSInfo : public TJsonProxyDataShard<TEvDataShard::TEvGetRSInfoRequest,
                                                                TEvDataShard::TEvGetRSInfoResponse>
{
public:
    TJsonProxyDataShardGetRSInfo(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetRSInfoRequest,
                              TEvDataShard::TEvGetRSInfoResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetRSInfoRequest> PrepareRequest(const TActorContext &) override {
        return new TEvDataShard::TEvGetRSInfoRequest;
    }
};

class TJsonProxyDataShardGetDataHistogram : public TJsonProxyDataShard<TEvDataShard::TEvGetDataHistogramRequest,
                                                                       TEvDataShard::TEvGetDataHistogramResponse>
{
public:
    TJsonProxyDataShardGetDataHistogram(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyDataShard<TEvDataShard::TEvGetDataHistogramRequest,
                              TEvDataShard::TEvGetDataHistogramResponse>(event)
    {
    }

    TAutoPtr<TEvDataShard::TEvGetDataHistogramRequest> PrepareRequest(const TActorContext &) override {
        return new TEvDataShard::TEvGetDataHistogramRequest;
    }
};

} // namespace NKikimr::NCms
