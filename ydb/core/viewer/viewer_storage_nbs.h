#pragma once

#include "json_pipe_req.h"

namespace NKikimr::NViewer {

class TStorageNbsDiscovery : public TViewerPipeClient {
    using TThis = TStorageNbsDiscovery;
    using TBase = TViewerPipeClient;
    using TPoolsResponse = NSysView::TEvSysView::TEvGetStoragePoolsResponse;

    TRequestResponse<TPoolsResponse> PoolsResponse;

public:
    TStorageNbsDiscovery(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev, "/storage/nbs")
    {}

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
        request->Record.SetNbsDiscoveryOnly(true);
        PoolsResponse = MakeRequestToTablet<TPoolsResponse>(GetBSControllerId(), request.release());
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TPoolsResponse, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TPoolsResponse::TPtr& ev) {
        if (PoolsResponse.Set(std::move(ev), /*updateCache=*/ false)) {
            RequestDone();
        }
    }

    void ReplyAndPassAway() override {
        NJson::TJsonValue result(NJson::JSON_MAP);
        if (PoolsResponse.IsOk() && PoolsResponse->Record.HasHasDDiskPools()) {
            result["Configured"] = PoolsResponse->Record.GetHasDDiskPools();
        } else {
            result["Configured"] = NJson::TJsonValue(NJson::JSON_NULL);
        }
        TBase::ReplyAndPassAway(GetHTTPOKJSON(result));
    }
};

} // namespace NKikimr::NViewer
