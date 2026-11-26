#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKnn {

struct TRemoteEmbedderHttpResponse {
    ui32 Status;
    TString Body;
    bool InvalidResponse = false;
};

NThreading::TFuture<TRemoteEmbedderHttpResponse> PostRemoteEmbedderRequest(
    TString host, ui16 port, bool useTls, TString request, TDuration timeout = TDuration::Seconds(60));
NThreading::TFuture<void> WaitBeforeRemoteEmbedderRequest(TDuration delay);

} // namespace NKnn
