#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKnn {

struct TRemoteEmbedderHttpResponse {
    ui32 Status;
    TString Body;
    bool TooLarge = false;
};

NThreading::TFuture<TRemoteEmbedderHttpResponse> PostRemoteEmbedderRequest(
    TString host, ui16 port, bool useTls, TString request);
NThreading::TFuture<void> WaitBeforeRemoteEmbedderRequest(TDuration delay);

} // namespace NKnn
