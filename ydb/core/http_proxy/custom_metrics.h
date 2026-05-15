#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

using namespace Ydb::DataStreams::V1;

template<class TProtoRequest>
void FillInputCustomMetrics(const TProtoRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    Y_UNUSED(request, httpContext, ctx);
}

template<class TProtoResult>
void FillOutputCustomMetrics(const TProtoResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    Y_UNUSED(result, httpContext, ctx);
}

TVector<std::pair<TString, TString>> BuildLabels(const TString& method, const THttpRequestContext& httpContext, const TString& name, bool setStreamPrefix = false);

static const bool setStreamPrefix{true};


} // namespace NKikimr::NHttpProxy
