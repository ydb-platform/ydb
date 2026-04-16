#pragma once

#include <library/cpp/http/io/stream.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>
#include <library/cpp/http/simple/http_client.h>

using namespace NYql::NFmr;

std::pair<TString, TString> GetLogContext(const THttpInput& input);

TKeepAliveHttpClient::THeaders GetHeadersWithLogContext(const TKeepAliveHttpClient::THeaders& headers, bool addLogContext = true);

TKeepAliveHttpClient::THeaders GetFullHttpHeaders(
    const TKeepAliveHttpClient::THeaders& headers,
    IFmrTvmClient::TPtr tvmClient,
    TTvmId tvmId,
    bool addLogContext = true
);

void HandleHttpError(TKeepAliveHttpClient::THttpCode statusCode, TString httpResponse);

