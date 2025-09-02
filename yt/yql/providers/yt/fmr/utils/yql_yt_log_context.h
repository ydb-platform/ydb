#pragma once

#include <library/cpp/http/io/stream.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <library/cpp/http/simple/http_client.h>

using namespace NYql::NFmr;

std::pair<TString, TString> GetLogContext(const THttpInput& input);

TKeepAliveHttpClient::THeaders GetHeadersWithLogContext(const TKeepAliveHttpClient::THeaders& headers, bool addLogContext = true);
