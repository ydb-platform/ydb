#pragma once

#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/server/response.h>

#include <vector>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>

namespace NYql::NFmr {

TMaybe<TString> GetTvmServiceTicketFromHeaders(const THttpHeaders& headers);

void CheckTvmServiceTicket(const THttpHeaders& headers, IFmrTvmClient::TPtr tvmClient, const std::vector<TTvmId> allowedSourceTvmIds);

TMaybe<TFmrTvmSpec> ParseFmrTvmSpec(const TMaybe<TString>& tvmSpecFilePath);

TMaybe<TString> ParseFmrTvmSecretFile(const TMaybe<TString>& tvmSecretFilePath);

} // namespace NYql::NFmr
