#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/public/api/client/nc_private/iam/v1/token_exchange_service.grpc.pb.h>

namespace NMVP {

bool BuildOAuth2ExchangeRequestFromConfig(const NMvp::TOAuth2Exchange* tokenExchangeInfo,
                                          nebius::iam::v1::ExchangeTokenRequest& request,
                                          TString& error);

} // namespace NMVP
