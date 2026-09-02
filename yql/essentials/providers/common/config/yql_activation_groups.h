#pragma once

#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

class TGatewaysConfig;

namespace NCommon {

TVector<TString> ApplyActivationGroupsInplace(
    TGatewaysConfig& gateways,
    const TString& username,
    const TCredentials::TPtr& credentials,
    const TQContext& qContext);

} // namespace NCommon
} // namespace NYql
