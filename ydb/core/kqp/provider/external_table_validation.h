#pragma once

#include "yql_kikimr_gateway.h"
#include <ydb/core/external_sources/external_source_factory.h>

namespace NYql {

using TLoadExternalTableMetadata = std::function<NThreading::TFuture<IKikimrGateway::TTableMetadataResult>(const TString&, bool)>;

NThreading::TFuture<IKikimrGateway::TGenericResult> ValidateExternalTableLocation(
    const TString& table, const TString& dataSource, const TString& location, bool existingOk,
    const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& factory,
    const TLoadExternalTableMetadata& loadMetadata);

} // namespace NYql
