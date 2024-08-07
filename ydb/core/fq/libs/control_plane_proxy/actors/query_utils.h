#pragma once

#include <util/generic/string.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>

namespace NFq {
namespace NPrivate {

TMaybe<TString> CreateSecretObjectQuery(const FederatedQuery::ConnectionSetting& setting,
                                        const TString& name,
                                        const TSigner::TPtr& signer,
                                        const TString& scope);

TMaybe<TString> DropSecretObjectQuery(const TString& name, const TString& scope);

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TSigner::TPtr& signer,
    const NConfig::TCommonConfig& common,
    bool replaceIfExists,
    const TString& scope);

TString MakeDeleteExternalDataSourceQuery(const TString& sourceName);

TString MakeCreateExternalDataTableQuery(const FederatedQuery::BindingContent& content,
                                         const TString& connectionName,
                                         bool replaceIfExists);

TString MakeDeleteExternalDataTableQuery(const TString& tableName);

} // namespace NPrivate
} // namespace NFq
