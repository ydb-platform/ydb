#pragma once

#include <util/generic/string.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>

namespace NFq {
namespace NPrivate {

TMaybe<TString> CreateSecretObjectQuery(const FederatedQuery::IamAuth& auth,
                                        const TString& name,
                                        const TSigner::TPtr& signer);

TMaybe<TString> DropSecretObjectQuery(const FederatedQuery::IamAuth& auth,
                                      const TString& name,
                                      const TSigner::TPtr& signer);

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TString& objectStorageEndpoint,
    const TSigner::TPtr& signer);

TString MakeDeleteExternalDataSourceQuery(const TString& sourceName);

TString MakeCreateExternalDataTableQuery(const FederatedQuery::BindingContent& content,
                                         const TString& connectionName);

TString MakeDeleteExternalDataTableQuery(const TString& tableName);

} // namespace NPrivate
} // namespace NFq
