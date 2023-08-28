#pragma once

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {

inline std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(const TString& scope,
                                                                 const NFq::TComputeConfig& computeConfig,
                                                                 const NFq::NConfig::TYdbStorageConfig& connection,
                                                                 const TYqSharedResources::TPtr& yqSharedResources,
                                                                 const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    
    NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetConnection(scope);
    computeConnection.set_endpoint(connection.endpoint());
    computeConnection.set_database(connection.database());
    computeConnection.set_usessl(connection.usessl());

    auto tableSettings = GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection,
                                                                            credentialsProviderFactory);
    return std::make_shared<NYdb::NTable::TTableClient>(yqSharedResources->UserSpaceYdbDriver,
                                                        tableSettings);
}

TString GetV1StatFromV2Plan(const TString& plan);

} // namespace NFq
