#pragma once

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {

template<typename T>
std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(
    const T& ev,
    const NFq::TComputeConfig& computeConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    auto scope = "yandexcloud://" + ev->Get()->FolderId;
    NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetConnection(scope);

    computeConnection.set_endpoint(ev->Get()->ComputeDatabase->connection().endpoint());
    computeConnection.set_database(ev->Get()->ComputeDatabase->connection().database());
    computeConnection.set_usessl(ev->Get()->ComputeDatabase->connection().usessl());

    auto tableSettings =
        GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection,
                                                         credentialsProviderFactory);
    return std::make_shared<NYdb::NTable::TTableClient>(yqSharedResources->UserSpaceYdbDriver,
                                                        tableSettings);
}

} // namespace NFq
