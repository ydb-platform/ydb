#pragma once

#include "ydb_common_ut.h"

#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <utility>

namespace NYdb {

struct TRelativeDatabaseFixture {
    static constexpr const char* PoolKind = "mydb";

    TKikimrWithGrpcAndRootSchema Server;
    ui16 Port;
    TString Endpoint;

    explicit TRelativeDatabaseFixture(const TString& database)
        : Server({}, {}, {}, false, nullptr, [&](auto& settings) {
            settings.FeatureFlags.SetEnableRelativePaths(true);
            settings.StoragePoolTypes.clear();
            settings.AddStoragePool(PoolKind, database + ':' + PoolKind);
        })
    {
        if (const auto parent = database.substr(0, database.rfind('/')); parent != "/Root") {
            TDriver rootDriver(TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << Server.GetPort()).SetDatabase("/Root")
                .SetDiscoveryMode(EDiscoveryMode::Sync));
            const auto result = NScheme::TSchemeClient(rootDriver).MakeDirectory(parent).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(database);
        auto* storage = request.mutable_resources()->add_storage_units();
        storage->set_unit_kind(PoolKind);
        storage->set_count(1);
        Server.Tenants_->CreateTenant(std::move(request));
        Port = Server.GetPortManager().GetPort();
        Server.GetServer().EnableGRpc(Port, Server.Tenants_->List(database).front(), database);
        Endpoint = TStringBuilder() << "localhost:" << Port;
    }

    TDriver CreateDriver(const TString& database) const {
        return TDriver(TDriverConfig().SetEndpoint(Endpoint).SetDatabase(database).SetDiscoveryMode(EDiscoveryMode::Sync));
    }
};

} // namespace NYdb
