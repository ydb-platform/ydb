#include <ydb/core/fq/libs/ydb/ydb_connection.h>
#include <ydb/core/fq/libs/ydb/sdk_table_client.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/util.h>

namespace NFq {


struct TSdkYdbConnection : public IYdbConnection {

    TSdkYdbConnection(
        const NKikimrConfig::TExternalStorage& config,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const NYdb::TDriver& driver)
        : Driver(driver)
        , TableClient(CreateSdkTableClient(driver, GetClientSettings<NYdb::NTable::TClientSettings>(config, credProviderFactory)))
        , Db(config.GetDatabase())
        , TablePathPrefix(JoinPath(Db, config.GetTablePrefix())) {
    }

    IYdbTableClient::TPtr GetTableClient() override {
        return TableClient;
    }

    TString GetTablePathPrefix() override {
        return TablePathPrefix;
    }

    TString GetDb() override {
        return Db;
    }

    TString GetTablePathPrefixWithoutDb() override {
        return TablePathPrefix;
    }


private:
    NYdb::TDriver Driver;
    IYdbTableClient::TPtr TableClient;
    const TString Db;
    const TString TablePathPrefix;
};

IYdbConnection::TPtr CreateSdkYdbConnection(
    const NKikimrConfig::TExternalStorage& config,
    const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
    const NYdb::TDriver& driver) {
    return MakeIntrusive<TSdkYdbConnection>(config, credProviderFactory, driver);
}

} // namespace NFq
