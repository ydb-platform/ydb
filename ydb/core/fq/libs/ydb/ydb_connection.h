#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/table_client.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/core/protos/config.pb.h>

namespace NFq {

struct IYdbConnection : public TThrRefBase {

    using TPtr = TIntrusivePtr<IYdbConnection>;

    virtual IYdbTableClient::TPtr GetTableClient() = 0;
    virtual TString GetTablePathPrefix() = 0;
    virtual TString GetDb() = 0;
    virtual TString GetTablePathPrefixWithoutDb() = 0;
};

IYdbConnection::TPtr CreateLocalYdbConnection(
    const TString& db,
    const TString& tablePathPrefix);

IYdbConnection::TPtr CreateSdkYdbConnection(
    const NKikimrConfig::TExternalStorage& config,
    const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
    const NYdb::TDriver& driver);

} // namespace NFq
