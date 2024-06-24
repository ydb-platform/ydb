#pragma once

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/system/mutex.h>

namespace NKikimr::NKqp {

// only exposed to be unit-tested
NExternalSource::TAuth MakeAuth(const NYql::TExternalSource& metadata);
std::shared_ptr<NExternalSource::TMetadata> ConvertToExternalSourceMetadata(const NYql::TKikimrTableMetadata& tableMetadata);
bool EnrichMetadata(NYql::TKikimrTableMetadata& tableMetadata, const NExternalSource::TMetadata& dynamicMetadata);

class TKqpTableMetadataLoader : public NYql::IKikimrGateway::IKqpTableMetadataLoader {
public:

    explicit TKqpTableMetadataLoader(const TString& cluster,
        TActorSystem* actorSystem,
        NYql::TKikimrConfiguration::TPtr config,
        bool needCollectSchemeData = false,
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr)
        : Cluster(cluster)
        , NeedCollectSchemeData(needCollectSchemeData)
        , ActorSystem(actorSystem)
        , Config(config)
        , TempTablesState(std::move(tempTablesState))
    {}

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadTableMetadata(
        const TString& cluster, const TString& table, const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData();

    ~TKqpTableMetadataLoader() = default;

protected:

    std::weak_ptr<TKqpTableMetadataLoader> weak_from_base() {
        return std::static_pointer_cast<TKqpTableMetadataLoader>(shared_from_this());
    }

private:
    template<typename TPath>
    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadTableMetadataCache(
        const TString& cluster, const TPath& id, NYql::IKikimrGateway::TLoadTableMetadataSettings settings, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadIndexMetadataByPathId(
        const TString& cluster, const NKikimr::TIndexId& indexId, const TString& tableName, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadIndexMetadata(
        NYql::IKikimrGateway::TTableMetadataResult& loadTableMetadataResult, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);

    void OnLoadedTableMetadata(NYql::IKikimrGateway::TTableMetadataResult& loadTableMetadataResult);

    const TString Cluster;
    TVector<NKikimrKqp::TKqpTableMetadataProto> CollectedSchemeData;
    TMutex Lock;
    bool NeedCollectSchemeData;
    TActorSystem* ActorSystem;
    NYql::TKikimrConfiguration::TPtr Config;
    TKqpTempTablesState::TConstPtr TempTablesState;
};

} // namespace NKikimr::NKqp
