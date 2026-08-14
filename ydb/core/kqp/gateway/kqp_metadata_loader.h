#pragma once

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/common/compilation/user_facing_trace.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/system/mutex.h>

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {

// only exposed to be unit-tested
NExternalSource::TAuth MakeAuth(const NYql::TExternalSource& metadata);
std::shared_ptr<NExternalSource::TMetadata> ConvertToExternalSourceMetadata(const NYql::TKikimrTableMetadata& tableMetadata);
bool EnrichMetadata(NYql::TKikimrTableMetadata& tableMetadata, const NExternalSource::TMetadata& dynamicMetadata);

class TUserFacingCompileDependencyCollector {
public:
    void Record(EUserFacingCompileDependency dependency, TString target, TInstant start, TInstant end,
            EUserFacingCompileStatus status) {
        std::lock_guard guard(Mutex);
        if (Spans.size() < MaxSpans) {
            Spans.push_back({dependency, std::move(target), start, end, status});
        } else {
            ++Dropped;
        }
    }

    std::shared_ptr<const TUserFacingCompileTrace> Snapshot() const {
        std::lock_guard guard(Mutex);
        return std::make_shared<const TUserFacingCompileTrace>(TUserFacingCompileTrace{
            .Spans = Spans,
            .Dropped = Dropped,
        });
    }

private:
    static constexpr size_t MaxSpans = 64;
    mutable std::mutex Mutex;
    std::vector<TUserFacingCompileSpan> Spans;
    size_t Dropped = 0;
};

class TKqpTableMetadataLoader : public NYql::IKikimrGateway::IKqpTableMetadataLoader {
public:

    explicit TKqpTableMetadataLoader(const TString& cluster,
        TActorSystem* actorSystem,
        NYql::TKikimrConfiguration::TPtr config,
        bool needCollectSchemeData = false,
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup = std::nullopt,
        std::shared_ptr<TUserFacingCompileDependencyCollector> userFacingCompileCollector = {})
        : Cluster(cluster)
        , NeedCollectSchemeData(needCollectSchemeData)
        , ActorSystem(actorSystem)
        , Config(config)
        , TempTablesState(std::move(tempTablesState))
        , FederatedQuerySetup(federatedQuerySetup)
        , UserFacingCompileCollector(std::move(userFacingCompileCollector))
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

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadSysViewRewrittenMetadata(
        const TString& cluster, const TString& table, const TString& sysViewName);

    const TString Cluster;
    TVector<NKikimrKqp::TKqpTableMetadataProto> CollectedSchemeData;
    TMutex Lock;
    bool NeedCollectSchemeData;
    TActorSystem* ActorSystem;
    NYql::TKikimrConfiguration::TPtr Config;
    TKqpTempTablesState::TConstPtr TempTablesState;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    std::shared_ptr<TUserFacingCompileDependencyCollector> UserFacingCompileCollector;
};

} // namespace NKikimr::NKqp
