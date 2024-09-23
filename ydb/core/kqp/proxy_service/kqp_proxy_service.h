#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

namespace NKikimrConfig {
    class TLogConfig;
    class TTableServiceConfig;
    class TQueryServiceConfig;
    class TMetadataProviderConfig;
}

namespace NKikimrKqp {
    class TKqpSetting;
    class TKqpProxyNodeResources;
}

namespace NKikimr::NKqp {

class IQueryReplayBackendFactory;
struct TKqpProxySharedResources;

struct TSimpleResourceStats {
    double Mean;
    double Deviation;
    ui64 CV;

    TSimpleResourceStats(double mean, double deviation, ui64 cv)
        : Mean(mean)
        , Deviation(deviation)
        , CV(cv)
    {}
};

struct TPeerStats {
    TSimpleResourceStats LocalSessionCount;
    TSimpleResourceStats CrossAZSessionCount;

    TSimpleResourceStats LocalCpu;
    TSimpleResourceStats CrossAZCpu;


    TPeerStats(TSimpleResourceStats localSessionsCount, TSimpleResourceStats crossAZSessionCount,
               TSimpleResourceStats localCpu, TSimpleResourceStats crossAZCpu)

        : LocalSessionCount(localSessionsCount)
        , CrossAZSessionCount(crossAZSessionCount)
        , LocalCpu(localCpu)
        , CrossAZCpu(crossAZCpu)
    {}
};

TSimpleResourceStats CalcPeerStats(
    const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId, bool localDatacenterPolicy,
    std::function<double(const NKikimrKqp::TKqpProxyNodeResources& entry)> ExtractValue);

TPeerStats CalcPeerStats(const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId);

IActor* CreateKqpProxyService(const NKikimrConfig::TLogConfig& logConfig,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    TVector<NKikimrKqp::TKqpSetting>&& settings,
    std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
    );

}  // namespace NKikimr::NKqp
