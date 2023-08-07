#pragma once

#include <ydb/core/base/appdata.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/datetime/base.h>

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
    const NKikimrProto::TTokenAccessorConfig& tokenAccessorConfig,
    TVector<NKikimrKqp::TKqpSetting>&& settings,
    std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources);

}  // namespace NKikimr::NKqp
