#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/base/location.h>
#include <ydb/core/protos/kqp.pb.h>

#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <cmath>

namespace NKikimr::NKqp {

TSimpleResourceStats CalcPeerStats(
    const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId, bool localDatacenterPolicy,
    std::function<double(const NKikimrKqp::TKqpProxyNodeResources& entry)> ExtractValue)
{
    if (data.empty())
        return TSimpleResourceStats(0.0, 0.0, 0);

    auto getDataCenterId = [](const auto& entry) {
        return entry.HasDataCenterId() ? entry.GetDataCenterId() : DataCenterToString(entry.GetDataCenterNumId());
    };

    double sum = 0;
    ui32 cnt = 0;
    for(const auto& entry: data) {
        if (getDataCenterId(entry) != selfDataCenterId && localDatacenterPolicy)
            continue;

        sum += ExtractValue(entry);
        ++cnt;
    }

    if (cnt == 1 || sum == 0)
        return TSimpleResourceStats(static_cast<double>(sum), 0.0, 0);

    const double dataSz = static_cast<double>(cnt);
    double mean = sum / dataSz;
    double deviation = 0;
    double xadd = 0;
    for(const auto& entry: data) {
        if (getDataCenterId(entry) != selfDataCenterId && localDatacenterPolicy)
            continue;

        xadd = ExtractValue(entry) - mean;
        deviation += xadd * xadd;
    }

    deviation = std::sqrt(deviation / (dataSz - 1));
    return TSimpleResourceStats(mean, deviation, static_cast<ui64>(std::llround(100.0f * deviation / mean)));
}

TPeerStats CalcPeerStats(const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId) {
    auto getCpu = [](const NKikimrKqp::TKqpProxyNodeResources& entry) {
        return entry.GetCpuUsage();
    };

    auto getSessionCount = [](const NKikimrKqp::TKqpProxyNodeResources& entry) {
        return static_cast<double>(entry.GetActiveWorkersCount());
    };

    return TPeerStats(
        CalcPeerStats(data, selfDataCenterId, true, getSessionCount),
        CalcPeerStats(data, selfDataCenterId, false, getSessionCount),

        CalcPeerStats(data, selfDataCenterId, true, getCpu),
        CalcPeerStats(data, selfDataCenterId, false, getCpu)
    );
}


}

template<>
void Out<NKikimr::NKqp::TSimpleResourceStats>(IOutputStream& out, const NKikimr::NKqp::TSimpleResourceStats& v) {
    out << "ResourceStats(Mean: " << v.Mean << ", CV: " << v.CV << ", Deviation: " << v.Deviation << ")";
}
