#pragma once

namespace NActors {
class TMon;
}

namespace NKikimr::NMetricChart {

void RegisterResources(NActors::TMon* mon);

} // namespace NKikimr::NMetricChart
