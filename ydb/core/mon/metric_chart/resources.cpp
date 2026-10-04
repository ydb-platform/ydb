#include "resources.h"

#include <ydb/core/mon/mon.h>
#include <library/cpp/monlib/service/pages/resource_mon_page.h>

namespace NKikimr::NMetricChart {

void RegisterResources(NActors::TMon* mon) {
    using NMonitoring::TResourceMonPage;
    mon->Register(new TResourceMonPage("static/metric-chart/chart.js", "metric-chart/chart.js", TResourceMonPage::JAVASCRIPT));
    mon->Register(new TResourceMonPage("static/metric-chart/allocation.js", "metric-chart/allocation.js", TResourceMonPage::JAVASCRIPT));
    mon->Register(new TResourceMonPage("static/metric-chart/client.js", "metric-chart/client.js", TResourceMonPage::JAVASCRIPT));
    mon->Register(new TResourceMonPage("static/metric-chart/chart.css", "metric-chart/chart.css", TResourceMonPage::CSS));
}

} // namespace NKikimr::NMetricChart
