#include "hostname.h"

#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/list.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonitoringUrlTest)
{
    Y_UNIT_TEST(ShouldReturnCorrectMonitoringVolumeUrl)
    {
        NProto::TDiagnosticsConfig protoConfig;
        auto* monitorngUrlConfig = protoConfig.MutableMonitoringUrlData();
        monitorngUrlConfig->SetMonitoringClusterName("company_devlab");
        monitorngUrlConfig->SetMonitoringUrl("https://monitoring.company.com");
        monitorngUrlConfig->SetMonitoringProject("development.cloud");
        monitorngUrlConfig->SetMonitoringVolumeDashboard(
            "monitoring_dashboard");
        monitorngUrlConfig->SetMonitoringUrlTemplate(
            "{MonitoringUrl}/projects/{MonitoringProject}/dashboards/"
            "{MonitoringVolumeDashboard}?from=now-1d&to=now&refresh=60000&p."
            "cluster={MonitoringClusterName}&p.volume={diskId}");

        TDiagnosticsConfig config(protoConfig);

        auto url = GetMonitoringVolumeUrl(config, "vol-1234567890abcdef");

        UNIT_ASSERT_VALUES_EQUAL(
            "https://monitoring.company.com/projects/development.cloud/"
            "dashboards/monitoring_dashboard"
            "?from=now-1d&to=now&refresh=60000"
            "&p.cluster=company_devlab"
            "&p.volume=vol-1234567890abcdef",
            url);
    }

    Y_UNIT_TEST(ShouldSubstituteAllTemplateFields)
    {
        NProto::TDiagnosticsConfig protoConfig;
        auto* monitorngUrlConfig = protoConfig.MutableMonitoringUrlData();

        monitorngUrlConfig->SetMonitoringUrl("1");
        monitorngUrlConfig->SetMonitoringProject("2");
        monitorngUrlConfig->SetMonitoringClusterName("3");
        monitorngUrlConfig->SetMonitoringVolumeDashboard("4");
        monitorngUrlConfig->SetMonitoringNBSAlertsDashboard("5");
        monitorngUrlConfig->SetMonitoringNBSTVDashboard("6");
        monitorngUrlConfig->SetMonitoringYDBProject("7");
        monitorngUrlConfig->SetMonitoringYDBGroupDashboard("8");

        monitorngUrlConfig->SetMonitoringUrlTemplate(
            "{MonitoringUrl}/"
            "{MonitoringProject}/"
            "{MonitoringClusterName}/"
            "{MonitoringVolumeDashboard}/"
            "{MonitoringNBSAlertsDashboard}/"
            "{MonitoringNBSTVDashboard}/"
            "{MonitoringYDBProject}/"
            "{MonitoringYDBGroupDashboard}/"
            "{diskId}");

        TDiagnosticsConfig config(protoConfig);
        auto url = GetMonitoringVolumeUrl(config, "9");

        UNIT_ASSERT_VALUES_EQUAL("1/2/3/4/5/6/7/8/9", url);
    }
}

}   // namespace

}   // namespace NCloud::NBlockStore
