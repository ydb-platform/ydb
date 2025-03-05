#include <gtest/gtest.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/profiling/solomon/registry.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/sensor_service.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TSensorService, GetSensor)
{
    auto registry = New<TSolomonRegistry>();
    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->UpdateSensorServiceTreePeriod = TDuration::MilliSeconds(100);
    config->EnableSelfProfiling = false;
    auto exporter = New<TSolomonExporter>(config, registry);

    exporter->Start();

    auto gauge = TProfiler(registry, "/foo").Gauge("/bar/baz");
    gauge.Update(117.0);

    registry->Collect();
    auto sensorService = exporter->GetSensorService();

    auto valueByName = [&] {
        auto options = CreateEphemeralAttributes();
        options->Set("name", "yt/foo/bar/baz");

        return ConvertTo<double>(SyncYPathGet(sensorService, "", /*attributeFilter*/ {}, options));
    }();

    EXPECT_EQ(valueByName, 117.0);

    TDelayedExecutor::WaitForDuration(2 * config->UpdateSensorServiceTreePeriod);

    auto valueByPath = [&] {
        auto options = CreateEphemeralAttributes();
        options->Set("read_all_projections", false);

        return ConvertTo<double>(SyncYPathGet(sensorService, "/yt/foo/bar/baz", /*attributeFilter*/ {}, options));
    }();

    EXPECT_EQ(valueByPath, 117.0);

    exporter->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
