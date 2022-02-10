#include <ydb/core/kqp/rm/kqp_resource_estimation.h> 
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpResourceEstimation) {

Y_UNIT_TEST(TestChannelSize) {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;
    config.SetChannelBufferSize(8_MB);
    config.SetMinChannelBufferSize(2_MB);
    config.SetMaxTotalChannelBuffersSize(2_GB);
    config.SetMkqlLightProgramMemoryLimit(100);

    NYql::NDqProto::TDqTask task;

    // 100 input channels
    auto* input = task.MutableInputs()->Add();
    for (int i = 0; i < 100; ++i) {
        input->MutableChannels()->Add();
    }

    // 100 input channels
    input = task.MutableInputs()->Add();
    for (int i = 0; i < 100; ++i) {
        input->MutableChannels()->Add();
    }

    auto* output = task.MutableOutputs()->Add();
    output->MutableChannels()->Add();

    auto est = EstimateTaskResources(task, 0, 0, config);
    UNIT_ASSERT_EQUAL(201, est.ChannelBuffersCount);
    UNIT_ASSERT_EQUAL(est.ChannelBufferMemoryLimit, config.GetChannelBufferSize());

    // add more channels, to be more then 256
    input = task.MutableInputs()->Add();
    for (int i = 0; i < 100; ++i) {
        input->MutableChannels()->Add();
    }

    est = EstimateTaskResources(task, 0, 0, config);
    UNIT_ASSERT_EQUAL(301, est.ChannelBuffersCount);

    UNIT_ASSERT(est.ChannelBufferMemoryLimit < config.GetChannelBufferSize());
    UNIT_ASSERT(est.ChannelBufferMemoryLimit >= config.GetMinChannelBufferSize());
}

Y_UNIT_TEST(TestScanBufferSize) {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;
    config.SetScanBufferSize(10_MB);
    config.SetMaxTotalScanBuffersSize(50_MB);
    config.SetMinScanBufferSize(2_MB);
    config.SetChannelBufferSize(100);
    config.SetMinChannelBufferSize(100);
    config.SetMaxTotalChannelBuffersSize(200_GB);
    config.SetMkqlLightProgramMemoryLimit(100);

    NYql::NDqProto::TDqTask task;

    for (int i = 1; i <= 5; ++i) {
        auto est = EstimateTaskResources(task, i, i, config);
        UNIT_ASSERT_VALUES_EQUAL(10_MB, est.ScanBufferMemoryLimit);
        UNIT_ASSERT_VALUES_EQUAL(i, est.ScanBuffersCount);
    }

    for (int i = 6; i <= 24; ++i) {
        auto est = EstimateTaskResources(task, i, i, config);
        UNIT_ASSERT(10_MB >= est.ScanBufferMemoryLimit);
        UNIT_ASSERT(2_MB < est.ScanBufferMemoryLimit);
        UNIT_ASSERT_VALUES_EQUAL(i, est.ScanBuffersCount);
    }

    for (int i = 25; i <= 30; ++i) {
        auto est = EstimateTaskResources(task, i, i, config);
        UNIT_ASSERT_VALUES_EQUAL(2_MB, est.ScanBufferMemoryLimit);
        UNIT_ASSERT_VALUES_EQUAL(i, est.ScanBuffersCount);
    }
}

} // suite KqpResourceEstimation

} // namespace NKikimr::NKqp
