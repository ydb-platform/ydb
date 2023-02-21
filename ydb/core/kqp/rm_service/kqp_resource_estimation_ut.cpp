#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
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

    auto est = EstimateTaskResources(task, config, 1);
    UNIT_ASSERT_EQUAL(2, est.ChannelBuffersCount);
    UNIT_ASSERT_EQUAL(est.ChannelBufferMemoryLimit, config.GetChannelBufferSize());

    // add more channels, to be more then 256
    input = task.MutableInputs()->Add();
    for (int i = 0; i < 100; ++i) {
        input->MutableChannels()->Add();
    }

    est = EstimateTaskResources(task, config, 1);
    UNIT_ASSERT_EQUAL(2, est.ChannelBuffersCount);

    UNIT_ASSERT(est.ChannelBufferMemoryLimit == config.GetChannelBufferSize());
    UNIT_ASSERT(est.ChannelBufferMemoryLimit >= config.GetMinChannelBufferSize());
}

} // suite KqpResourceEstimation

} // namespace NKikimr::NKqp
