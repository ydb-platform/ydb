#include "make_config.h"

#include <util/datetime/base.h>
#include <util/string/printf.h>

#include <ydb/core/persqueue/utils.h>

namespace NKikimr::NPQ::NHelpers {

NKikimrPQ::TPQTabletConfig MakeConfig(const TMakeConfigParams& params)
{
    NKikimrPQ::TPQTabletConfig config;

    config.SetVersion(params.Version);

    for (auto& c : params.Consumers) {
        config.AddReadRules(c.Consumer);
        config.AddReadRuleGenerations(c.Generation);
    }

    for (const auto& e : params.AllPartitions) {
        auto* p = config.AddAllPartitions();
        p->SetPartitionId(e.Id);
        p->SetTabletId(e.TabletId);
        for (auto t : e.Children) {
            p->AddChildPartitionIds(t);
        }
        for (auto t : e.Parents) {
            p->AddParentPartitionIds(t);
        }
    }

    for (const auto& e : params.Partitions) {
        auto* p = config.AddPartitions();
        p->SetPartitionId(e.Id);
    }

    if (params.AllPartitions.empty() && params.Partitions.empty()) {
        for (ui32 id = 0; id < params.PartitionsCount; ++id) {
            config.AddPartitionIds(id);
        }
    }

    config.SetTopicName("rt3.dc1--account--topic");
    config.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
    config.SetFederationAccount("account");
    config.SetLocalDC(true);
    config.SetYdbDatabasePath("");

    config.SetMeteringMode(params.MeteringMode);
    config.MutablePartitionConfig()->SetLifetimeSeconds(TDuration::Hours(24).Seconds());
    config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(10 << 20);

    if (params.HugeConfig) {
        for (size_t i = 0; i < 2'500; ++i) {
            TString name = Sprintf("fake-consumer-%s-%" PRISZT,
                                   TString(3'000, 'a').data(), i);
            config.AddReadRules(name);
            config.AddReadRuleGenerations(1);
        }
    }

    Migrate(config);

    return config;
}

NKikimrPQ::TPQTabletConfig MakeConfig(ui64 version,
                                      const TVector<TCreateConsumerParams>& consumers,
                                      ui32 partitionsCount,
                                      NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode)
{
    TMakeConfigParams params;
    params.Version = version;
    params.Consumers = consumers;
    params.PartitionsCount = partitionsCount;
    params.MeteringMode = meteringMode;
    return MakeConfig(params);
}

NKikimrPQ::TBootstrapConfig MakeBootstrapConfig()
{
    return {};
}

}
