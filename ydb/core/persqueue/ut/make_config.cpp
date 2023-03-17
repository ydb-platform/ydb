#include "make_config.h"

namespace NKikimr::NPQ::NHelpers {

NKikimrPQ::TPQTabletConfig MakeConfig(ui64 version,
                                      const TVector<TCreateConsumerParams>& consumers,
                                      ui32 partitionsCount)
{
    NKikimrPQ::TPQTabletConfig config;

    config.SetVersion(version);

    for (auto& c : consumers) {
        config.AddReadRules(c.Consumer);
        config.AddReadRuleGenerations(c.Generation);
    }

    for (ui32 id = 0; id < partitionsCount; ++id) {
        config.AddPartitionIds(id);
    }

    config.SetTopicName("rt3.dc1--account--topic");
    config.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
    config.SetFederationAccount("account");
    config.SetLocalDC(true);
    config.SetYdbDatabasePath("");

    config.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

    return config;
}

NKikimrPQ::TBootstrapConfig MakeBootstrapConfig()
{
    return {};
}

}
