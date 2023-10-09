#include "ut_common.h"

#include "ut_common.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

const ui32 partitionsN = 32;
const TString topicName = "topic";


namespace {

void CreateDatabase(TTestEnv& env, const TString& databaseName) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 2);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);
    subdomainSettings.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

bool CheckCounter(::NMonitoring::TDynamicCounterPtr group, const char* sensorName, ui32 refValue,
                  bool isDerivative) {
    auto value = group->GetNamedCounter("name", sensorName, isDerivative)->Val();
    Cerr << "CHECK COUNTER " << sensorName << " wait " << refValue << " got " << value << "\n";
    return (value == refValue);
}

bool CheckLtCounter(::NMonitoring::TDynamicCounterPtr group, const char* sensorName, ui32 refValue,
                  bool isDerivative) {
    auto value = group->GetNamedCounter("name", sensorName, isDerivative)->Val();
    Cerr << "CHECK COUNTER " << sensorName << " wait less than " << refValue << " got " << value << "\n";
    return (value <= refValue);
}

bool CheckLabeledCounters(::NMonitoring::TDynamicCounterPtr databaseGroup, const TString& dbId,
    std::function<bool(::NMonitoring::TDynamicCounterPtr)> particularCountersCheck) {
    bool isGood{true};
    Y_UNUSED(dbId);

    auto topicGroup = databaseGroup
            ->GetSubgroup("cloud_id", "")
            ->GetSubgroup("folder_id", "")
            ->GetSubgroup("database_id", "")
            ->GetSubgroup("host", "")
            ->GetSubgroup("topic", topicName);
    {
        {
            TStringStream ss;
            topicGroup->OutputHtml(ss);
            Cerr << ss.Str() << Endl;
        }

        isGood &= particularCountersCheck(topicGroup);
    }

    return isGood;
}

void GetCounters(TTestEnv& env, const TString& databaseName, const TString& databasePath,
                 std::function<bool(::NMonitoring::TDynamicCounterPtr)> particularCountersCheck) {
    for (size_t iter = 0; iter < 35; ++iter) {
        Cerr << "iteration " << iter << Endl;

        bool checkDb = false;

        for (ui32 nodeId = 0; nodeId < env.GetServer().GetRuntime()->GetNodeCount(); ++nodeId) {
            auto counters = env.GetServer().GetRuntime()->GetAppData(nodeId).Counters;
            auto labeledGroup = GetServiceCounters(counters, "labeled_serverless", false);
            Y_ABORT_UNLESS(labeledGroup);

            auto databaseGroup = labeledGroup->FindSubgroup("database", databasePath);
            if (databaseGroup) {
                checkDb = CheckLabeledCounters(databaseGroup, databaseName, particularCountersCheck);
            }
        }

        if (checkDb) {
            return;
        }

        Sleep(TDuration::Seconds(10));
    }
    UNIT_ASSERT_C(false, "out of 35 iterations with delay 10s");
}

} // namespace

Y_UNIT_TEST_SUITE(LabeledDbCounters) {

    Y_UNIT_TEST(OneTablet) {
        TTestEnv env(1, 2, 0, 1, true);
        const TString databaseName = NPQ::TTabletPreparationParameters().databaseId;
        const TString databasePath = NPQ::TTabletPreparationParameters().databasePath;
        auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();
        auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
            bool isGood{true};

            isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN, false);
            isGood &= CheckCounter(topicGroup, "topic.partition.write.speed_limit_bytes_per_second", 50'000'000, false);
            isGood &= CheckCounter(topicGroup, "topic.producers_count", 0, false);

            return isGood;
        };

        CreateDatabase(env, databaseName);
        NPQ::PQTabletPrepare({.partitions=partitionsN}, {}, *env.GetServer().GetRuntime(),
                                 env.GetPqTabletIds()[0], edge);
        GetCounters(env, databaseName, databasePath, check);
    }


    Y_UNIT_TEST(OneTabletRemoveCounters) {
        TTestEnv env(1, 2, 0, 1, true);
        const TString databaseName = NPQ::TTabletPreparationParameters().databaseId;
        const TString databasePath = NPQ::TTabletPreparationParameters().databasePath;
        auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();
        auto checkExists = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
            bool isGood{true};
            auto consumerGroup = topicGroup->FindSubgroup("consumer", "consumer");
            if (!consumerGroup)
                return false;
            isGood &= CheckCounter(consumerGroup, "topic.partition.alive_count", partitionsN, false);

            return isGood;
        };

        CreateDatabase(env, databaseName);
        NPQ::PQTabletPrepare({.partitions=partitionsN}, {{"consumer", false}}, *env.GetServer().GetRuntime(),
                                 env.GetPqTabletIds()[0], edge);
        GetCounters(env, databaseName, databasePath, checkExists);

        NPQ::PQTabletPrepare({.partitions=partitionsN}, {}, *env.GetServer().GetRuntime(),
                                 env.GetPqTabletIds()[0], edge);
        //TODO: fix clearence of groups in sys view service/processor
/*        auto checkNotExists = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
            auto consumerGroup = topicGroup->FindSubgroup("consumer", "consumer");
            return !consumerGroup;
        };

        GetCounters(env, databaseName, databasePath, checkNotExists);
*/

    }


    Y_UNIT_TEST(OneTabletRestart) {
        TTestEnv env(1, 2, 0, 1, true);
        const TString databaseName = NPQ::TTabletPreparationParameters().databaseId;
        const TString databasePath = NPQ::TTabletPreparationParameters().databasePath;
        auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        CreateDatabase(env, databaseName);
        NPQ::PQTabletPrepare({.partitions=partitionsN}, {}, *env.GetServer().GetRuntime(),
                                 env.GetPqTabletIds()[0], edge);

        {
            auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
                bool isGood{true};

                {
                    TStringStream ss;
                    topicGroup->OutputHtml(ss);
                    Cerr << ss.Str() << Endl;
                }

                isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN, false);
                isGood &= CheckCounter(topicGroup, "topic.partition.write.speed_limit_bytes_per_second", 50'000'000, false);
                isGood &= CheckCounter(topicGroup, "topic.producers_count", 0, false);

                return isGood;
            };
            GetCounters(env, databaseName, databasePath, check);
        }

        Sleep(TDuration::Seconds(60));
        env.GetServer().GetRuntime()->Register(CreateTabletKiller(env.GetPqTabletIds()[0]));

        {
            auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
                bool isGood{true};

                isGood &= CheckLtCounter(topicGroup, "topic.partition.uptime_milliseconds_min",
                                         TDuration::Seconds(60).MilliSeconds() + 200, false);
                isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN, false);
                return isGood;
            };
            GetCounters(env, databaseName, databasePath, check);
        }
    }

    Y_UNIT_TEST(TwoTablets) {
        TTestEnv env(1, 2, 0, 2, true);
        const TString databaseName = NPQ::TTabletPreparationParameters().databaseId;
        const TString databasePath = NPQ::TTabletPreparationParameters().databasePath;
        auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
            bool isGood{true};

            isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN*2, false);
            isGood &= CheckCounter(topicGroup, "topic.partition.write.speed_limit_bytes_per_second", 50'000'000, false);
            isGood &= CheckCounter(topicGroup, "topic.producers_count", 0, false);

            return isGood;
        };

        CreateDatabase(env, databaseName);
        for (auto& tbId : env.GetPqTabletIds()) {
            NPQ::PQTabletPrepare({.partitions=partitionsN}, {}, *env.GetServer().GetRuntime(),
                                     tbId, env.GetServer().GetRuntime()->AllocateEdgeActor());
        }

        GetCounters(env, databaseName, databasePath, check);
    }

    Y_UNIT_TEST(TwoTabletsKillOneTablet) {
        TTestEnv env(1, 2, 0, 2, true);
        const TString databaseName = NPQ::TTabletPreparationParameters().databaseId;
        const TString databasePath = NPQ::TTabletPreparationParameters().databasePath;
        auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();
        CreateDatabase(env, databaseName);
        for (auto& tbId : env.GetPqTabletIds()) {
            NPQ::PQTabletPrepare({.partitions=partitionsN}, {}, *env.GetServer().GetRuntime(),
                                     tbId, edge);
        }

        {
            auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
                bool isGood{true};

                isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN*2, false);
                isGood &= CheckCounter(topicGroup, "topic.partition.write.speed_limit_bytes_per_second", 50'000'000, false);
                isGood &= CheckCounter(topicGroup, "topic.producers_count", 0, false);

                return isGood;
            };

            GetCounters(env, databaseName, databasePath, check);
        }

        for (ui32 i = 0; i < env.GetServer().StaticNodes() + env.GetServer().DynamicNodes(); i++) {
            env.GetClient().MarkNodeInHive(env.GetServer().GetRuntime(), i, false);
        }
        env.GetServer().GetRuntime()->Register(CreateTabletKiller(env.GetPqTabletIds()[0]));

        {
            auto check = [](::NMonitoring::TDynamicCounterPtr topicGroup) {
                bool isGood{true};

                isGood &= CheckCounter(topicGroup, "topic.partition.alive_count", partitionsN, false);

                return isGood;
            };

            GetCounters(env, databaseName, databasePath, check);
        }
    }
}

} // NSysView
} // NKikimr
