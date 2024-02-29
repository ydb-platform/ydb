#pragma once

#include "validator.h"

#include <ydb/core/base/localdb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/protos/interconnect.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

namespace NKikimr::NConsole {
namespace NTests {

inline void RemoveNode(ui32 id,
                       NKikimrConfig::TStaticNameserviceConfig &config)
{
    for (size_t i = 0; i < config.NodeSize(); ++i) {
        if (config.GetNode(i).GetNodeId() == id) {
            config.MutableNode()->DeleteSubrange(i, 1);
            break;
        }
    }
}

inline void AddNode(ui32 id,
                    const TString &host,
                    ui16 port,
                    const TString &resolveHost,
                    const TString &addr,
                    const TString &dc,
                    NKikimrConfig::TStaticNameserviceConfig &config)
{
    auto &node = *config.AddNode();
    node.SetNodeId(id);
    node.SetAddress(addr);
    node.SetPort(port);
    node.SetHost(host);
    node.SetInterconnectHost(resolveHost);
    if (dc)
        node.MutableWalleLocation()->SetDataCenter(dc);
}

inline void UpdateNode(ui32 id,
                       const TString &host,
                       ui16 port,
                       const TString &resolveHost,
                       const TString &addr,
                       const TString &dc,
                       NKikimrConfig::TStaticNameserviceConfig &config)
{
    RemoveNode(id, config);
    AddNode(id, host, port, resolveHost, addr, dc, config);
}

inline NKikimrConfig::TStaticNameserviceConfig MakeDefaultNameserviceConfig()
{
    NKikimrConfig::TStaticNameserviceConfig res;
    AddNode(1, "host1", 101, "rhost1", "addr1", "dc1", res);
    AddNode(2, "host2", 102, "rhost2", "addr2", "dc1", res);
    AddNode(3, "host3", 103, "rhost3", "addr3", "dc1", res);
    AddNode(4, "host4", 104, "rhost4", "addr4", "dc1", res);
    res.SetClusterUUID("cluster_uuid_1");
    return res;
}

inline void AddQueue(const TString &name,
                     ui32 weight,
                     TVector<ui64> limits,
                     NKikimrResourceBroker::TResourceBrokerConfig &config)
{
    auto queue = config.AddQueues();
    queue->SetName(name);
    queue->SetWeight(weight);
    for (auto limit : limits)
        queue->MutableLimit()->AddResource(limit);
}

inline void AddTask(const TString &name,
                    const TString &queue,
                    ui64 defaultDuration,
                    NKikimrResourceBroker::TResourceBrokerConfig &config)
{
    auto task = config.AddTasks();
    task->SetName(name);
    task->SetQueueName(queue);
    task->SetDefaultDuration(defaultDuration);
}

inline NKikimrResourceBroker::TResourceBrokerConfig MakeDefaultResourceBrokerConfig()
{
    NKikimrResourceBroker::TResourceBrokerConfig res;
    AddQueue(NLocalDb::DefaultQueueName, 100, {1}, res);
    AddTask(NLocalDb::UnknownTaskName, NLocalDb::DefaultQueueName, 10000000, res);
    res.MutableResourceLimit()->AddResource(100);
    return res;
}

inline void AddTablet(NKikimrConfig::TBootstrap::ETabletType type,
                      const TVector<ui32> &nodes,
                      NKikimrConfig::TBootstrap &config)
{
    auto &tablet = *config.AddTablet();
    tablet.SetType(type);
    for (ui32 node : nodes)
        tablet.AddNode(node);
}

inline void RemoveTablet(NKikimrConfig::TBootstrap::ETabletType type,
                         NKikimrConfig::TBootstrap &config)
{
    for (size_t i = 0; i < config.TabletSize(); ++i) {
        if (config.GetTablet(i).GetType() == type) {
            config.MutableTablet()->DeleteSubrange(i, 1);
            break;
        }
    }
}

inline NKikimrConfig::TBootstrap MakeDefaultBootstrapConfig()
{
    NKikimrConfig::TBootstrap res;
    res.MutableResourceBroker()->CopyFrom(MakeDefaultResourceBrokerConfig());
    AddTablet(NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::TX_MEDIATOR, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::TX_ALLOCATOR, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::CONSOLE, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::CMS, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::NODE_BROKER, {1, 2}, res);
    AddTablet(NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER, {1, 2}, res);
    return res;
}

template <typename TValidator>
void CheckConfig(const NKikimrConfig::TAppConfig &oldCfg,
                 const NKikimrConfig::TAppConfig &newCfg,
                 bool result,
                 ui32 warnings)
{
    TValidator validator;
    TVector<Ydb::Issue::IssueMessage> issues;
    if (validator.CheckConfig(oldCfg, newCfg, issues) != result) {
        for (auto &issue : issues)
            Cerr << issue.DebugString();
        UNIT_FAIL("wrong check result");
    }
    for (auto &issue :  issues) {
        if (issue.severity() == NYql::TSeverityIds::S_WARNING)
            --warnings;
    }
    if (warnings) {
        for (auto &issue : issues)
            Cerr << issue.DebugString();
        UNIT_ASSERT_VALUES_EQUAL(warnings, 0);
    }
}

template <typename TValidator>
void CheckConfig(const NKikimrConfig::TAppConfig &config,
                 bool result,
                 ui32 warnings)
{
    CheckConfig<TValidator>({}, config, result, warnings);
}

} // namespace NTests
} // namespace NKikimr::NConsole
