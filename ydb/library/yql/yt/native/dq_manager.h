#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/service/service_node.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>

#include <library/cpp/yt/yson_string/string.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NYqlPlugin {

using namespace NYson;
using namespace NYql;

////////////////////////////////////////////////////////////////////////////////

struct TDqManagerConfig
    : public NYTree::TYsonStruct
{
    ui16 InterconnectPort;
    ui16 GrpcPort;
    ui32 ActorThreads;
    bool UseIPv4;

    std::vector<NYTree::INodePtr> YtBackends;

    NYTree::INodePtr YtCoordinator;
    NYTree::INodePtr Scheduler;
    NYTree::INodePtr ICSettings;
    NYTree::INodePtr AddressResolver;

    TMap<TString, TString> UdfsWithMd5; // autofilled by yql_plugin
    NYql::TFileStoragePtr FileStorage; // autofilled by yql_plugin

    REGISTER_YSON_STRUCT(TDqManagerConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TDqManagerConfig)
DEFINE_REFCOUNTED_TYPE(TDqManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDqManager
    : public TRefCounted
{
public:
    explicit TDqManager(const TDqManagerConfigPtr& config);
    void Start();

private:
    const TDqManagerConfigPtr& Config_;

    NActors::TActorSystem* ActorSystem_;
    ICoordinationHelper::TPtr Coordinator_;
    THolder<TServiceNode> ServiceNode_;
    IMetricsRegistryPtr MetricsRegistry_;
    NActors::IActor* StatsCollector_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDqManager)
DEFINE_REFCOUNTED_TYPE(TDqManager)

///////////////////////////////////////////

} // namespace NYT::NYqlPlugin

