#include "mvp_test_runtime.h"

#include "appdata.h"
#include "mvp_log.h"

#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/core/executor_thread.h>

const TString& GetEServiceName(NActors::NLog::EComponent component) {
    static const TString loggerName("LOGGER");
    static const TString mvpName("MVP");
    static const TString grpcName("GRPC");
    static const TString queryName("QUERY");
    static const TString unknownName("UNKNOW");
    switch (component) {
    case NMVP::EService::Logger:
        return loggerName;
    case NMVP::EService::MVP:
        return mvpName;
    case NMVP::EService::GRPC:
        return grpcName;
    case NMVP::EService::QUERY:
        return queryName;
    default:
        return unknownName;
    }
}

void TMvpTestRuntime::InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) {
    node->LogSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );

    node->LogSettings->Append(
        NMVP::EService::MIN,
        NMVP::EService::MAX,
        GetEServiceName
    );
    TString explanation;
    node->LogSettings->SetLevel(NActors::NLog::PRI_DEBUG, NActorsServices::HTTP, explanation);
    node->LogSettings->SetLevel(NActors::NLog::PRI_DEBUG, NMVP::EService::MVP, explanation);
    node->LogSettings->SetLevel(NActors::NLog::PRI_DEBUG, NMVP::EService::GRPC, explanation);
    node->LogSettings->SetLevel(NActors::NLog::PRI_INFO, NMVP::EService::QUERY, explanation);

    node->AppData0.reset(new NMVP::TMVPAppData());

    if (!UseRealThreads) {
        node->SchedulerPool.Reset(CreateExecutorPoolStub(this, nodeIndex, node, 0));
        node->MailboxTable.Reset(new NActors::TMailboxTable());
        node->ActorSystem = MakeActorSystem(nodeIndex, node);
        node->ExecutorThread.Reset(new NActors::TExecutorThread(0, node->ActorSystem.Get(), node->SchedulerPool.Get(), "TestExecutor"));
    } else {
        node->ActorSystem = MakeActorSystem(nodeIndex, node);
    }

    node->ActorSystem->AppData<NMVP::TMVPAppData>()->MetricRegistry = std::make_shared<NMonitoring::TMetricRegistry>();
    node->ActorSystem->Start();
}
