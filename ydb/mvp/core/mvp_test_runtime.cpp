#include "mvp_test_runtime.h"
#include "appdata.h"
#include "mvp_log.h"
#include <ydb/library/actors/http/http.h>

#include <algorithm>
#include <cstring>

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

template void EatWholeString<NHttp::THttpIncomingRequest>(TIntrusivePtr<NHttp::THttpIncomingRequest>& request, const TString& data);
template void EatWholeString<NHttp::THttpIncomingResponse>(TIntrusivePtr<NHttp::THttpIncomingResponse>& request, const TString& data);

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

    node->ActorSystem->AppData<NMVP::TMVPAppData>()->MetricRegistry = NMonitoring::TMetricRegistry::SharedInstance();
    node->ActorSystem->Start();
}
