#include "example_module.h"

using namespace NBus;
using namespace NBus::NTest;

TExampleModule::TExampleModule()
    : TBusModule("TExampleModule")
{
    TBusQueueConfig queueConfig;
    queueConfig.NumWorkers = 5;
    Queue = CreateMessageQueue(queueConfig);
}

void TExampleModule::StartModule() {
    CreatePrivateSessions(Queue.Get());
    StartInput();
}

bool TExampleModule::Shutdown() {
    TBusModule::Shutdown();
    return true;
}

TBusServerSessionPtr TExampleModule::CreateExtSession(TBusMessageQueue&) {
    return nullptr;
}

TBusServerSessionPtr TExampleServerModule::CreateExtSession(TBusMessageQueue& queue) {
    TBusServerSessionPtr r = CreateDefaultDestination(queue, &Proto, TBusServerSessionConfig());
    ServerAddr = TNetAddr("localhost", r->GetActualListenPort());
    return r;
}

TExampleClientModule::TExampleClientModule()
    : Source()
{
}

TBusServerSessionPtr TExampleClientModule::CreateExtSession(TBusMessageQueue& queue) {
    Source = CreateDefaultSource(queue, &Proto, TBusServerSessionConfig());
    Source->RegisterService("localhost");
    return nullptr;
}
