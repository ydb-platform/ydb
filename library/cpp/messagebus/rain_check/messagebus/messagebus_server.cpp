#include "messagebus_server.h"

#include <library/cpp/messagebus/rain_check/core/spawn.h>

using namespace NRainCheck;

TBusTaskStarter::TBusTaskStarter(TAutoPtr<ITaskFactory> taskFactory)
    : TaskFactory(taskFactory)
{
}

void TBusTaskStarter::OnMessage(NBus::TOnMessageContext& onMessage) {
    TaskFactory->NewTask(onMessage);
}

TBusTaskStarter::~TBusTaskStarter() {
}
