#include "mon_service_messagebus.h"

using namespace NMonitoring;

TMonServiceMessageBus::TMonServiceMessageBus(ui16 port, const TString& title)
    : TMonService2(port, title)
{
}
