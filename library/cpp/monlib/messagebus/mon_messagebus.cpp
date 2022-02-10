#include <library/cpp/messagebus/www/www.h>

#include "mon_messagebus.h"

using namespace NMonitoring;

void TBusNgMonPage::Output(NMonitoring::IMonHttpRequest& request) {
    NBus::TBusWww::TOptionalParams params;
    params.ParentLinks.push_back(NBus::TBusWww::TLink{"/", request.GetServiceTitle()});
    BusWww->ServeHttp(request.Output(), request.GetParams(), params);
}
