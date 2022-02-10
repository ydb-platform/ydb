#include "http_ping.h"

namespace NKikimr {
namespace NHttp {

TPing::TPing()
    : NMonitoring::IMonPage("ping")
{}

void TPing::Output(NMonitoring::IMonHttpRequest& request) {
    IOutputStream& out(request.Output());
    out << NMonitoring::HTTPOKTEXT;
    out << "ok /ping";
}


TPing* CreatePing() {
    return new TPing();
}

}
}
