#include "diag_mon_page.h"

using namespace NMonitoring;

void TDiagMonPage::OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest& request) {
    out << "uri:       " << request.GetUri() << "\n";
    out << "path:      " << request.GetPath() << "\n";
    out << "path info: " << request.GetPathInfo() << "\n";
}
