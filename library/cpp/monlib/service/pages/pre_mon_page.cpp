#include "pre_mon_page.h"

using namespace NMonitoring;

void TPreMonPage::OutputContent(NMonitoring::IMonHttpRequest& request) {
    auto& out = request.Output();
    if (PreTag) {
        BeforePre(request);
        out << "<pre>\n";
        OutputText(out, request);
        out << "</pre>\n";
    } else {
        OutputText(out, request);
    }
}

void TPreMonPage::BeforePre(NMonitoring::IMonHttpRequest&) {
}
