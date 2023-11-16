#include "resource_mon_page.h"

using namespace NMonitoring;

void TResourceMonPage::Output(NMonitoring::IMonHttpRequest& request) {
    IOutputStream& out = request.Output();
    switch (ResourceType) {
        case TEXT:
            out << HTTPOKTEXT;
            break;
        case JSON:
            out << HTTPOKJSON;
            break;
        case CSS:
            out << (IsCached ? HTTPOKCSS_CACHED : HTTPOKCSS);
            break;
        case JAVASCRIPT:
            out << (IsCached ? HTTPOKJAVASCRIPT_CACHED : HTTPOKJAVASCRIPT);
            break;
        case FONT_EOT:
            out << (IsCached ? HTTPOKFONTEOT_CACHED : HTTPOKFONTEOT);
            break;
        case FONT_TTF:
            out << (IsCached ? HTTPOKFONTTTF_CACHED : HTTPOKFONTTTF);
            break;
        case FONT_WOFF:
            out << (IsCached ? HTTPOKFONTWOFF_CACHED : HTTPOKFONTWOFF);
            break;
        case FONT_WOFF2:
            out << (IsCached ? HTTPOKFONTWOFF2_CACHED : HTTPOKFONTWOFF2);
            break;
        case PNG:
            out << HTTPOKPNG;
            break;
        case SVG:
            out << HTTPOKSVG;
            break;
        default:
            out << HTTPOKBIN;
            break;
    }
    out << NResource::Find(ResourceName);
}

void TResourceMonPage::NotFound(NMonitoring::IMonHttpRequest& request) const {
    IOutputStream& out = request.Output();
    out << HTTPNOTFOUND;
    out.Flush();
}
