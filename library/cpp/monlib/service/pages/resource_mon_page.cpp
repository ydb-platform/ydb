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
            out << HTTPOKCSS; 
            break; 
        case JAVASCRIPT: 
            out << HTTPOKJAVASCRIPT; 
            break; 
        case FONT_EOT:
            out << HTTPOKFONTEOT;
            break;
        case FONT_TTF:
            out << HTTPOKFONTTTF;
            break;
        case FONT_WOFF:
            out << HTTPOKFONTWOFF;
            break;
        case FONT_WOFF2:
            out << HTTPOKFONTWOFF2;
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
