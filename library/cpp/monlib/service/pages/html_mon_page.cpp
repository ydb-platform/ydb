#include "html_mon_page.h"

#include <library/cpp/monlib/service/pages/templates.h>

using namespace NMonitoring;

void THtmlMonPage::Output(NMonitoring::IMonHttpRequest& request) {
    IOutputStream& out = request.Output();

    out << HTTPOKHTML;
    HTML(out) {
        out << "<!DOCTYPE html>\n";
        HTML_TAG() {
            HEAD() {
                if (!!Title) {
                    out << "<title>" << Title << "</title>\n";
                }
                out << "<link rel='stylesheet' href='/static/css/bootstrap.min.css'>\n";
                out << "<script language='javascript' type='text/javascript' src='/static/js/jquery.min.js'></script>\n";
                out << "<script language='javascript' type='text/javascript' src='/static/js/bootstrap.min.js'></script>\n";

                if (OutputTableSorterJsCss) {
                    out << "<link rel='stylesheet' href='/jquery.tablesorter.css'>\n";
                    out << "<script language='javascript' type='text/javascript' src='/jquery.tablesorter.js'></script>\n";
                }

                out << "<style type=\"text/css\">\n";
                out << ".table-nonfluid { width: auto; }\n";
                out << ".narrow-line50 {line-height: 50%}\n";
                out << ".narrow-line60 {line-height: 60%}\n";
                out << ".narrow-line70 {line-height: 70%}\n";
                out << ".narrow-line80 {line-height: 80%}\n";
                out << ".narrow-line90 {line-height: 90%}\n";
                out << "</style>\n";
            }
            BODY() {
                OutputNavBar(out);

                DIV_CLASS("container") {
                    if (!!Title) {
                        out << "<h2>" << Title << "</h2>";
                    }
                    OutputContent(request);
                }
            }
        }
    }
}

void THtmlMonPage::NotFound(NMonitoring::IMonHttpRequest& request) const {
    IOutputStream& out = request.Output();
    out << HTTPNOTFOUND;
    out.Flush();
}

void THtmlMonPage::NoContent(NMonitoring::IMonHttpRequest& request) const {
    IOutputStream& out = request.Output();
    out << HTTPNOCONTENT;
    out.Flush();
}
