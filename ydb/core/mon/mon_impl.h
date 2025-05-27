#pragma once

#include "mon.h"
#include <ydb/library/services/services.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>

namespace NActors {

using namespace NMonitoring;
using THttpResponsePtr = THolder<NMon::IEvHttpInfoRes>;

////////////////////////////////////////////////////////////////////////////////
// HTML results page
////////////////////////////////////////////////////////////////////////////////
class THtmlResultMonPage: public THtmlMonPage {
public:
    THtmlResultMonPage(const TString &path, const TString &title, const TString &host, bool preTag,
                        const NMon::IEvHttpInfoRes &result)
        : THtmlMonPage(path, title, true)
        , Host(host)
        , PreTag(preTag)
        , Result(result)
    {
    }

    void Output(NMonitoring::IMonHttpRequest& request) override {
        IOutputStream& out = request.Output();

        out << "HTTP/1.1 200 Ok\r\n"
            << "Content-Type: text/html\r\n"
            << "Connection: Close\r\n";
        TString origin = TString(request.GetHeader("Origin"));
        if (origin.empty()) {
            origin = "*";
        }
        out << "Access-Control-Allow-Origin: " << origin << "\r\n"
            << "Access-Control-Allow-Credentials: true\r\n"
            << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
            << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
        out << "\r\n";

        out << "<!DOCTYPE html>\n";
        out << "<html>";
        out << "<head>";
        if (Title) {
            if (Host) {
                out << "<title>" << Title << " - " << Host << "</title>\n";
            } else {
                out << "<title>" << Title << "</title>\n";
            }
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
        out << "</head>";
        out << "<body>";

        OutputNavBar(out);

        out << "<div class='container'>";
        if (Title) {
            out << "<h2>" << Title << "</h2>";
        }
        OutputContent(request);
        out << "</div>";
        out << "</body>";
    }

    void OutputContent(NMonitoring::IMonHttpRequest &request) override {
        if (PreTag) {
            request.Output() << "<pre>\n";
        }
        Result.Output(request.Output());
        if (PreTag) {
            request.Output() << "</pre>\n";
        }
    }

private:
    TString Host;
    bool PreTag;
    const NMon::IEvHttpInfoRes &Result;
};


////////////////////////////////////////////////////////////////////////////////
// INDEX PAGE
// Redirects index page to fixed url
////////////////////////////////////////////////////////////////////////////////
class TIndexRedirectMonPage: public IMonPage {
public:
    TIndexRedirectMonPage(TIntrusivePtr<TIndexMonPage> indexMonPage, const TString& path = "internal")
        : IMonPage(path)
        , IndexMonPage(std::move(indexMonPage))
    {
    }

    void Output(IMonHttpRequest& request) override {
        auto& out = request.Output();
        out << HTTPOKHTML;
        out << "<html>\n";
        IndexMonPage->OutputHead(out);

        out << "<body>\n";

        // part of common navbar
        IndexMonPage->OutputNavBar(out);

        out << "<div class='container'>\n"
                << "<h2>" << IndexMonPage->Title << "</h2>\n";
        IndexMonPage->OutputIndex(out, true);
        out << "</div>\n"
            << "</body>\n";
        out << "</html>\n";
    }

    TIntrusivePtr<TIndexMonPage> IndexMonPage;
};


////////////////////////////////////////////////////////////////////////////////
// ACTOR MONITORING PAGE
// Encapsulates a request to an actor
////////////////////////////////////////////////////////////////////////////////
class TActorMonPage: public IMonPage {
public:
    TActorMonPage(const TString &path, const TString &title, const TString &host, bool preTag,
                    TActorSystem *actorSystem, const TActorId &actorId, const TVector<TString> &sids,
                    TMon::TRequestAuthorizer authorizer, TString monServiceName = "utils")
        : IMonPage(path, title)
        , Host(host)
        , PreTag(preTag)
        , ActorSystem(actorSystem)
        , TargetActorId(actorId)
        , AllowedSIDs(sids)
        , Authorizer(std::move(authorizer))
        , MonServiceName(monServiceName)
    {
    }

    void Output(IMonHttpRequest &request) override {
        NMon::TEvHttpInfoRes(TStringBuilder() << "Error: not implemented, page '" << Title << "'.\n").Output(request.Output());
    }

    TString Host;
    bool PreTag;
    TActorSystem *ActorSystem;
    TActorId TargetActorId;
    const TVector<TString> AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
    TString MonServiceName;
};

inline TString GetPageFullPath(const NMonitoring::IMonPage* page) {
    TStringBuilder path;
    if (page->Parent) {
        path << GetPageFullPath(page->Parent);
        path << '/';
    }
    path << page->Path;
    return path;
}

}
