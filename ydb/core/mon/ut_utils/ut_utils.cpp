#include "ut_utils.h"

#include <library/cpp/http/misc/httpcodes.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;

const TString TEST_MON_PATH = "test_mon";
const TString TEST_RESPONSE = "Test actor";
const TString AUTHORIZATION_HEADER = "Authorization";
const TString VALID_TOKEN = "Bearer token";
const TVector<TString> DEFAULT_TICKET_PARSER_GROUPS = {"group_name"};

void TTestActorPage::Bootstrap() {
    Become(&TTestActorPage::StateWork);
}

void TTestActorPage::Handle(NMon::TEvHttpInfo::TPtr& ev) {
    TStringBuilder body;
    body << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";
    Send(ev->Sender, new NMon::TEvHttpInfoRes(body));
}

void TTestActorHandler::Bootstrap() {
    Become(&TTestActorHandler::StateWork);
}

void TTestActorHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    TStringBuilder body;
    body << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";

    TStringBuilder response;
    response << "HTTP/1.1 200 OK\r\n"
                << "Content-Type: text/html\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "Connection: Close\r\n\r\n"
                << body;

    Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(
        ev->Get()->Request->CreateResponseString(response)));
}

TTestMonPage::TTestMonPage()
    : NMonitoring::IMonPage(TEST_MON_PATH, TString("Test Page"))
{
}

void TTestMonPage::Output(NMonitoring::IMonHttpRequest& request) {
    const TStringBuf pathInfo = request.GetPathInfo();
    if (!pathInfo.empty() && pathInfo != TStringBuf("/")) {
        request.Output() << NMonitoring::HTTPNOTFOUND;
        return;
    }

    auto& out = request.Output();
    out << NMonitoring::HTTPOKHTML;
    out << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";
}

TFakeTicketParserActor::TFakeTicketParserActor(TVector<TString> groupSIDs)
    : TActor<TFakeTicketParserActor>(&TFakeTicketParserActor::StateFunc)
    , GroupSIDs(std::move(groupSIDs))
{}

void TFakeTicketParserActor::Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, "Ticket parser: got TEvAuthorizeTicket event: " << ev->Get()->Ticket << " " << ev->Get()->Database << " " << ev->Get()->Entries.size());
    ++AuthorizeTicketRequests;

    if (ev->Get()->Database != "/Root") {
        Fail(ev, TStringBuilder() << "Incorrect database " << ev->Get()->Database);
        return;
    }

    if (ev->Get()->Ticket != VALID_TOKEN) {
        Fail(ev, TStringBuilder() << "Incorrect token " << ev->Get()->Ticket);
        return;
    }

    bool databaseIdFound = false;
    bool folderIdFound = false;
    for (const TEvTicketParser::TEvAuthorizeTicket::TEntry& entry : ev->Get()->Entries) {
        for (const std::pair<TString, TString>& attr : entry.Attributes) {
            if (attr.first == "database_id") {
                databaseIdFound = true;
                if (attr.second != "test_database_id") {
                    Fail(ev, TStringBuilder() << "Incorrect database_id " << attr.second);
                    return;
                }
            } else if (attr.first == "folder_id") {
                folderIdFound = true;
                if (attr.second != "test_folder_id") {
                    Fail(ev, TStringBuilder() << "Incorrect folder_id " << attr.second);
                    return;
                }
            }
        }
    }
    if (!databaseIdFound) {
        Fail(ev, "database_id not found");
        return;
    }
    if (!folderIdFound) {
        Fail(ev, "folder_id not found");
        return;
    }

    Success(ev);
}

void TFakeTicketParserActor::Fail(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const TString& message) {
    ++AuthorizeTicketFails;
    TEvTicketParser::TError err;
    err.Retryable = false;
    err.Message = message ? message : "Test error";
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER,
        "Send TEvAuthorizeTicketResult: " << err.Message);
    Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, err));
}

void TFakeTicketParserActor::Success(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
    ++AuthorizeTicketSuccesses;
    NACLib::TUserToken::TUserTokenInitFields args;
    args.UserSID = "username";
    args.GroupSIDs = GroupSIDs;
    TIntrusivePtr<NACLib::TUserToken> userToken = MakeIntrusive<NACLib::TUserToken>(args);
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER,
        "Send TEvAuthorizeTicketResult success");
    Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, userToken));
}

} // namespace NMonitoring::NTests
