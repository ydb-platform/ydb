#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/security/ticket_parser_settings.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;

extern const TString TEST_MON_PATH;
extern const TString TEST_RESPONSE;
extern const TString AUTHORIZATION_HEADER;
extern const TString VALID_TOKEN;
extern const TVector<TString> DEFAULT_TICKET_PARSER_GROUPS;

class TTestActorPage : public TActorBootstrapped<TTestActorPage> {
public:
    void Bootstrap();

private:
    void Handle(NMon::TEvHttpInfo::TPtr& ev);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfo, Handle);
        }
    }
};

class TTestActorHandler : public TActorBootstrapped<TTestActorHandler> {
public:
    void Bootstrap();

private:
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

class TTestMonPage : public NMonitoring::IMonPage {
public:
    TTestMonPage();
    void Output(NMonitoring::IMonHttpRequest& request) override;
};

struct TFakeTicketParserActor : public TActor<TFakeTicketParserActor> {
    TFakeTicketParserActor(TVector<TString> groupSIDs);
    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev);
    void Fail(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const TString& message);
    void Success(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev);

    size_t AuthorizeTicketRequests = 0;
    size_t AuthorizeTicketSuccesses = 0;
    size_t AuthorizeTicketFails = 0;
    TVector<TString> GroupSIDs;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
            default:
                break;
        }
    }
};

} // namespace NMonitoring::NTests
