#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>
#include <ydb/library/security/util.h>
#include <util/string/vector.h>
#include "ticket_parser_impl.h"
#include "ticket_parser.h"

namespace NKikimr {

class TTicketParser : public TTicketParserImpl<TTicketParser> {
    using TThis = TTicketParser;
    using TBase = TTicketParserImpl<TTicketParser>;
    using TBase::TBase;
    using TTokenRecord = TBase::TTokenRecordBase;

    friend TBase;

    enum class ETokenType {
        Unknown,
        Unsupported,
        Builtin,
        Login,
    };

    THashMap<TString, TTokenRecord> UserTokens;

    THashMap<TString, TTokenRecord>& GetUserTokens() {
        return UserTokens;
    }

    static TStringStream GetKey(TEvTicketParser::TEvAuthorizeTicket* request) {
        return request->Ticket;
    }

    TTokenRefreshRecord MakeTokenRefreshRecord(const TString& key, const TTokenRecord& record) const {
        return {key, record.ExpireTime};
    }

    bool IsTicketEmpty(const TStringBuf ticket, TEvTicketParser::TEvAuthorizeTicket::TPtr&) {
        return ticket.empty();
    }

    void Handle(TEvTicketParser::TEvRefreshTicket::TPtr& ev, const TActorContext&) {
        UserTokens.erase(ev->Get()->Ticket);
    }

    void Refresh(const TString&, const TTokenRecord&, const TActorContext&) {
        // we don't need to refresh anything
    }

    static void MakeHtmlTable(TStringBuilder& html) {
        html << "table.ticket-parser-proplist > tbody > tr > td { padding: 1px 3px; } ";
        html << "table.ticket-parser-proplist > tbody > tr > td:first-child { font-weight: bold; text-align: right; } ";
        html << "table.simple-table1 th { margin: 0px 3px; text-align: center; } ";
        html << "table.simple-table1 > tbody > tr > td:nth-child(6) { text-align: right; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(7) { text-align: right; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(8) { white-space: nowrap; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(9) { white-space: nowrap; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(10) { white-space: nowrap; }";
        html << "table.table-hover tbody tr:hover > td { background-color: #9dddf2; }";
    }

public:
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev, const NActors::TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTicketParser::TEvAuthorizeTicket, TBase::Handle);
            HFunc(TEvTicketParser::TEvRefreshTicket, Handle);
            HFunc(TEvTicketParser::TEvDiscardTicket, TBase::Handle);
            HFunc(TEvTicketParser::TEvUpdateLoginSecurityState, TBase::Handle);
            HFunc(NMon::TEvHttpInfo, TBase::Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleRefresh);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }
};

IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig) {
    return new TTicketParser(authConfig);
}

}
