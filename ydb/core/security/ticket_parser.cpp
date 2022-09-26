#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/digest/md5/md5.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/security/util.h>
#include <util/string/vector.h>
#include "ticket_parser_impl.h"
#include "ticket_parser.h"

namespace NKikimr {

class TTicketParser : public TTicketParserImpl<TTicketParser> {
    using TThis = TTicketParser;
    using TBase = TTicketParserImpl<TTicketParser>;
    using TBase::TBase;

    friend TBase;

    enum class ETokenType {
        Unknown,
        Unsupported,
        Builtin,
        Login,
    };

    ETokenType ParseTokenType(const TStringBuf tokenType) const {
        if (tokenType == "Login") {
            if (UseLoginProvider) {
                return ETokenType::Login;
            } else {
                return ETokenType::Unsupported;
            }
        }
        return ETokenType::Unknown;
    }

    struct TTokenRecord : TBase::TTokenRecordBase {
        using TBase::TTokenRecordBase::TTokenRecordBase;

        ETokenType TokenType = ETokenType::Unknown;

        TString GetSubject() const {
            return Subject;
        }

        TString GetAuthType() const {
            switch (TokenType) {
                case ETokenType::Unknown:
                    return "Unknown";
                case ETokenType::Unsupported:
                    return "Unsupported";
                case ETokenType::Builtin:
                    return "Builtin";
                case ETokenType::Login:
                    return "Login";
            }
        }
    };

    THashMap<TString, TTokenRecord> UserTokens;

    TTokenRecord* GetUserToken(const TString& key) {
        auto it = UserTokens.find(key);
        return it != UserTokens.end() ? &it->second : nullptr;
    }

    TTokenRecord* InsertUserToken(const TString& key, const TStringBuf ticket) {
        auto it = UserTokens.emplace(key, ticket).first;
        return &it->second;
    }

    static TStringStream GetKey(TEvTicketParser::TEvAuthorizeTicket* request) {
        return request->Ticket;
    }

    void InitTokenRecord(const TString& key, TTokenRecord& record, const TActorContext& ctx) {
        TInstant now = ctx.Now();
        record.InitTime = now;
        record.AccessTime = now;
        record.ExpireTime = GetExpireTime(now);

        if (record.Error) {
            return;
        }

        if (record.TokenType == ETokenType::Unknown || record.TokenType == ETokenType::Builtin) {
            if(record.Ticket.EndsWith("@" BUILTIN_ACL_DOMAIN)) {
                record.TokenType = ETokenType::Builtin;
                SetToken(key, record, new NACLib::TUserToken({
                    .OriginalUserToken = record.Ticket,
                    .UserSID = record.Ticket,
                    .AuthType = record.GetAuthType()
                }), ctx);
                CounterTicketsBuiltin->Inc();
                return;
            }

            if(record.Ticket.EndsWith("@" BUILTIN_ERROR_DOMAIN)) {
                record.TokenType = ETokenType::Builtin;
                SetError(key, record, {"Builtin error simulation"}, ctx);
                CounterTicketsBuiltin->Inc();
                return;
            }
        }

        if (UseLoginProvider && (record.TokenType == ETokenType::Unknown || record.TokenType == ETokenType::Login)) {
            TString database = Config.GetDomainLoginOnly() ? DomainName : record.Database;
            auto itLoginProvider = LoginProviders.find(database);
            if (itLoginProvider != LoginProviders.end()) {
                NLogin::TLoginProvider& loginProvider(itLoginProvider->second);
                auto response = loginProvider.ValidateToken({.Token = record.Ticket});
                if (response.Error) {
                    if (!response.TokenUnrecognized || record.TokenType != ETokenType::Unknown) {
                        record.TokenType = ETokenType::Login;
                        TEvTicketParser::TError error;
                        error.Message = response.Error;
                        error.Retryable = response.ErrorRetryable;
                        SetError(key, record, error, ctx);
                        CounterTicketsLogin->Inc();
                        return;
                    }
                } else {
                    record.TokenType = ETokenType::Login;
                    TVector<NACLib::TSID> groups;
                    if (response.Groups.has_value()) {
                        const std::vector<TString>& tokenGroups = response.Groups.value();
                        groups.assign(tokenGroups.begin(), tokenGroups.end());
                    } else {
                        const std::vector<TString> providerGroups = loginProvider.GetGroupsMembership(response.User);
                        groups.assign(providerGroups.begin(), providerGroups.end());
                    }
                    record.ExpireTime = ToInstant(response.ExpiresAt);
                    SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = response.User,
                        .GroupSIDs = groups,
                        .AuthType = record.GetAuthType()
                    }), ctx);
                    CounterTicketsLogin->Inc();
                    return;
                }
            } else {
                if (record.TokenType == ETokenType::Login) {
                    TEvTicketParser::TError error;
                    error.Message = "Login state is not available yet";
                    error.Retryable = false;
                    SetError(key, record, error, ctx);
                    CounterTicketsLogin->Inc();
                    return;
                }
            }
        }

        if (record.TokenType == ETokenType::Unknown && record.ResponsesLeft == 0) {
            record.Error.Message = "Could not find correct token validator";
            record.Error.Retryable = false;
        }
    }

    void SetToken(const TString& key, TTokenRecord& record, TIntrusivePtr<NACLib::TUserToken> token, const TActorContext& ctx) {
        TInstant now = ctx.Now();
        record.Error.clear();
        record.Token = token;
        EnrichUserTokenWithBuiltins(record);
        if (!token->GetUserSID().empty()) {
            record.Subject = token->GetUserSID();
        }
        record.SerializedToken = token->SerializeAsString();
        if (!record.ExpireTime) {
            record.ExpireTime = GetExpireTime(now);
        }
        CounterTicketsSuccess->Inc();
        CounterTicketsBuildTime->Collect((now - record.InitTime).MilliSeconds());
        LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                    << record.PeerName << ") has now valid token of " << record.Subject);
        RefreshQueue.push({key, record.ExpireTime});
    }

    void SetError(const TString& key, TTokenRecord& record, const TEvTicketParser::TError& error, const TActorContext& ctx) {
        record.Error = error;
        if (record.Error.Retryable) {
            record.ExpireTime = GetExpireTime(ctx.Now());
            CounterTicketsErrorsRetryable->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                        << record.PeerName << ") has now retryable error message '" << error.Message << "'");
        } else {
            record.Token = nullptr;
            record.SerializedToken.clear();
            CounterTicketsErrorsPermanent->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                        << record.PeerName << ") has now permanent error message '" << error.Message << "'");
        }
        CounterTicketsErrors->Inc();
        RefreshQueue.push({key, record.ExpireTime});
    }

    bool IsTicketEmpty(const TStringBuf ticket, TEvTicketParser::TEvAuthorizeTicket::TPtr&) {
        return ticket.empty();
    }

    void SetTokenType(TTokenRecord& record, TStringBuf&, const TStringBuf ticketType) {
        if (ticketType) {
            record.TokenType = ParseTokenType(ticketType);
            switch (record.TokenType) {
                case ETokenType::Unsupported:
                    record.Error.Message = "Token is not supported";
                    record.Error.Retryable = false;
                    break;
                case ETokenType::Unknown:
                    record.Error.Message = "Unknown token";
                    record.Error.Retryable = false;
                    break;
                default:
                    break;
            }
        }
    }

    void Handle(TEvTicketParser::TEvRefreshTicket::TPtr& ev, const TActorContext&) {
        UserTokens.erase(ev->Get()->Ticket);
    }

    void Handle(TEvTicketParser::TEvDiscardTicket::TPtr& ev, const TActorContext&) {
        UserTokens.erase(ev->Get()->Ticket);
    }

    void HandleRefresh(const NActors::TActorContext& ctx) {
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= ctx.Now()) {
            TString key = RefreshQueue.top().Key;
            RefreshQueue.pop();
            auto itToken = UserTokens.find(key);
            if (itToken == UserTokens.end()) {
                continue;
            }
            auto& record(itToken->second);
            if ((record.ExpireTime > ctx.Now()) && (record.AccessTime + GetLifeTime() > ctx.Now())) {
                // we don't need to refresh anything
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Expired ticket " << MaskTicket(record.Ticket));
                if (!record.AuthorizeRequests.empty()) {
                    record.Error = {"Timed out", true};
                    Respond(record, ctx);
                }
                UserTokens.erase(itToken);
            }
        }
        ctx.Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        const auto& params = ev->Get()->Request.GetParams();
        TStringBuilder html;
        if (params.Has("token")) {
            TString token = params.Get("token");
            html << "<head>";
            html << "<style>";
            html << "table.ticket-parser-proplist > tbody > tr > td { padding: 1px 3px; } ";
            html << "table.ticket-parser-proplist > tbody > tr > td:first-child { font-weight: bold; text-align: right; } ";
            html << "</style>";
            for (const std::pair<const TString, TTokenRecord>& pr : UserTokens) {
                if (MD5::Calc(pr.first) == token) {
                    const TTokenRecord& record = pr.second;
                    html << "<div>";
                    html << "<table class='ticket-parser-proplist'>";
                    html << "<tr><td>Ticket</td><td>" << MaskTicket(record.Ticket) << "</td></tr>";
                    if (record.TokenType == ETokenType::Login) {
                        TVector<TString> tokenData;
                        Split(record.Ticket, ".", tokenData);
                        if (tokenData.size() > 1) {
                            TString header;
                            TString payload;
                            try {
                                header = Base64DecodeUneven(tokenData[0]);
                            }
                            catch (const std::exception&) {
                                header = tokenData[0];
                            }
                            try {
                                payload = Base64DecodeUneven(tokenData[1]);
                            }
                            catch (const std::exception&) {
                                payload = tokenData[1];
                            }
                            html << "<tr><td>Header</td><td>" << header << "</td></tr>";
                            html << "<tr><td>Payload</td><td>" << payload << "</td></tr>";
                        }
                    }
                    html << "<tr><td>Subject</td><td>" << record.Subject << "</td></tr>";
                    html << "<tr><td>Additional SIDs</td><td>" << JoinStrings(record.AdditionalSIDs.begin(), record.AdditionalSIDs.end(), ", ") << "</td></tr>";
                    html << "<tr><td>Error</td><td>" << record.Error << "</td></tr>";
                    html << "<tr><td>Requests Infly</td><td>" << record.AuthorizeRequests.size() << "</td></tr>";
                    html << "<tr><td>Responses Left</td><td>" << record.ResponsesLeft << "</td></tr>";
                    html << "<tr><td>Expire Time</td><td>" << record.ExpireTime << "</td></tr>";
                    html << "<tr><td>Access Time</td><td>" << record.AccessTime << "</td></tr>";
                    html << "<tr><td>Peer Name</td><td>" << record.PeerName << "</td></tr>";
                    if (record.Token != nullptr) {
                        html << "<tr><td>User SID</td><td>" << record.Token->GetUserSID() << "</td></tr>";
                        for (const TString& group : record.Token->GetGroupSIDs()) {
                            html << "<tr><td>Group SID</td><td>" << group << "</td></tr>";
                        }
                    }
                    html << "</table>";
                    html << "</div>";
                    break;
                }
            }
            html << "</head>";
        } else {
            html << "<head>";
            html << "<script>$('.container').css('width', 'auto');</script>";
            html << "<style>";
            html << "table.ticket-parser-proplist > tbody > tr > td { padding: 1px 3px; } ";
            html << "table.ticket-parser-proplist > tbody > tr > td:first-child { font-weight: bold; text-align: right; } ";
            html << "table.simple-table1 th { margin: 0px 3px; text-align: center; } ";
            html << "table.simple-table1 > tbody > tr > td:nth-child(6) { text-align: right; }";
            html << "table.simple-table1 > tbody > tr > td:nth-child(7) { text-align: right; }";
            html << "table.simple-table1 > tbody > tr > td:nth-child(8) { white-space: nowrap; }";
            html << "table.simple-table1 > tbody > tr > td:nth-child(9) { white-space: nowrap; }";
            html << "table.simple-table1 > tbody > tr > td:nth-child(10) { white-space: nowrap; }";
            html << "table.table-hover tbody tr:hover > td { background-color: #9dddf2; }";
            html << "</style>";
            html << "</head>";
            html << "<div style='margin-bottom: 10px; margin-left: 100px'>";
            html << "<table class='ticket-parser-proplist'>";
            html << "<tr><td>User Tokens</td><td>" << UserTokens.size() << "</td></tr>";
            html << "<tr><td>Refresh Queue</td><td>" << RefreshQueue.size() << "</td></tr>";
            if (!RefreshQueue.empty()) {
                html << "<tr><td>Refresh Queue Time</td><td>" << RefreshQueue.top().RefreshTime << "</td></tr>";
            }
            html << "<tr><td>Refresh Period</td><td>" << RefreshPeriod << "</td></tr>";
            html << "<tr><td>Life Time</td><td>" << LifeTime << "</td></tr>";
            html << "<tr><td>Expire Time</td><td>" << ExpireTime << "</td></tr>";
            if (UseLoginProvider) {
                for (const auto& [databaseName, loginProvider] : LoginProviders) {
                    html << "<tr><td>LoginProvider Database</td><td>" << databaseName << " (" << loginProvider.Audience << ")</td></tr>";
                    html << "<tr><td>LoginProvider Keys</td><td>" << GetLoginProviderKeys(loginProvider) << "</td></tr>";
                    html << "<tr><td>LoginProvider Sids</td><td>" << loginProvider.Sids.size() << "</td></tr>";
                }
            }
            html << "<tr><td>Login</td><td>" << HtmlBool(UseLoginProvider) << "</td></tr>";
            html << "</table>";
            html << "</div>";

            html << "<div>";
            html << "<table class='table simple-table1 table-hover table-condensed'>";
            html << "<thead><tr>";
            html << "<th>Ticket</th>";
            html << "<th>UID</th>";
            html << "<th>Database</th>";
            html << "<th>Subject</th>";
            html << "<th>Error</th>";
            html << "<th>Token</th>";
            html << "<th>Requests</th>";
            html << "<th>Responses Left</th>";
            html << "<th>Refresh</th>";
            html << "<th>Expire</th>";
            html << "<th>Access</th>";
            html << "<th>Peer</th>";
            html << "</tr></thead><tbody>";
            for (const std::pair<const TString, TTokenRecord>& pr : UserTokens) {
                const TTokenRecord& record = pr.second;
                html << "<tr>";
                html << "<td>" << MaskTicket(record.Ticket) << "</td>";
                html << "<td>" << record.Database << "</td>";
                html << "<td>" << record.Subject << "</td>";
                html << "<td>" << record.Error << "</td>";
                html << "<td>" << "<a href='ticket_parser?token=" << MD5::Calc(pr.first) << "'>" << HtmlBool(record.Token != nullptr) << "</a>" << "</td>";
                html << "<td>" << record.AuthorizeRequests.size() << "</td>";
                html << "<td>" << record.ResponsesLeft << "</td>";
                html << "<td>" << record.ExpireTime << "</td>";
                html << "<td>" << record.AccessTime << "</td>";
                html << "<td>" << record.PeerName << "</td>";
                html << "</tr>";
            }
            html << "</tbody></table>";
            html << "</div>";
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
    }

public:
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev, const NActors::TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTicketParser::TEvAuthorizeTicket, TBase::Handle);
            HFunc(TEvTicketParser::TEvRefreshTicket, Handle);
            HFunc(TEvTicketParser::TEvDiscardTicket, Handle);
            HFunc(TEvTicketParser::TEvUpdateLoginSecurityState, TBase::Handle);
            HFunc(NMon::TEvHttpInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleRefresh);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> rootCounters = AppData(ctx)->Counters;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> authCounters = GetServiceCounters(rootCounters, "auth");
        ::NMonitoring::TDynamicCounterPtr counters = authCounters->GetSubgroup("subsystem", "TicketParser");
        CounterTicketsReceived = counters->GetCounter("TicketsReceived", true);
        CounterTicketsSuccess = counters->GetCounter("TicketsSuccess", true);
        CounterTicketsErrors = counters->GetCounter("TicketsErrors", true);
        CounterTicketsErrorsRetryable = counters->GetCounter("TicketsErrorsRetryable", true);
        CounterTicketsErrorsPermanent = counters->GetCounter("TicketsErrorsPermanent", true);
        CounterTicketsBuiltin = counters->GetCounter("TicketsBuiltin", true);
        CounterTicketsLogin = counters->GetCounter("TicketsLogin", true);
        CounterTicketsCacheHit = counters->GetCounter("TicketsCacheHit", true);
        CounterTicketsCacheMiss = counters->GetCounter("TicketsCacheMiss", true);
        CounterTicketsBuildTime = counters->GetHistogram("TicketsBuildTimeMs",
                                                         NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000}));

        if (Config.GetUseLoginProvider()) {
            UseLoginProvider = true;
        }
        if (AppData() && AppData()->DomainsInfo && !AppData()->DomainsInfo->Domains.empty()) {
            DomainName = "/" + AppData()->DomainsInfo->Domains.begin()->second->Name;
        }

        RefreshPeriod = TDuration::Parse(Config.GetRefreshPeriod());
        LifeTime = TDuration::Parse(Config.GetLifeTime());
        ExpireTime = TDuration::Parse(Config.GetExpireTime());

        NActors::TMon* mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "ticket_parser", "Ticket Parser", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }

        ctx.Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
        TBase::Become(&TThis::StateWork);
    }
};

IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig) {
    return new TTicketParser(authConfig);
}

}
