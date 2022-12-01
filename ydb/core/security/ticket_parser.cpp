#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/openssl/init/init.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/security/util.h>
#include <util/generic/queue.h>
#include <util/generic/deque.h>
#include <util/stream/file.h>
#include <util/string/vector.h>
#include "ticket_parser.h"

namespace NKikimr {

class TTicketParser : public TActorBootstrapped<TTicketParser> {
    using TThis = TTicketParser;
    using TBase = TActorBootstrapped<TTicketParser>;

    NKikimrProto::TAuthConfig Config;
    TString DomainName;

    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsReceived;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsSuccess;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsRetryable;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsPermanent;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsBuiltin;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsLogin;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsCacheHit;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsCacheMiss;
    NMonitoring::THistogramPtr CounterTicketsBuildTime;

    TDuration RefreshPeriod = TDuration::Seconds(1); // how often do we check for ticket freshness/expiration
    TDuration LifeTime = TDuration::Hours(1); // for how long ticket will remain in the cache after last access
    TDuration ExpireTime = TDuration::Hours(24); // after what time ticket will expired and removed from cache

    enum class ETokenType {
        Unknown,
        Unsupported,
        Builtin,
        Login,
    };

    ETokenType ParseTokenType(TStringBuf tokenType) {
        if (tokenType == "Login") {
            if (UseLoginProvider) {
                return ETokenType::Login;
            } else {
                return ETokenType::Unsupported;
            }
        }
        return ETokenType::Unknown;
    }

    struct TTokenRecord {
        TTokenRecord(const TTokenRecord&) = delete;
        TTokenRecord& operator =(const TTokenRecord&) = delete;

        TString Ticket;
        TString Subject; // login
        TEvTicketParser::TError Error;
        TIntrusivePtr<NACLib::TUserToken> Token;
        TString SerializedToken;
        TDeque<THolder<TEventHandle<TEvTicketParser::TEvAuthorizeTicket>>> AuthorizeRequests;
        ui64 ResponsesLeft = 0;
        TInstant InitTime;
        TInstant ExpireTime;
        TInstant AccessTime;
        ETokenType TokenType = ETokenType::Unknown;
        TString PeerName;
        TString Database;
        TStackVec<TString> AdditionalSIDs;

        TString GetSubject() const {
            return Subject;
        }

        TTokenRecord(TStringBuf ticket)
            : Ticket(ticket)
        {}

        bool IsTokenReady() const {
            return Token != nullptr;
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

    struct TTokenRefreshRecord {
        TString Key;
        TInstant RefreshTime;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

    THashMap<TString, TTokenRecord> UserTokens;
    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    std::unordered_map<TString, NLogin::TLoginProvider> LoginProviders;
    bool UseLoginProvider = false;

    static TString GetKey(TEvTicketParser::TEvAuthorizeTicket* request) {
        TStringStream key;
        key << request->Ticket;
        key << ':';
        if (request->Database) {
            key << request->Database;
            key << ':';
        }
        for (const auto& entry : request->Entries) {
            for (auto it = entry.Attributes.begin(); it != entry.Attributes.end(); ++it) {
                if (it != entry.Attributes.begin()) {
                    key << '-';
                }
                key << it->second;
            }
            key << ':';
            for (auto it = entry.Permissions.begin(); it != entry.Permissions.end(); ++it) {
                if (it != entry.Permissions.begin()) {
                    key << '-';
                }
                key << it->Permission << "(" << it->Required << ")";
            }
        }
        return key.Str();
    }

    static TStringBuf GetTicketFromKey(TStringBuf key) {
        return key.Before(':');
    }

    TInstant GetExpireTime(TInstant now) {
        return now + ExpireTime;
    }

    TDuration GetLifeTime() {
        return LifeTime;
    }

    static void EnrichUserTokenWithBuiltins(const TTokenRecord& tokenRecord) {
        const TString& allAuthenticatedUsers = AppData()->AllAuthenticatedUsers;
        if (!allAuthenticatedUsers.empty()) {
            tokenRecord.Token->AddGroupSID(allAuthenticatedUsers);
        }
        for (const TString& sid : tokenRecord.AdditionalSIDs) {
            tokenRecord.Token->AddGroupSID(sid);
        }
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

            if (record.Ticket.EndsWith("@" BUILTIN_SYSTEM_DOMAIN)) {
                record.TokenType = ETokenType::Builtin;
                SetError(key, record, { "System domain not available for user usage", false }, ctx);
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

    void Respond(TTokenRecord& record, const TActorContext& ctx) {
        if (record.IsTokenReady()) {
            for (const auto& request : record.AuthorizeRequests) {
                ctx.Send(request->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(record.Ticket, record.Token, record.SerializedToken), 0, request->Cookie);
            }
        } else {
            for (const auto& request : record.AuthorizeRequests) {
                ctx.Send(request->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(record.Ticket, record.Error), 0, request->Cookie);
            }
        }
        record.AuthorizeRequests.clear();
    }

    void CrackTicket(const TString& ticketBody, TStringBuf& ticket, TStringBuf& ticketType) {
        ticket = ticketBody;
        ticketType = ticket.NextTok(' ');
        if (ticket.empty()) {
            ticket = ticketBody;
            ticketType.Clear();
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const TActorContext& ctx) {
        TStringBuf ticket;
        TStringBuf ticketType;
        CrackTicket(ev->Get()->Ticket, ticket, ticketType);

        TString key = GetKey(ev->Get());
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        CounterTicketsReceived->Inc();
        if (ticket.empty()) {
            TEvTicketParser::TError error;
            error.Message = "Ticket is empty";
            error.Retryable = false;
            LOG_ERROR_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(ticket) << ": " << error);
            ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, error), 0, cookie);
            return;
        }
        auto it = UserTokens.find(key);
        if (it != UserTokens.end()) {
            auto& record = it->second;
            // we know about token
            if (record.IsTokenReady()) {
                // token already have built
                record.AccessTime = ctx.Now();
                ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Token, record.SerializedToken), 0, cookie);
            } else if (record.Error) {
                // token stores information about previous error
                record.AccessTime = ctx.Now();
                ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Error), 0, cookie);
            } else {
                // token building in progress
                record.AuthorizeRequests.emplace_back(ev.Release());
            }
            CounterTicketsCacheHit->Inc();
            return;
        } else {
            it = UserTokens.emplace(key, ticket).first;
            CounterTicketsCacheMiss->Inc();
        }

        auto& record = it->second;
        record.PeerName = std::move(ev->Get()->PeerName);
        record.Database = std::move(ev->Get()->Database);
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
        InitTokenRecord(key, record, ctx);
        if (record.Error) {
            LOG_ERROR_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(ticket) << ": " << record.Error);
            ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Error), 0, cookie);
            return;
        }
        if (record.IsTokenReady()) {
            // offline check ready
            ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Token, record.SerializedToken), 0, cookie);
            return;
        }
        record.AuthorizeRequests.emplace_back(ev.Release());
    }

    void Handle(TEvTicketParser::TEvRefreshTicket::TPtr& ev, const TActorContext&) {
        UserTokens.erase(ev->Get()->Ticket);
    }

    void Handle(TEvTicketParser::TEvDiscardTicket::TPtr& ev, const TActorContext&) {
        UserTokens.erase(ev->Get()->Ticket);
    }

    static TString GetLoginProviderKeys(const NLogin::TLoginProvider& loginProvider) {
        TStringBuilder keys;
        for (const auto& [key, pubKey, privKey, expiresAt] : loginProvider.Keys) {
            if (!keys.empty()) {
                keys << ",";
            }
            keys << key;
        }
        return keys;
    }

    void Handle(TEvTicketParser::TEvUpdateLoginSecurityState::TPtr& ev, const TActorContext& ctx) {
        auto& loginProvider = LoginProviders[ev->Get()->SecurityState.GetAudience()];
        loginProvider.UpdateSecurityState(ev->Get()->SecurityState);
        LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER,
            "Updated state for " << loginProvider.Audience << " keys " << GetLoginProviderKeys(loginProvider));
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

    static TStringBuf HtmlBool(bool v) {
        return v ? "<span style='font-weight:bold'>&#x2611;</span>" : "<span style='font-weight:bold'>&#x2610;</span>";
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
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::TICKET_PARSER_ACTOR; }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev, const NActors::TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
            HFunc(TEvTicketParser::TEvRefreshTicket, Handle);
            HFunc(TEvTicketParser::TEvDiscardTicket, Handle);
            HFunc(TEvTicketParser::TEvUpdateLoginSecurityState, Handle);
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

    TTicketParser(const NKikimrProto::TAuthConfig& authConfig)
        : Config(authConfig) {}
};

IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig) {
    return new TTicketParser(authConfig);
}

}
