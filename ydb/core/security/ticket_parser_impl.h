#pragma once
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/digest/md5/md5.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/library/security/util.h>
#include <util/generic/queue.h>

namespace NKikimr {

template <typename TDerived>
class TTicketParserImpl : public TActorBootstrapped<TDerived> {
    using TThis = TTicketParserImpl;
    using TBase = TActorBootstrapped<TDerived>;

    TDerived* GetDerived() {
        return static_cast<TDerived*>(this);
    }

    static TString GetKey(TEvTicketParser::TEvAuthorizeTicket* request) {
        TStringStream key(TDerived::GetKey(request));
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

protected:
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
    ::NMonitoring::THistogramPtr CounterTicketsBuildTime;

    TDuration RefreshPeriod = TDuration::Seconds(1); // how often do we check for ticket freshness/expiration
    TDuration RefreshTime = TDuration::Hours(1); // within this time we will try to refresh valid ticket
    TDuration MinErrorRefreshTime = TDuration::Seconds(1); // between this and next time we will try to refresh retryable error
    TDuration MaxErrorRefreshTime = TDuration::Minutes(1);
    TDuration LifeTime = TDuration::Hours(1); // for how long ticket will remain in the cache after last access
    TDuration ExpireTime = TDuration::Hours(24); // after what time ticket will expired and removed from cache

    auto ParseTokenType(const TStringBuf tokenType) const {
        if (tokenType == "Login") {
            if (UseLoginProvider) {
                return TDerived::ETokenType::Login;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        }
        return TDerived::ETokenType::Unknown;
    }

    struct TTokenRecordBase {
        TTokenRecordBase(const TTokenRecordBase&) = delete;
        TTokenRecordBase& operator =(const TTokenRecordBase&) = delete;

        TString Ticket;
        typename TDerived::ETokenType TokenType = TDerived::ETokenType::Unknown;
        TString Subject; // login
        TEvTicketParser::TError Error;
        TIntrusivePtr<NACLib::TUserToken> Token;
        TString SerializedToken;
        TDeque<THolder<TEventHandle<TEvTicketParser::TEvAuthorizeTicket>>> AuthorizeRequests;
        ui64 ResponsesLeft = 0;
        TInstant InitTime;
        TInstant RefreshTime;
        TInstant ExpireTime;
        TInstant AccessTime;
        TDuration CurrentMaxRefreshTime = TDuration::Seconds(1);
        TDuration CurrentMinRefreshTime = TDuration::Seconds(1);
        TString PeerName;
        TString Database;
        TStackVec<TString> AdditionalSIDs;

        TTokenRecordBase(const TStringBuf ticket)
            : Ticket(ticket)
        {}

        template <typename T>
        void SetErrorRefreshTime(TTicketParserImpl<T>* ticketParser, TInstant now) {
            if (Error.Retryable) {
                if (CurrentMaxRefreshTime < ticketParser->MaxErrorRefreshTime) {
                    CurrentMaxRefreshTime += ticketParser->MinErrorRefreshTime;
                }
                CurrentMinRefreshTime = ticketParser->MinErrorRefreshTime;
            } else {
                CurrentMaxRefreshTime = ticketParser->RefreshTime;
                CurrentMinRefreshTime = CurrentMaxRefreshTime / 2;
            }
            SetRefreshTime(now);
        }

        template <typename T>
        void SetOkRefreshTime(TTicketParserImpl<T>* ticketParser, TInstant now) {
            CurrentMaxRefreshTime = ticketParser->RefreshTime;
            CurrentMinRefreshTime = CurrentMaxRefreshTime / 2;
            SetRefreshTime(now);
        }

        void SetRefreshTime(TInstant now) {
            if (CurrentMinRefreshTime < CurrentMaxRefreshTime) {
                TDuration currentDuration = CurrentMaxRefreshTime - CurrentMinRefreshTime;
                TDuration refreshDuration = CurrentMinRefreshTime + TDuration::MilliSeconds(RandomNumber<double>() * currentDuration.MilliSeconds());
                RefreshTime = now + refreshDuration;
            } else {
                RefreshTime = now + CurrentMinRefreshTime;
            }
        }

        TString GetSubject() const {
            return Subject;
        }

        TString GetAuthType() const {
            switch (TokenType) {
                case TDerived::ETokenType::Unknown:
                    return "Unknown";
                case TDerived::ETokenType::Unsupported:
                    return "Unsupported";
                case TDerived::ETokenType::Builtin:
                    return "Builtin";
                case TDerived::ETokenType::Login:
                    return "Login";
            }
        }

        bool IsOfflineToken() const {
            switch (TokenType) {
                case TDerived::ETokenType::Builtin:
                case TDerived::ETokenType::Login:
                    return true;
                default:
                    return false;
            }
        }

        bool IsTokenReady() const {
            return Token != nullptr;
        }
    };

    struct TTokenRefreshRecord {
        TString Key;
        TInstant RefreshTime;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    std::unordered_map<TString, NLogin::TLoginProvider> LoginProviders;
    bool UseLoginProvider = false;

    static TStringBuf GetTicketFromKey(const TStringBuf key) {
        return key.Before(':');
    }

    TInstant GetExpireTime(TInstant now) const {
        return now + ExpireTime;
    }

    TInstant GetRefreshTime(TInstant now) const {
        return now + RefreshTime;
    }

    TDuration GetLifeTime() const {
        return LifeTime;
    }

    static void EnrichUserTokenWithBuiltins(const TTokenRecordBase& tokenRecord) {
        const TString& allAuthenticatedUsers = AppData()->AllAuthenticatedUsers;
        if (!allAuthenticatedUsers.empty()) {
            tokenRecord.Token->AddGroupSID(allAuthenticatedUsers);
        }
        for (const TString& sid : tokenRecord.AdditionalSIDs) {
            tokenRecord.Token->AddGroupSID(sid);
        }
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString&, TTokenRecord& record, const TActorContext&, TInstant) {
        if (record.TokenType == TDerived::ETokenType::Unknown && record.ResponsesLeft == 0) {
            record.Error.Message = "Could not find correct token validator";
            record.Error.Retryable = false;
        }
    }

    template <typename TTokenRecord>
    bool CanInitBuiltinToken(const TString& key, TTokenRecord& record, const TActorContext& ctx) {
        if (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Builtin) {
            if(record.Ticket.EndsWith("@" BUILTIN_ACL_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetToken(key, record, new NACLib::TUserToken({
                    .OriginalUserToken = record.Ticket,
                    .UserSID = record.Ticket,
                    .AuthType = record.GetAuthType()
                }), ctx);
                CounterTicketsBuiltin->Inc();
                return true;
            }

            if (record.Ticket.EndsWith("@" BUILTIN_ERROR_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetError(key, record, { "Builtin error simulation" }, ctx);
                CounterTicketsBuiltin->Inc();
                return true;
            }

            if (record.Ticket.EndsWith("@" BUILTIN_SYSTEM_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetError(key, record, { "System domain not available for user usage", false }, ctx);
                CounterTicketsBuiltin->Inc();
                return true;
            }
        }
        return false;
    }

    template <typename TTokenRecord>
    bool CanInitLoginToken(const TString& key, TTokenRecord& record, const TActorContext& ctx) {
        if (UseLoginProvider && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Login)) {
            TString database = Config.GetDomainLoginOnly() ? DomainName : record.Database;
            auto itLoginProvider = LoginProviders.find(database);
            if (itLoginProvider != LoginProviders.end()) {
                NLogin::TLoginProvider& loginProvider(itLoginProvider->second);
                auto response = loginProvider.ValidateToken({.Token = record.Ticket});
                if (response.Error) {
                    if (!response.TokenUnrecognized || record.TokenType != TDerived::ETokenType::Unknown) {
                        record.TokenType = TDerived::ETokenType::Login;
                        TEvTicketParser::TError error;
                        error.Message = response.Error;
                        error.Retryable = response.ErrorRetryable;
                        SetError(key, record, error, ctx);
                        CounterTicketsLogin->Inc();
                        return true;
                    }
                } else {
                    record.TokenType = TDerived::ETokenType::Login;
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
                    return true;
                }
            } else {
                if (record.TokenType == TDerived::ETokenType::Login) {
                    TEvTicketParser::TError error;
                    error.Message = "Login state is not available yet";
                    error.Retryable = false;
                    SetError(key, record, error, ctx);
                    CounterTicketsLogin->Inc();
                    return true;
                }
            }
        }
        return false;
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record, const TActorContext& ctx) {
        TInstant now = ctx.Now();
        record.InitTime = now;
        record.AccessTime = now;
        record.ExpireTime = GetExpireTime(now);
        record.RefreshTime = GetRefreshTime(now);

        if (record.Error) {
            return;
        }

        if (CanInitBuiltinToken(key, record, ctx) ||
            CanInitLoginToken(key, record, ctx)) {
            return;
        }

        GetDerived()->InitTokenRecord(key, record, ctx, now);
    }

    template <typename TTokenRecord>
    void EnrichUserToken(const TTokenRecord& record) const {
        EnrichUserTokenWithBuiltins(record);
    }

    template <typename TTokenRecord>
    void SetToken(const TString& key, TTokenRecord& record, TIntrusivePtr<NACLib::TUserToken> token, const TActorContext& ctx) {
        TInstant now = ctx.Now();
        record.Error.clear();
        record.Token = token;
        GetDerived()->EnrichUserToken(record);
        if (!token->GetUserSID().empty()) {
            record.Subject = token->GetUserSID();
        }
        record.SerializedToken = token->SerializeAsString();
        if (!record.ExpireTime) {
            record.ExpireTime = GetExpireTime(now);
        }
        if (record.IsOfflineToken()) {
            record.RefreshTime = record.ExpireTime;
        } else {
            record.SetOkRefreshTime(this, now);
        }
        CounterTicketsSuccess->Inc();
        CounterTicketsBuildTime->Collect((now - record.InitTime).MilliSeconds());
        LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                    << record.PeerName << ") has now valid token of " << record.Subject);
        RefreshQueue.push(GetDerived()->MakeTokenRefreshRecord(key, record));
    }

    template <typename TTokenRecord>
    void SetError(const TString& key, TTokenRecord& record, const TEvTicketParser::TError& error, const TActorContext& ctx) {
        record.Error = error;
        if (record.Error.Retryable) {
            record.ExpireTime = GetExpireTime(ctx.Now());
            record.SetErrorRefreshTime(this, ctx.Now());
            CounterTicketsErrorsRetryable->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                        << record.PeerName << ") has now retryable error message '" << error.Message << "'");
        } else {
            record.Token = nullptr;
            record.SerializedToken.clear();
            record.SetOkRefreshTime(this, ctx.Now());
            CounterTicketsErrorsPermanent->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(record.Ticket) << " ("
                        << record.PeerName << ") has now permanent error message '" << error.Message << "'");
        }
        CounterTicketsErrors->Inc();
        RefreshQueue.push(GetDerived()->MakeTokenRefreshRecord(key, record));
    }

    void Respond(TTokenRecordBase& record, const TActorContext& ctx) {
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

    template <typename TTokenRecord>
    void TokenRecordSetup(TTokenRecord& record, TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        record.PeerName = std::move(ev->Get()->PeerName);
        record.Database = std::move(ev->Get()->Database);
    }

    template <typename TTokenRecord>
    void SetTokenType(TTokenRecord& record, TStringBuf&, const TStringBuf ticketType) {
        if (ticketType) {
            record.TokenType = GetDerived()->ParseTokenType(ticketType);
            switch (record.TokenType) {
                case TDerived::ETokenType::Unsupported:
                    record.Error.Message = "Token is not supported";
                    record.Error.Retryable = false;
                    break;
                case TDerived::ETokenType::Unknown:
                    record.Error.Message = "Unknown token";
                    record.Error.Retryable = false;
                    break;
                default:
                    break;
            }
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
        if (GetDerived()->IsTicketEmpty(ticket, ev)) {
            TEvTicketParser::TError error;
            error.Message = "Ticket is empty";
            error.Retryable = false;
            LOG_ERROR_S(ctx, NKikimrServices::TICKET_PARSER, "Ticket " << MaskTicket(ticket) << ": " << error);
            ctx.Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, error), 0, cookie);
            return;
        }
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(key);
        if (it != userTokens.end()) {
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
            it = userTokens.emplace(key, ticket).first;
            CounterTicketsCacheMiss->Inc();
        }

        auto& record = it->second;
        GetDerived()->TokenRecordSetup(record, ev);

        GetDerived()->SetTokenType(record, ticket, ticketType);

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

    void Handle(TEvTicketParser::TEvDiscardTicket::TPtr& ev, const TActorContext&) {
        auto& userTokens = GetDerived()->GetUserTokens();
        userTokens.erase(ev->Get()->Ticket);
    }

    void HandleRefresh(const NActors::TActorContext& ctx) {
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= ctx.Now()) {
            TString key = RefreshQueue.top().Key;
            RefreshQueue.pop();
            auto& userTokens = GetDerived()->GetUserTokens();
            auto it = userTokens.find(key);
            if (it == userTokens.end()) {
                continue;
            }
            auto& record = it->second;
            if ((record.ExpireTime > ctx.Now()) && (record.AccessTime + GetLifeTime() > ctx.Now())) {
                GetDerived()->Refresh(key, record, ctx);
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TICKET_PARSER, "Expired ticket " << MaskTicket(record.Ticket));
                if (!record.AuthorizeRequests.empty()) {
                    record.Error = {"Timed out", true};
                    Respond(record, ctx);
                }
                userTokens.erase(it);
            }
        }
        ctx.Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
    }

    static TStringBuf HtmlBool(bool v) {
        return v ? "<span style='font-weight:bold'>&#x2611;</span>" : "<span style='font-weight:bold'>&#x2610;</span>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordInfo(TStringBuilder& html, const TTokenRecord& record) {
        html << "<tr><td>Subject</td><td>" << record.Subject << "</td></tr>";
        html << "<tr><td>Additional SIDs</td><td>" << JoinStrings(record.AdditionalSIDs.begin(), record.AdditionalSIDs.end(), ", ") << "</td></tr>";
        html << "<tr><td>Error</td><td>" << record.Error << "</td></tr>";
        html << "<tr><td>Requests Infly</td><td>" << record.AuthorizeRequests.size() << "</td></tr>";
        html << "<tr><td>Responses Left</td><td>" << record.ResponsesLeft << "</td></tr>";
        TDerived::WriteTokenRecordTimesInfo(html, record);
        html << "<tr><td>Peer Name</td><td>" << record.PeerName << "</td></tr>";
        if (record.Token != nullptr) {
            html << "<tr><td>User SID</td><td>" << record.Token->GetUserSID() << "</td></tr>";
            for (const TString& group : record.Token->GetGroupSIDs()) {
                html << "<tr><td>Group SID</td><td>" << group << "</td></tr>";
            }
        }
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordTimesInfo(TStringBuilder& html, const TTokenRecord& record) {
        html << "<tr><td>Expire Time</td><td>" << record.ExpireTime << "</td></tr>";
        html << "<tr><td>Access Time</td><td>" << record.AccessTime << "</td></tr>";
    }

    void WriteRefreshTimeValues(TStringBuilder& html) {
        html << "<tr><td>Refresh Period</td><td>" << RefreshPeriod << "</td></tr>";
    }

    void WriteAuthorizeMethods(TStringBuilder& html) {
        html << "<tr><td>Login</td><td>" << HtmlBool(UseLoginProvider) << "</td></tr>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordTimesValues(TStringBuilder& html, const TTokenRecord& record) {
        html << "<td>" << record.ExpireTime << "</td>";
        html << "<td>" << record.AccessTime << "</td>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordValues(TStringBuilder& html, const TString& key, const TTokenRecord& record) {
        html << "<td>" << record.Database << "</td>";
        html << "<td>" << record.Subject << "</td>";
        html << "<td>" << record.Error << "</td>";
        html << "<td>" << "<a href='ticket_parser?token=" << MD5::Calc(key) << "'>" << HtmlBool(record.Token != nullptr) << "</a>" << "</td>";
        html << "<td>" << record.AuthorizeRequests.size() << "</td>";
        html << "<td>" << record.ResponsesLeft << "</td>";
        TDerived::WriteTokenRecordTimesValues(html, record);
        html << "<td>" << record.PeerName << "</td>";
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
            for (const auto& [key, record] : GetDerived()->GetUserTokens()) {
                if (MD5::Calc(key) == token) {
                    html << "<div>";
                    html << "<table class='ticket-parser-proplist'>";
                    html << "<tr><td>Ticket</td><td>" << MaskTicket(record.Ticket) << "</td></tr>";
                    if (record.TokenType == TDerived::ETokenType::Login) {
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
                    TDerived::WriteTokenRecordInfo(html, record);
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
            TDerived::MakeHtmlTable(html);
            html << "</style>";
            html << "</head>";
            html << "<div style='margin-bottom: 10px; margin-left: 100px'>";
            html << "<table class='ticket-parser-proplist'>";
            html << "<tr><td>User Tokens</td><td>" << GetDerived()->GetUserTokens().size() << "</td></tr>";
            html << "<tr><td>Refresh Queue</td><td>" << RefreshQueue.size() << "</td></tr>";
            if (!RefreshQueue.empty()) {
                html << "<tr><td>Refresh Queue Time</td><td>" << RefreshQueue.top().RefreshTime << "</td></tr>";
            }
            GetDerived()->WriteRefreshTimeValues(html);
            html << "<tr><td>Life Time</td><td>" << LifeTime << "</td></tr>";
            html << "<tr><td>Expire Time</td><td>" << ExpireTime << "</td></tr>";
            if (UseLoginProvider) {
                for (const auto& [databaseName, loginProvider] : LoginProviders) {
                    html << "<tr><td>LoginProvider Database</td><td>" << databaseName << " (" << loginProvider.Audience << ")</td></tr>";
                    html << "<tr><td>LoginProvider Keys</td><td>" << GetLoginProviderKeys(loginProvider) << "</td></tr>";
                    html << "<tr><td>LoginProvider Sids</td><td>" << loginProvider.Sids.size() << "</td></tr>";
                }
            }
            GetDerived()->WriteAuthorizeMethods(html);
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
            for (const auto& [key, record] : GetDerived()->GetUserTokens()) {
                html << "<tr>";
                html << "<td>" << MaskTicket(record.Ticket) << "</td>";
                TDerived::WriteTokenRecordValues(html, key, record);
                html << "</tr>";
            }
            html << "</tbody></table>";
            html << "</div>";
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
    }

    void InitCounters(::NMonitoring::TDynamicCounterPtr counters) {
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
    }

    void InitAuthProvider(const NActors::TActorContext&) {
        if (Config.GetUseLoginProvider()) {
            UseLoginProvider = true;
        }
    }

    void InitTime() {
        RefreshPeriod = TDuration::Parse(Config.GetRefreshPeriod());
        RefreshTime = TDuration::Parse(Config.GetRefreshTime());
        MinErrorRefreshTime = TDuration::Parse(Config.GetMinErrorRefreshTime());
        MaxErrorRefreshTime = TDuration::Parse(Config.GetMaxErrorRefreshTime());
        LifeTime = TDuration::Parse(Config.GetLifeTime());
        ExpireTime = TDuration::Parse(Config.GetExpireTime());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::TICKET_PARSER_ACTOR; }

    void Bootstrap(const TActorContext& ctx) {
        TIntrusivePtr<NMonitoring::TDynamicCounters> rootCounters = AppData(ctx)->Counters;
        TIntrusivePtr<NMonitoring::TDynamicCounters> authCounters = GetServiceCounters(rootCounters, "auth");
        NMonitoring::TDynamicCounterPtr counters = authCounters->GetSubgroup("subsystem", "TicketParser");
        GetDerived()->InitCounters(counters);

        GetDerived()->InitAuthProvider(ctx);
        if (AppData() && AppData()->DomainsInfo && !AppData()->DomainsInfo->Domains.empty()) {
            DomainName = "/" + AppData()->DomainsInfo->Domains.begin()->second->Name;
        }

        GetDerived()->InitTime();

        NActors::TMon* mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "ticket_parser", "Ticket Parser", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }

        ctx.Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
        TBase::Become(&TDerived::StateWork);
    }

    TTicketParserImpl(const NKikimrProto::TAuthConfig& authConfig)
        : Config(authConfig) {}
};

}
