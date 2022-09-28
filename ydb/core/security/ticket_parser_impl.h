#pragma once
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
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
    TDuration LifeTime = TDuration::Hours(1); // for how long ticket will remain in the cache after last access
    TDuration ExpireTime = TDuration::Hours(24); // after what time ticket will expired and removed from cache

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
        TInstant ExpireTime;
        TInstant AccessTime;
        TString PeerName;
        TString Database;
        TStackVec<TString> AdditionalSIDs;

        TTokenRecordBase(const TStringBuf ticket)
            : Ticket(ticket)
        {}

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

    static TStringBuf GetTicketFromKey(const TStringBuf key) {
        return key.Before(':');
    }

    TInstant GetExpireTime(TInstant now) {
        return now + ExpireTime;
    }

    TDuration GetLifeTime() {
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
    void SetTime(TTokenRecord& record, TInstant now) {
        record.InitTime = now;
        record.AccessTime = now;
        record.ExpireTime = GetExpireTime(now);
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString&, TTokenRecord& record, const TActorContext&, TInstant) {
        if (record.TokenType == TDerived::ETokenType::Unknown && record.ResponsesLeft == 0) {
            record.Error.Message = "Could not find correct token validator";
            record.Error.Retryable = false;
        }
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record, const TActorContext& ctx) {
        TInstant now = ctx.Now();
        GetDerived()->SetTime(record, now);

        if (record.Error) {
            return;
        }

        if (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Builtin) {
            if(record.Ticket.EndsWith("@" BUILTIN_ACL_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                GetDerived()->SetToken(key, record, new NACLib::TUserToken({
                    .OriginalUserToken = record.Ticket,
                    .UserSID = record.Ticket,
                    .AuthType = record.GetAuthType()
                }), ctx);
                CounterTicketsBuiltin->Inc();
                return;
            }

            if(record.Ticket.EndsWith("@" BUILTIN_ERROR_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                GetDerived()->SetError(key, record, {"Builtin error simulation"}, ctx);
                CounterTicketsBuiltin->Inc();
                return;
            }
        }

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
                        GetDerived()->SetError(key, record, error, ctx);
                        CounterTicketsLogin->Inc();
                        return;
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
                    GetDerived()->SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = response.User,
                        .GroupSIDs = groups,
                        .AuthType = record.GetAuthType()
                    }), ctx);
                    CounterTicketsLogin->Inc();
                    return;
                }
            } else {
                if (record.TokenType == TDerived::ETokenType::Login) {
                    TEvTicketParser::TError error;
                    error.Message = "Login state is not available yet";
                    error.Retryable = false;
                    GetDerived()->SetError(key, record, error, ctx);
                    CounterTicketsLogin->Inc();
                    return;
                }
            }
        }

        GetDerived()->InitTokenRecord(key, record, ctx, now);
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
        auto& UserTokens = GetDerived()->GetUserTokens();
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

    static TStringBuf HtmlBool(bool v) {
        return v ? "<span style='font-weight:bold'>&#x2611;</span>" : "<span style='font-weight:bold'>&#x2610;</span>";
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::TICKET_PARSER_ACTOR; }

    TTicketParserImpl(const NKikimrProto::TAuthConfig& authConfig)
        : Config(authConfig) {}
};

}
