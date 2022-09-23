#pragma once
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <util/generic/queue.h>

namespace NKikimr {

template <typename T>
class TTicketParserImpl : public TActorBootstrapped<T> {
    using TThis = TTicketParserImpl;
    using TBase = TActorBootstrapped<T>;

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

        TTokenRecordBase(const TStringBuf& ticket)
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
        TStringStream key;
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

    static void EnrichUserTokenWithBuiltins(const TTokenRecordBase& tokenRecord) {
        const TString& allAuthenticatedUsers = AppData()->AllAuthenticatedUsers;
        if (!allAuthenticatedUsers.empty()) {
            tokenRecord.Token->AddGroupSID(allAuthenticatedUsers);
        }
        for (const TString& sid : tokenRecord.AdditionalSIDs) {
            tokenRecord.Token->AddGroupSID(sid);
        }
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
