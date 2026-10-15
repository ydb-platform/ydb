#pragma once
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/login/login.h>
#include <util/string/builder.h>

#include <variant>

namespace NKikimr {
    struct TEvTicketParser {
        enum EEv {
            // requests
            EvAuthorizeTicket = EventSpaceBegin(TKikimrEvents::ES_TICKET_PARSER),
            EvRefreshTicket,
            EvDiscardTicket,
            EvUpdateLoginSecurityState,

            // replies
            EvAuthorizeTicketResult = EvAuthorizeTicket + 512,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TICKET_PARSER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TICKET_PARSER)");

        struct TEvAuthorizeTicket : TEventLocal<TEvAuthorizeTicket, EvAuthorizeTicket> {
            struct TTraceContext {
                TString PeerName;
                TString RequestId;

                TTraceContext(TString peerName, TString requestId)
                    : PeerName(std::move(peerName))
                    , RequestId(std::move(requestId))
                {}
            };

            struct TPermission {
                TString Permission;
                bool Required = false;
            };

            static TPermission Required(const TString& permission) {
                return {
                    .Permission = permission,
                    .Required = true
                };
            }

            static TPermission Optional(const TString& permission) {
                return {
                    .Permission = permission,
                    .Required = false
                };
            }

            static TVector<TPermission> ToPermissions(const TVector<TString>& permissions) {
                TVector<TPermission> result;
                std::transform(permissions.begin(), permissions.end(), std::back_inserter(result),
                   [](const TString& s) { return Optional(s); });
                return result;
            }

            struct TEntry {
                TStackVec<TPermission> Permissions;
                TStackVec<std::pair<TString, TString>> Attributes;

                TEntry(const TVector<TString>& permissions, const TVector<std::pair<TString, TString>>& attributes)
                    : Permissions(ToPermissions(permissions))
                    , Attributes(attributes)
                {}

                TEntry(const TVector<TPermission>& permissions, const TVector<std::pair<TString, TString>>& attributes)
                    : Permissions(permissions)
                    , Attributes(attributes)
                {}
            };

            const TString Database;
            const TString Ticket;
            const TTraceContext TraceContext;

            // if two identical permissions with different attributies are specified,
            // only one of them will be processed. Which one is not guaranteed
            const std::vector<TEntry> Entries;

            struct TAccessKeySignature {
                TString AccessKeyId;
                TString StringToSign;
                TString Signature;

                TInstant SignedAt;
                TString Service;
                TString Region;
            };

            const TAccessKeySignature Signature;

            struct TInitializationFieldsWithTicket {
                TString Ticket;
                TString Database;
                TTraceContext TraceContext;
                std::vector<TEntry> Entries;
            };
            struct TInitializationFieldsWithSignature {
                TAccessKeySignature Signature;
                TString Database;
                TTraceContext TraceContext;
                std::vector<TEntry> Entries;
            };

            TEvAuthorizeTicket(TInitializationFieldsWithTicket&& init)
                : Database(std::move(init.Database))
                , Ticket(std::move(init.Ticket))
                , TraceContext(std::move(init.TraceContext))
                , Entries(std::move(init.Entries))
            {
            }

            TEvAuthorizeTicket(TInitializationFieldsWithSignature&& init)
                : Database(std::move(init.Database))
                , TraceContext(std::move(init.TraceContext))
                , Entries(std::move(init.Entries))
                , Signature(std::move(init.Signature))
            {
            }
        };

        struct TError {
            TString Message;
            TString LogMessage;
            bool Retryable = true;

            bool empty() const {
                return Message.empty() && LogMessage.empty();
            }

            bool HasMessage() const {
                return !Message.empty();
            }

            bool HasLogMessage() const {
                return !LogMessage.empty();
            }

            void clear() {
                Message.clear();
                LogMessage.clear();
                Retryable = true;
            }

            operator bool() const {
                return !empty();
            }

            TString ToString() const {
                return TStringBuilder()
                    << "{message:\"" << Message << "\",retryable:" << Retryable << "}";
            }
        };

        struct TEvAuthorizeTicketResult : TEventLocal<TEvAuthorizeTicketResult, EvAuthorizeTicketResult> {
            using TTokenPtr = TIntrusiveConstPtr<NACLib::TUserToken>;
            using TResult = std::variant<TError, TTokenPtr>;

            const TString Ticket;

            TEvAuthorizeTicketResult(TString ticket, TTokenPtr token)
                : Ticket(std::move(ticket))
                , Result_((token != nullptr)
                    ? TResult(std::move(token))
                    : TResult(TError{.Message = "Empty user token", .Retryable = false}))
            {
            }

            TEvAuthorizeTicketResult(TString ticket, TError error)
                : Ticket(std::move(ticket))
                , Result_(std::move(error))
            {
            }

            bool HasError() const {
                return std::holds_alternative<TError>(Result_);
            }

            // Must check HasError() before this call
            const TError& GetError() const {
                return std::get<TError>(Result_);
            }

            // Must check !HasError() before this call
            TTokenPtr GetToken() const {
                return std::get<TTokenPtr>(Result_);
            }

        private:
            const TResult Result_;
        };

        struct TEvRefreshTicket : TEventLocal<TEvRefreshTicket, EvRefreshTicket> {
            const TString Ticket;

            TEvRefreshTicket(const TString& ticket)
                : Ticket(ticket)
            {}
        };

        struct TEvDiscardTicket : TEventLocal<TEvDiscardTicket, EvDiscardTicket> {
            const TString Ticket;

            TEvDiscardTicket(const TString& ticket)
                : Ticket(ticket)
            {}
        };

        struct TEvUpdateLoginSecurityState : TEventLocal<TEvUpdateLoginSecurityState, EvUpdateLoginSecurityState> {
            NLoginProto::TSecurityState SecurityState;

            TEvUpdateLoginSecurityState(NLoginProto::TSecurityState securityState)
                : SecurityState(std::move(securityState))
            {
            }
        };
    };

    inline NActors::TActorId MakeTicketParserID() {
        const char name[12] = "ticketparse";
        return NActors::TActorId(0, TStringBuf(name, 12));
    }
}

template <>
inline void Out<NKikimr::TEvTicketParser::TError>(IOutputStream& str, const NKikimr::TEvTicketParser::TError& error) {
    str << error.Message;
}

namespace NKikimr {
namespace NGRpcService {

class ICheckerIface {
public:
    virtual void SetEntries(const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>& entries) = 0;
};

}
}
