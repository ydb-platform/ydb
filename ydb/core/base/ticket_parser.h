#pragma once
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/login/login.h>
#include <util/string/builder.h>

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
            const TString PeerName;

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

            struct TInitializationFields {
                TString Database;
                TString Ticket;
                TString PeerName;
                std::vector<TEntry> Entries;
            };

            TEvAuthorizeTicket(TInitializationFields&& init)
                : Database(std::move(init.Database))
                , Ticket(std::move(init.Ticket))
                , PeerName(std::move(init.PeerName))
                , Entries(std::move(init.Entries))
            {
            }

            TEvAuthorizeTicket(const TString& ticket)
                : Ticket(ticket)
            {}

            TEvAuthorizeTicket(const TString& ticket, const TString& peerName)
                : Ticket(ticket)
                , PeerName(peerName)
            {}

            TEvAuthorizeTicket(const TString& ticket, const TVector<std::pair<TString, TString>>& attributes, const TVector<TString>& permissions)
                : Ticket(ticket)
                , Entries({{ToPermissions(permissions), attributes}})
            {}

            TEvAuthorizeTicket(const TString& ticket, const TString& peerName, const TVector<std::pair<TString, TString>>& attributes, const TVector<TString>& permissions)
                : Ticket(ticket)
                , PeerName(peerName)
                , Entries({{ToPermissions(permissions), attributes}})
            {}

            TEvAuthorizeTicket(const TString& ticket, const TVector<std::pair<TString, TString>>& attributes, const TVector<TPermission>& permissions)
                : Ticket(ticket)
                , Entries({{permissions, attributes}})
            {}

            TEvAuthorizeTicket(const TString& ticket, const TString& peerName, const TVector<std::pair<TString, TString>>& attributes, const TVector<TPermission>& permissions)
                : Ticket(ticket)
                , PeerName(peerName)
                , Entries({{permissions, attributes}})
            {}

            TEvAuthorizeTicket(const TString& ticket, const TVector<TEntry>& entries)
                : Ticket(ticket)
                , Entries(entries)
            {}

            TEvAuthorizeTicket(const TString& ticket, const TString& peerName, const TVector<TEntry>& entries)
                : Ticket(ticket)
                , PeerName(peerName)
                , Entries(entries)
            {}

            TEvAuthorizeTicket(TAccessKeySignature&& sign, const TString& peerName, const TVector<TEntry>& entries)
                : Ticket("")
                , PeerName(peerName)
                , Entries(entries)
                , Signature(std::move(sign))
            {}

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
            TString Ticket;
            TError Error;
            TIntrusiveConstPtr<NACLib::TUserToken> Token;
            const TString SerializedToken;

            TEvAuthorizeTicketResult(const TString& ticket, const TIntrusiveConstPtr<NACLib::TUserToken>& token)
                : Ticket(ticket)
                , Token(token)
                , SerializedToken(token ? token->GetSerializedToken() : "")
            {
            }

            TEvAuthorizeTicketResult(const TString& ticket, const TError& error)
                : Ticket(ticket)
                , Error(error)
            {}
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
