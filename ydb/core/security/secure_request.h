#pragma once
#include "ticket_parser.h"
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>

namespace NKikimr {

template <typename TBase, typename TDerived, typename TBootstrap = TDerived>
class TSecureRequestActor : public TBase {
private:
    TString Database;
    TString SecurityToken;
    TString PeerName;
    THolder<TEvTicketParser::TEvAuthorizeTicketResult> AuthorizeTicketResult;
    bool RequireAdminAccess = false;
    bool UserAdmin = false;
    TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> Entries;

    static bool GetEnforceUserTokenRequirement() {
        return AppData()->EnforceUserTokenRequirement;
    }

    static bool GetEnforceUserTokenCheckRequirement() {
        return AppData()->EnforceUserTokenCheckRequirement;
    }

    static const TVector<TString>& GetAdministrationAllowedSIDs() {
        return AppData()->AdministrationAllowedSIDs;
    }

    static const TVector<TString>& GetDefaultUserSIDs() {
        return AppData()->DefaultUserSIDs;
    }

    bool IsTokenExists() const {
        return !SecurityToken.empty() || !GetDefaultUserSIDs().empty();
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (!result.Error.empty()) {
            if (IsTokenRequired()) {
                return static_cast<TDerived*>(this)->OnAccessDenied(result.Error, ctx);
            }
        } else {
            if (RequireAdminAccess) {
                UserAdmin = IsTokenAllowed(result.Token.Get(), GetAdministrationAllowedSIDs());
                if (!UserAdmin) {
                    return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{.Message = "Administrative access denied", .Retryable = false}, ctx);

                }
            }
        }
        AuthorizeTicketResult = ev.Get()->Release();
        static_cast<TBootstrap*>(this)->Bootstrap(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        if (IsTokenRequired()) {
            return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{.Message = "Access denied - error parsing token", .Retryable = false}, ctx);
        }
        static_cast<TBootstrap*>(this)->Bootstrap(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ_AUTH;
    }

    template <typename... Args>
    TSecureRequestActor(Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {}

    void SetDatabase(const TString& database) {
        Database = database;
    }

    void SetSecurityToken(const TString& securityToken) {
        SecurityToken = securityToken;
    }

    void SetPeerName(const TString& peerName) {
        PeerName = peerName;
    }

    const TString& GetPeerName() const {
        return PeerName;
    }

    void SetRequireAdminAccess(bool requireAdminAccess) {
        RequireAdminAccess = requireAdminAccess;
    }

    void SetEntries(const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>& entries) {
        Entries = entries;
    }

    const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>& GetEntries() const {
        return Entries;
    }

    const TEvTicketParser::TEvAuthorizeTicketResult* GetAuthorizeTicketResult() const {
        return AuthorizeTicketResult.Get();
    }

    TString GetSecurityToken() const {
        return SecurityToken;
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetParsedToken() const {
        if (AuthorizeTicketResult) {
            return AuthorizeTicketResult->Token;
        }
        return nullptr;
    }

    TString GetSerializedToken() const {
        if (AuthorizeTicketResult) {
            if (AuthorizeTicketResult->Token) {
                return AuthorizeTicketResult->Token->GetSerializedToken();
            }
        }
        return TString();
    }

    TString GetUserSID() const {
        if (AuthorizeTicketResult) {
            if (AuthorizeTicketResult->Token) {
                return AuthorizeTicketResult->Token->GetUserSID();
            }
        }
        const TVector<TString>& defaultUserSIDs = GetDefaultUserSIDs();
        if (!defaultUserSIDs.empty()) {
            return defaultUserSIDs.front();
        }
        return BUILTIN_ACL_ROOT;
    }

    TString GetSanitizedToken() const {
        if (AuthorizeTicketResult) {
            if (AuthorizeTicketResult->Token) {
                return AuthorizeTicketResult->Token->GetSanitizedToken();
            }
        }
        return TString();
    }

    bool IsUserAdmin() const {
        return UserAdmin;
    }

public:
    bool IsTokenRequired() const {
        if (GetEnforceUserTokenRequirement()) {
            return true;
        }

        // Admin access
        if (RequireAdminAccess && !GetAdministrationAllowedSIDs().empty()) {
            return true;
        }

         // Acts in case of !EnforceUserTokenRequirement: If user specify token,
         // it is checked and required to be valid for futher usage of YDB.
         // If user doesn't specify token, no checks are made.
        if (GetEnforceUserTokenCheckRequirement() && IsTokenExists()) {
            return true;
        }

        return false;
    }

    void Bootstrap(const TActorContext& ctx) {
        if (IsTokenRequired() && !IsTokenExists()) {
            return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{.Message = "Access denied without user token", .Retryable = false}, ctx);
        }
        if (SecurityToken.empty()) {
            if (!GetDefaultUserSIDs().empty()) {
                TIntrusivePtr<NACLib::TUserToken> userToken = new NACLib::TUserToken(GetDefaultUserSIDs());
                THolder<TEvTicketParser::TEvAuthorizeTicketResult> AuthorizeTicketResult = MakeHolder<TEvTicketParser::TEvAuthorizeTicketResult>(TString(), userToken);
                ctx.Send(ctx.SelfID, AuthorizeTicketResult.Release());
            } else {
                return static_cast<TBootstrap*>(this)->Bootstrap(ctx);
            }
        } else {
            ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                .Database = Database,
                .Ticket = SecurityToken,
                .PeerName = PeerName,
                .Entries = Entries
            }));
        }
        TBase::Become(&TSecureRequestActor::StateWaitForTicket);
    }

    STFUNC(StateWaitForTicket) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
        }
    }
};

template <typename TDerived>
class TActorBootstrappedSecureRequest : public TSecureRequestActor<TActorBootstrapped<TActorBootstrappedSecureRequest<TDerived>>, TDerived> {
public:
    template <typename... Args>
    TActorBootstrappedSecureRequest(Args&&... args)
        : TSecureRequestActor<TActorBootstrapped<TActorBootstrappedSecureRequest<TDerived>>, TDerived>(std::forward<Args>(args)...)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TSecureRequestActor<TActorBootstrapped<TActorBootstrappedSecureRequest<TDerived>>, TDerived>::Bootstrap(ctx);
    }
};

}
