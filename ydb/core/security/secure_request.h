#pragma once
#include "ticket_parser.h"
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {

template <typename TBase, typename TDerived, typename TBootstrap = TDerived>
class TSecureRequestActor : public TBase {
protected:
    struct TAuthInfo {
        struct TCertAuth {
            TString ClientCertificate;
            bool NeedAuthByCertificate = false;
        };

        TCertAuth CertAuth;
        TString Token;

        bool IsExists() const {
            return (CertAuth.NeedAuthByCertificate && !CertAuth.ClientCertificate.empty()) || !Token.empty();
        }
    };

private:
    TString Database;
    TString PeerName;
    THolder<TEvTicketParser::TEvAuthorizeTicketResult> AuthorizeTicketResult;
    bool RequireAdminAccess = false;
    bool UserAdmin = false;
    TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> Entries;
    TAuthInfo AuthInfo;

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
        return AuthInfo.IsExists() || !GetDefaultUserSIDs().empty();
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (!result.Error.empty()) {
            if (IsTokenRequired()) {
                if (result.AuthInfo.IsCertificate) {
                    ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                        .Database = Database,
                        .AuthInfo = {.Ticket = AuthInfo.Token},
                        .PeerName = PeerName,
                        .Entries = Entries
                    }));
                    return;
                }
                return static_cast<TDerived*>(this)->OnAccessDenied(result.Error, ctx);
            }
        } else {
            if (RequireAdminAccess) {
                if (!GetAdministrationAllowedSIDs().empty()) {
                    const auto& allowedSIDs(GetAdministrationAllowedSIDs());
                    if (std::find_if(allowedSIDs.begin(), allowedSIDs.end(), [&result](const TString& sid) -> bool { return result.Token->IsExist(sid); }) == allowedSIDs.end()) {
                        return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{"Administrative access denied", false}, ctx);
                    }
                }
                UserAdmin = true;
            }
        }
        AuthorizeTicketResult = ev.Get()->Release();
        static_cast<TBootstrap*>(this)->Bootstrap(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        if (IsTokenRequired()) {
            return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{"Access denied - error parsing token", false}, ctx);
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
        AuthInfo.Token = securityToken;
    }

    void SetAuthInfo(const TAuthInfo& authInfo) {
        AuthInfo = authInfo;
    }

    void SetAuthInfo(TAuthInfo&& authInfo) {
        AuthInfo = std::move(authInfo);
    }

    void SetPeerName(const TString& peerName) {
        PeerName = peerName;
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

    TAuthInfo GetAuthInfo() const {
        return AuthInfo;
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
            return static_cast<TDerived*>(this)->OnAccessDenied(TEvTicketParser::TError{"Access denied without user token", false}, ctx);
        }
        if (!AuthInfo.IsExists()) {
            if (!GetDefaultUserSIDs().empty()) {
                TIntrusivePtr<NACLib::TUserToken> userToken = new NACLib::TUserToken(GetDefaultUserSIDs());
                TEvTicketParser::TAuthInfo authInfo;
                THolder<TEvTicketParser::TEvAuthorizeTicketResult> AuthorizeTicketResult = MakeHolder<TEvTicketParser::TEvAuthorizeTicketResult>(authInfo, userToken);
                ctx.Send(ctx.SelfID, AuthorizeTicketResult.Release());
            } else {
                return static_cast<TBootstrap*>(this)->Bootstrap(ctx);
            }
        } else {
            TEvTicketParser::TAuthInfo authInfo;
            if (AuthInfo.CertAuth.NeedAuthByCertificate) {
                authInfo.Ticket = AuthInfo.CertAuth.ClientCertificate;
                authInfo.IsCertificate = true;
            } else {
                authInfo.Ticket = AuthInfo.Token;
            }
            ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                .Database = Database,
                .AuthInfo = authInfo,
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
