#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/ticket_parser.h>
#include "ticket_parser_log.h"
#include "ldap_auth_provider.h"
#include "ldap_utils.h"

// This temporary solution
// These lines should be declared outside ldap_compat.h
// because ldap_compat.h is included in other files where next structures are defined
struct ldap;
struct ldapmsg;
struct berelement;

// Next usings were added for compatibility with names of structures from ldap library
using LDAP = ldap;
using LDAPMessage = ldapmsg;
using BerElement = berelement;

#include "ldap_compat.h"

namespace NKikimr {

class TLdapAuthProvider : public NActors::TActorBootstrapped<TLdapAuthProvider> {
private:
    using TThis = TLdapAuthProvider;
    using TBase = NActors::TActorBootstrapped<TLdapAuthProvider>;

    struct TBasicRequest {};

    struct TSearchUserRequest : TBasicRequest {
        LDAP* Ld = nullptr;
        TString User;
        char** RequestedAttributes = nullptr;
    };

    struct TGetAttributeValuesRequest : TBasicRequest {
        LDAP* Ld = nullptr;
        LDAPMessage* Entry = nullptr;
        char* Attribute = nullptr;
    };

    struct TAuthenticateUserRequest : TBasicRequest {
        LDAP** Ld = nullptr;
        LDAPMessage* Entry = nullptr;
        TString Login;
        TString Password;
    };

    struct TBasicResponse {
        TEvLdapAuthProvider::EStatus Status = TEvLdapAuthProvider::EStatus::SUCCESS;
        TEvLdapAuthProvider::TError Error;
    };

    struct TInitializeLdapConnectionResponse : TBasicResponse {};

    struct TSearchUserResponse : TBasicResponse {
        LDAPMessage* SearchMessage = nullptr;
    };

    struct TAuthenticateUserResponse : TBasicResponse {};

    struct TInitAndBindResponse {
        bool Success = true;
        THolder<IEventBase> Event;
    };

public:
    TLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings)
        : Settings(settings)
        , FilterCreator(Settings)
    {
        const TString& requestedGroupAttribute = Settings.GetRequestedGroupAttribute();
        RequestedAttributes[0] = const_cast<char*>(requestedGroupAttribute.empty() ? "memberOf" : requestedGroupAttribute.c_str());
        RequestedAttributes[1] = nullptr;
    }

    void Bootstrap() {
        TBase::Become(&TThis::StateWork);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLdapAuthProvider::TEvEnrichGroupsRequest, Handle);
            hFunc(TEvLdapAuthProvider::TEvAuthenticateRequest, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

private:
    void Handle(TEvLdapAuthProvider::TEvAuthenticateRequest::TPtr& ev) {
        TEvLdapAuthProvider::TEvAuthenticateRequest* request = ev->Get();
        LDAP* ld = nullptr;
        auto initAndBindResult = InitAndBind(&ld, [](const TEvLdapAuthProvider::EStatus& status, const TEvLdapAuthProvider::TError& error) {
            return MakeHolder<TEvLdapAuthProvider::TEvAuthenticateResponse>(status, error);
        });
        if (!initAndBindResult.Success) {
            Send(ev->Sender, initAndBindResult.Event.Release());
            return;
        }

        TSearchUserResponse searchUserResponse = SearchUserRecord({.Ld = ld,
                                                                .User = request->Login,
                                                                .RequestedAttributes = NKikimrLdap::noAttributes});
        if (searchUserResponse.Status != TEvLdapAuthProvider::EStatus::SUCCESS) {
            NKikimrLdap::Unbind(ld);
            Send(ev->Sender, new TEvLdapAuthProvider::TEvAuthenticateResponse(searchUserResponse.Status, searchUserResponse.Error));
            return;
        }
        auto entry = NKikimrLdap::FirstEntry(ld, searchUserResponse.SearchMessage);
        TAuthenticateUserResponse authResponse = AuthenticateUser({.Ld = &ld, .Entry = entry, .Login = request->Login, .Password = request->Password});
        NKikimrLdap::MsgFree(entry);
        NKikimrLdap::Unbind(ld);
        Send(ev->Sender, new TEvLdapAuthProvider::TEvAuthenticateResponse(authResponse.Status, authResponse.Error));
    }

    void Handle(TEvLdapAuthProvider::TEvEnrichGroupsRequest::TPtr& ev) {
        TEvLdapAuthProvider::TEvEnrichGroupsRequest* request = ev->Get();
        LDAP* ld = nullptr;
        auto initAndBindResult = InitAndBind(&ld, [&request](const TEvLdapAuthProvider::EStatus& status, const TEvLdapAuthProvider::TError& error) {
            return MakeHolder<TEvLdapAuthProvider::TEvEnrichGroupsResponse>(request->Key, status, error);
        });
        if (!initAndBindResult.Success) {
            Send(ev->Sender, initAndBindResult.Event.Release());
            return;
        }

        TSearchUserResponse searchUserResponse = SearchUserRecord({.Ld = ld,
                                                                .User = request->User,
                                                                .RequestedAttributes = RequestedAttributes});
        if (searchUserResponse.Status != TEvLdapAuthProvider::EStatus::SUCCESS) {
            NKikimrLdap::Unbind(ld);
            Send(ev->Sender, new TEvLdapAuthProvider::TEvEnrichGroupsResponse(request->Key, searchUserResponse.Status, searchUserResponse.Error));
            return;
        }
        LDAPMessage* entry = NKikimrLdap::FirstEntry(ld, searchUserResponse.SearchMessage);
        BerElement* ber = nullptr;
        std::vector<TString> groupsDn;
        char* attribute = NKikimrLdap::FirstAttribute(ld, entry, &ber);
        if (attribute != nullptr) {
            groupsDn = NKikimrLdap::GetAllValuesOfAttribute(ld, entry, attribute);
            NKikimrLdap::MemFree(attribute);
        }
        if (ber) {
            NKikimrLdap::BerFree(ber, 0);
        }
        NKikimrLdap::MsgFree(entry);
        NKikimrLdap::Unbind(ld);
        Send(ev->Sender, new TEvLdapAuthProvider::TEvEnrichGroupsResponse(request->Key, request->User, groupsDn));
    }

    TInitAndBindResponse InitAndBind(LDAP** ld, std::function<THolder<IEventBase>(const TEvLdapAuthProvider::EStatus&, const TEvLdapAuthProvider::TError&)> eventFabric) {
        const auto initializeResponse = InitializeLDAPConnection(ld);
        if (initializeResponse.Error) {
            return {.Success = false, .Event = eventFabric(initializeResponse.Status, initializeResponse.Error)};
        }

        int result = 0;
        if (Settings.GetUseTls().GetEnable()) {
            result = NKikimrLdap::StartTLS(*ld);
            if (!NKikimrLdap::IsSuccess(result)) {
                TEvLdapAuthProvider::TError error {
                    .Message = "Could not start TLS\n" + NKikimrLdap::ErrorToString(result),
                    .Retryable = NKikimrLdap::IsRetryableError(result)
                };
                // The Unbind operation is not the antithesis of the Bind operation as the name implies.
                // Close the LDAP connection, free the resources contained in the LDAP structure
                NKikimrLdap::Unbind(*ld);
                return {.Success = false, .Event = eventFabric(NKikimrLdap::ErrorToStatus(result), error)};
            }
        }

        result = NKikimrLdap::Bind(*ld, Settings.GetBindDn(), Settings.GetBindPassword());
        if (!NKikimrLdap::IsSuccess(result)) {
            TEvLdapAuthProvider::TError error {
                .Message = "Could not perform initial LDAP bind for dn " + Settings.GetBindDn() + " on server " + Settings.GetHost() + "\n"
                            + NKikimrLdap::ErrorToString(result),
                .Retryable = NKikimrLdap::IsRetryableError(result)
            };
            // The Unbind operation is not the antithesis of the Bind operation as the name implies.
            // Close the LDAP connection, free the resources contained in the LDAP structure
            NKikimrLdap::Unbind(*ld);
            return {.Success = false, .Event = eventFabric(NKikimrLdap::ErrorToStatus(result), error)};
        }
        return {};
    }

    TInitializeLdapConnectionResponse InitializeLDAPConnection(LDAP** ld) {
        const TString& host = Settings.GetHost();
        if (host.empty()) {
            return {{TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "Ldap server host is empty", .Retryable = false}}};
        }

        const ui32 port = Settings.GetPort() != 0 ? Settings.GetPort() : NKikimrLdap::GetPort();

        int result = 0;
        if (Settings.GetUseTls().GetEnable()) {
            const TString& caCertificateFile = Settings.GetUseTls().GetCaCertFile();
            result = NKikimrLdap::SetOption(*ld, NKikimrLdap::EOption::TLS_CACERTFILE, caCertificateFile.c_str());
            if (!NKikimrLdap::IsSuccess(result)) {
                NKikimrLdap::Unbind(*ld);
                return {{NKikimrLdap::ErrorToStatus(result),
                        {.Message = "Could not set LDAP ca certificate file \"" + caCertificateFile + "\": " + NKikimrLdap::ErrorToString(result),
                        .Retryable = NKikimrLdap::IsRetryableError(result)}}};
            }
        }

        *ld = NKikimrLdap::Init(host, port);
        if (*ld == nullptr) {
            return {{TEvLdapAuthProvider::EStatus::UNAVAILABLE,
                    {.Message = "Could not initialize LDAP connection for host: " + host + ", port: " + ToString(port) + ". " + NKikimrLdap::LdapError(*ld),
                    .Retryable = false}}};
        }

        result = NKikimrLdap::SetProtocolVersion(*ld);
        if (!NKikimrLdap::IsSuccess(result)) {
            NKikimrLdap::Unbind(*ld);
            return {{NKikimrLdap::ErrorToStatus(result),
                    {.Message = "Could not set LDAP protocol version: " + NKikimrLdap::ErrorToString(result),
                    .Retryable = NKikimrLdap::IsRetryableError(result)}}};
        }

        if (Settings.GetUseTls().GetEnable()) {
            int requireCert = NKikimrLdap::ConvertRequireCert(Settings.GetUseTls().GetCertRequire());
            result = NKikimrLdap::SetOption(*ld, NKikimrLdap::EOption::TLS_REQUIRE_CERT, &requireCert);
            if (!NKikimrLdap::IsSuccess(result)) {
                NKikimrLdap::Unbind(*ld);
                return {{NKikimrLdap::ErrorToStatus(result),
                        {.Message = "Could not set require certificate option: " + NKikimrLdap::ErrorToString(result),
                        .Retryable = NKikimrLdap::IsRetryableError(result)}}};
            }
        }
        return {};
    }

    TAuthenticateUserResponse AuthenticateUser(const TAuthenticateUserRequest& request) {
        char* dn = NKikimrLdap::GetDn(*request.Ld, request.Entry);
        if (dn == nullptr) {
            return {{TEvLdapAuthProvider::EStatus::UNAUTHORIZED,
                    {.Message = "Could not get dn for the first entry matching " + FilterCreator.GetFilter(request.Login) + " on server " + Settings.GetHost() + "\n"
                            + NKikimrLdap::LdapError(*request.Ld),
                    .Retryable = false}}};
        }
        TEvLdapAuthProvider::TError error;
        int result = NKikimrLdap::Bind(*request.Ld, dn, request.Password);
        if (!NKikimrLdap::IsSuccess(result)) {
            error.Message = "LDAP login failed for user " + TString(dn) + " on server " + Settings.GetHost() + "\n"
                            + NKikimrLdap::ErrorToString((result));
            error.Retryable = NKikimrLdap::IsRetryableError(result);
        }
        NKikimrLdap::MemFree(dn);
        return {{NKikimrLdap::ErrorToStatus(result), error}};
    }

    TSearchUserResponse SearchUserRecord(const TSearchUserRequest& request) {
        LDAPMessage* searchMessage = nullptr;
        const TString searchFilter = FilterCreator.GetFilter(request.User);

        int result = NKikimrLdap::Search(request.Ld,
                                        Settings.GetBaseDn(),
                                        NKikimrLdap::EScope::SUBTREE,
                                        searchFilter,
                                        request.RequestedAttributes,
                                        0,
                                        &searchMessage);
        TSearchUserResponse response;
        if (!NKikimrLdap::IsSuccess(result)) {
            response.Status = NKikimrLdap::ErrorToStatus(result);
            response.Error = {.Message = "Could not search for filter " + searchFilter + " on server " + Settings.GetHost() + "\n"
                                         + NKikimrLdap::ErrorToString(result),
                              .Retryable = NKikimrLdap::IsRetryableError(result)};
            return response;
        }
        const int countEntries = NKikimrLdap::CountEntries(request.Ld, searchMessage);
        if (countEntries != 1) {
            if (countEntries == 0) {
                response.Error  = {.Message = "LDAP user " + request.User + " does not exist. "
                                              "LDAP search for filter " + searchFilter + " on server " + Settings.GetHost() + " return no entries",
                                   .Retryable = false};
            } else {
                response.Error = {.Message = "LDAP user " + request.User + " is not unique. "
                                             "LDAP search for filter " + searchFilter + " on server " + Settings.GetHost() + " return " + countEntries + " entries",
                                  .Retryable = false};
            }
            response.Status = TEvLdapAuthProvider::EStatus::UNAUTHORIZED;
            NKikimrLdap::MsgFree(searchMessage);
            return response;
        }
        response.SearchMessage = searchMessage;
        return response;
    }

private:
    const NKikimrProto::TLdapAuthentication Settings;
    const TSearchFilterCreator FilterCreator;
    char* RequestedAttributes[2];
};

IActor* CreateLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings) {
    return new TLdapAuthProvider(settings);
}

}
