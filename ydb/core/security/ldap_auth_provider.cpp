#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/ticket_parser.h>
#include "ticket_parser_log.h"
#include <util/generic/string.h>
#include "ldap_auth_provider.h"

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

    struct TBasicResponse {
        TEvLdapAuthProvider::EStatus Status = TEvLdapAuthProvider::EStatus::SUCCESS;
        TEvLdapAuthProvider::TError Error;
    };

    struct TInitializeLdapConnectionResponse : TBasicResponse {};

    struct TSearchUserResponse : TBasicResponse {
        LDAPMessage* SearchMessage = nullptr;
    };

public:
    TLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings)
        : Settings(settings)
    {}

    void Bootstrap() {
        TBase::Become(&TThis::StateWork);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLdapAuthProvider::TEvEnrichGroupsRequest, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

private:
    void Handle(TEvLdapAuthProvider::TEvEnrichGroupsRequest::TPtr& ev) {
        TEvLdapAuthProvider::TEvEnrichGroupsRequest* request = ev->Get();
        LDAP* ld = nullptr;
        const auto initializeResponse = InitializeLDAPConnection(&ld);
        if (initializeResponse.Error) {
            Send(ev->Sender, new TEvLdapAuthProvider::TEvEnrichGroupsResponse(request->Key, initializeResponse.Status, initializeResponse.Error));
            return;
        }

        int result = NKikimrLdap::Bind(ld, Settings.GetBindDn(), Settings.GetBindPassword());
        if (!NKikimrLdap::IsSuccess(result)) {
            TEvLdapAuthProvider::TError error {
                .Message = "Could not perform initial LDAP bind for dn " + Settings.GetBindDn() + " on server " + Settings.GetHost() + "\n"
                            + NKikimrLdap::ErrorToString(result),
                .Retryable = NKikimrLdap::IsRetryableError(result)
            };
            NKikimrLdap::Unbind(ld);
            Send(ev->Sender, new TEvLdapAuthProvider::TEvEnrichGroupsResponse(request->Key, NKikimrLdap::ErrorToStatus(result), error));
            return;
        }

        char* requestedAttributes[] = {const_cast<char*>("memberOf"), nullptr};
        TSearchUserResponse searchUserResponse = SearchUserRecord({.Ld = ld,
                                                                .User = request->User,
                                                                .RequestedAttributes = requestedAttributes});
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

    TInitializeLdapConnectionResponse InitializeLDAPConnection(LDAP** ld) {
        const TString& host = Settings.GetHost();
        if (host.empty()) {
            return {{TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "Ldap server host is empty", .Retryable = false}}};
        }

        const ui32 port = Settings.GetPort() != 0 ? Settings.GetPort() : NKikimrLdap::GetPort();

        *ld = NKikimrLdap::Init(host, port);
        if (*ld == nullptr) {

        return {{TEvLdapAuthProvider::EStatus::UNAVAILABLE,
                {.Message = "Could not initialize LDAP connection for host: " + host + ", port: " + ToString(port) + ". " + NKikimrLdap::LdapError(*ld),
                .Retryable = false}}};
        }

        int result = NKikimrLdap::SetProtocolVersion(*ld);
        if (!NKikimrLdap::IsSuccess(result)) {
            NKikimrLdap::Unbind(*ld);
            return {{NKikimrLdap::ErrorToStatus(result),
                    {.Message = "Could not set LDAP protocol version: " + NKikimrLdap::ErrorToString(result),
                    .Retryable = NKikimrLdap::IsRetryableError(result)}}};
        }
        return {};
    }

    TSearchUserResponse SearchUserRecord(const TSearchUserRequest& request) {
        LDAPMessage* searchMessage = nullptr;
        const TString searchFilter = GetFilter(request.User);

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

    TString GetFilter(const TString& userName) const {
        if (!Settings.GetSearchFilter().empty()) {
            return GetFormatSearchFilter(userName);
        } else if (!Settings.GetSearchAttribute().empty()) {
            return Settings.GetSearchAttribute() + "=" + userName;
        } else {
            return "uid=" + userName;
        }
    }

    TString GetFormatSearchFilter(const TString& userName) const {
        const TStringBuf namePlaceHolder = "$username";
        const TString& searchFilter = Settings.GetSearchFilter();
        size_t n = searchFilter.find(namePlaceHolder);
        if (n == TString::npos) {
            return searchFilter;
        }
        return searchFilter.substr(0, n) + userName + searchFilter.substr(n + namePlaceHolder.size());
    }

private:
    const NKikimrProto::TLdapAuthentication Settings;
};

IActor* CreateLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings) {
    return new TLdapAuthProvider(settings);
}

}
