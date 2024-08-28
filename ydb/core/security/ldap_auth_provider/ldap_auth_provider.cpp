#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/security/ticket_parser_log.h>
#include <ydb/core/util/address_classifier.h>
#include <queue>
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
        , UrisCreator(Settings, Settings.GetPort() != 0 ? Settings.GetPort() : NKikimrLdap::GetPort(Settings.GetScheme()))
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
        std::vector<TString> directUserGroups;
        char* attribute = NKikimrLdap::FirstAttribute(ld, entry, &ber);
        if (attribute != nullptr) {
            directUserGroups = NKikimrLdap::GetAllValuesOfAttribute(ld, entry, attribute);
            NKikimrLdap::MemFree(attribute);
        }
        if (ber) {
            NKikimrLdap::BerFree(ber, 0);
        }
        std::vector<TString> allUserGroups;
        auto& extendedSettings = Settings.GetExtendedSettings();
        if (extendedSettings.GetEnableNestedGroupsSearch() && !directUserGroups.empty()) {
            // Active Directory has special matching rule to fetch nested groups in one request it is MatchingRuleInChain
            // We don`t know what is ldap server. Is it Active Directory or OpenLdap or other server?
            // If using MatchingRuleInChain return empty list of groups it means that ldap server isn`t Active Directory
            // but it is known that there are groups and we are trying to do tree traversal
            allUserGroups = TryToGetGroupsUseMatchingRuleInChain(ld, entry);
            if (allUserGroups.empty()) {
                allUserGroups = std::move(directUserGroups);
                GetNestedGroups(ld, &allUserGroups);
            }
        } else {
            allUserGroups = std::move(directUserGroups);
        }
        NKikimrLdap::MsgFree(entry);
        NKikimrLdap::Unbind(ld);
        Send(ev->Sender, new TEvLdapAuthProvider::TEvEnrichGroupsResponse(request->Key, request->User, allUserGroups));
    }

    TInitAndBindResponse InitAndBind(LDAP** ld, std::function<THolder<IEventBase>(const TEvLdapAuthProvider::EStatus&, const TEvLdapAuthProvider::TError&)> eventFabric) {
        const auto initializeResponse = InitializeLDAPConnection(ld);
        if (initializeResponse.Error) {
            return {.Success = false, .Event = eventFabric(initializeResponse.Status, initializeResponse.Error)};
        }

        int result = 0;
        if (Settings.GetScheme() != NKikimrLdap::LDAPS_SCHEME && Settings.GetUseTls().GetEnable()) {
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
                .Message = "Could not perform initial LDAP bind for dn " + Settings.GetBindDn() + " on server " + UrisCreator.GetUris() + "\n"
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
        if (TInitializeLdapConnectionResponse response = CheckRequiredSettingsParameters(); response.Status != TEvLdapAuthProvider::EStatus::SUCCESS) {
            return response;
        }

        int result = 0;
        if (Settings.GetScheme() == NKikimrLdap::LDAPS_SCHEME || Settings.GetUseTls().GetEnable()) {
            const TString& caCertificateFile = Settings.GetUseTls().GetCaCertFile();
            result = NKikimrLdap::SetOption(*ld, NKikimrLdap::EOption::TLS_CACERTFILE, caCertificateFile.c_str());
            if (!NKikimrLdap::IsSuccess(result)) {
                NKikimrLdap::Unbind(*ld);
                return {{NKikimrLdap::ErrorToStatus(result),
                        {.Message = "Could not set LDAP ca certificate file \"" + caCertificateFile + "\": " + NKikimrLdap::ErrorToString(result),
                        .Retryable = NKikimrLdap::IsRetryableError(result)}}};
            }
        }

        result = NKikimrLdap::Init(ld, Settings.GetScheme(), UrisCreator.GetUris(), UrisCreator.GetConfiguredPort());
        if (!NKikimrLdap::IsSuccess(result)) {
            return {{TEvLdapAuthProvider::EStatus::UNAVAILABLE,
                    {.Message = "Could not initialize LDAP connection for uris: " + UrisCreator.GetUris() + ". " + NKikimrLdap::LdapError(*ld),
                    .Retryable = false}}};
        }

        result = NKikimrLdap::SetProtocolVersion(*ld);
        if (!NKikimrLdap::IsSuccess(result)) {
            NKikimrLdap::Unbind(*ld);
            return {{NKikimrLdap::ErrorToStatus(result),
                    {.Message = "Could not set LDAP protocol version: " + NKikimrLdap::ErrorToString(result),
                    .Retryable = NKikimrLdap::IsRetryableError(result)}}};
        }

        if (Settings.GetScheme() == NKikimrLdap::LDAPS_SCHEME || Settings.GetUseTls().GetEnable()) {
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
                    {.Message = "Could not get dn for the first entry matching " + FilterCreator.GetFilter(request.Login) + " on server " + UrisCreator.GetUris() + "\n"
                            + NKikimrLdap::LdapError(*request.Ld),
                    .Retryable = false}}};
        }
        TEvLdapAuthProvider::TError error;
        int result = NKikimrLdap::Bind(*request.Ld, dn, request.Password);
        if (!NKikimrLdap::IsSuccess(result)) {
            error.Message = "LDAP login failed for user " + TString(dn) + " on server " + UrisCreator.GetUris() + "\n"
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
            response.Error = {.Message = "Could not search for filter " + searchFilter + " on server " + UrisCreator.GetUris() + "\n"
                                         + NKikimrLdap::ErrorToString(result),
                              .Retryable = NKikimrLdap::IsRetryableError(result)};
            return response;
        }
        const int countEntries = NKikimrLdap::CountEntries(request.Ld, searchMessage);
        if (countEntries != 1) {
            if (countEntries == 0) {
                response.Error  = {.Message = "LDAP user " + request.User + " does not exist. "
                                              "LDAP search for filter " + searchFilter + " on server " + UrisCreator.GetUris() + " return no entries",
                                   .Retryable = false};
            } else {
                response.Error = {.Message = "LDAP user " + request.User + " is not unique. "
                                             "LDAP search for filter " + searchFilter + " on server " + UrisCreator.GetUris() + " return " + countEntries + " entries",
                                  .Retryable = false};
            }
            response.Status = TEvLdapAuthProvider::EStatus::UNAUTHORIZED;
            NKikimrLdap::MsgFree(searchMessage);
            return response;
        }
        response.SearchMessage = searchMessage;
        return response;
    }

    std::vector<TString> TryToGetGroupsUseMatchingRuleInChain(LDAP* ld, LDAPMessage* entry) const {
        static const TString matchingRuleInChain = "1.2.840.113556.1.4.1941"; // Only Active Directory supports
        TStringBuilder filter;
        filter << "(member:" << matchingRuleInChain << ":=" << NKikimrLdap::GetDn(ld, entry) << ')';
        LDAPMessage* searchMessage = nullptr;
        int result = NKikimrLdap::Search(ld, Settings.GetBaseDn(), NKikimrLdap::EScope::SUBTREE, filter, NKikimrLdap::noAttributes, 0, &searchMessage);
        if (!NKikimrLdap::IsSuccess(result)) {
            return {};
        }
        const int countEntries = NKikimrLdap::CountEntries(ld, searchMessage);
        if (countEntries == 0) {
            NKikimrLdap::MsgFree(searchMessage);
            return {};
        }
        std::vector<TString> groups;
        groups.reserve(countEntries);
        for (LDAPMessage* groupEntry = NKikimrLdap::FirstEntry(ld, searchMessage); groupEntry != nullptr; groupEntry = NKikimrLdap::NextEntry(ld, groupEntry)) {
            groups.push_back(NKikimrLdap::GetDn(ld, groupEntry));
        }
        NKikimrLdap::MsgFree(searchMessage);
        return groups;
    }

    void GetNestedGroups(LDAP* ld, std::vector<TString>* groups) {
        std::unordered_set<TString> viewedGroups(groups->cbegin(), groups->cend());
        std::queue<TString> queue;
        for (const auto& group : *groups) {
            queue.push(group);
        }
        while (!queue.empty()) {
            TStringBuilder filter;
            filter << "(|";
            filter << "(entryDn=" << queue.front() << ')';
            queue.pop();
            //should filter string is separated into several batches
            while (!queue.empty()) {
                // entryDn specific for OpenLdap, may get this value from config
                filter << "(entryDn=" << queue.front() << ')';
                queue.pop();
            }
            filter << ')';
            LDAPMessage* searchMessage = nullptr;
            int result = NKikimrLdap::Search(ld, Settings.GetBaseDn(), NKikimrLdap::EScope::SUBTREE, filter, RequestedAttributes, 0, &searchMessage);
            if (!NKikimrLdap::IsSuccess(result)) {
                return;
            }
            if (NKikimrLdap::CountEntries(ld, searchMessage) == 0) {
                NKikimrLdap::MsgFree(searchMessage);
                return;
            }
            for (LDAPMessage* groupEntry = NKikimrLdap::FirstEntry(ld, searchMessage); groupEntry != nullptr; groupEntry = NKikimrLdap::NextEntry(ld, groupEntry)) {
                BerElement* ber = nullptr;
                std::vector<TString> foundGroups;
                char* attribute = NKikimrLdap::FirstAttribute(ld, groupEntry, &ber);
                if (attribute != nullptr) {
                    foundGroups = NKikimrLdap::GetAllValuesOfAttribute(ld, groupEntry, attribute);
                    NKikimrLdap::MemFree(attribute);
                }
                if (ber) {
                    NKikimrLdap::BerFree(ber, 0);
                }
                for (const auto& newGroup : foundGroups) {
                    if (!viewedGroups.contains(newGroup)) {
                        viewedGroups.insert(newGroup);
                        queue.push(newGroup);
                        groups->push_back(newGroup);
                    }
                }
            }
            NKikimrLdap::MsgFree(searchMessage);
        }
    }

    TInitializeLdapConnectionResponse CheckRequiredSettingsParameters() const {
        if (Settings.GetHosts().empty() && Settings.GetHost().empty()) {
            return {TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "List of ldap server hosts is empty", .Retryable = false}};
        }
        if (Settings.GetBaseDn().empty()) {
            return {TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "Parameter BaseDn is empty", .Retryable = false}};
        }
        if (Settings.GetBindDn().empty()) {
            return {TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "Parameter BindDn is empty", .Retryable = false}};
        }
        if (Settings.GetBindPassword().empty()) {
            return {TEvLdapAuthProvider::EStatus::UNAVAILABLE, {.Message = "Parameter BindPassword is empty", .Retryable = false}};
        }
        return {TEvLdapAuthProvider::EStatus::SUCCESS, {}};
    }

private:
    const NKikimrProto::TLdapAuthentication Settings;
    const TSearchFilterCreator FilterCreator;
    const TLdapUrisCreator UrisCreator;
    char* RequestedAttributes[2];
};

IActor* CreateLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings) {
    return new TLdapAuthProvider(settings);
}

}
