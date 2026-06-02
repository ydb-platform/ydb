#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include "ldap_utils.h"

namespace NKikimr {

namespace {

// Escape LDAP filter value according to https://www.rfc-editor.org/rfc/rfc2254#page-5
TString EscapeLdapFilterValue(TStringBuf value) {
    TStringBuilder escaped;
    escaped.reserve(value.size() + 4);
    for (unsigned char ch : value) {
        switch (ch) {
            case '*':
                escaped << "\\2a";
                break;
            case '(':
                escaped << "\\28";
                break;
            case ')':
                escaped << "\\29";
                break;
            case '\\':
                escaped << "\\5c";
                break;
            case '\0':
                escaped << "\\00";
                break;
            default:
                escaped << static_cast<char>(ch);
                break;
        }
    }
    return TString{escaped};
}

} // namespace

TSearchFilterCreator::TSearchFilterCreator(const NKikimrProto::TLdapAuthentication& settings)
    : Settings(settings)
{}

TString TSearchFilterCreator::GetFilter(const TString& userName) const {
    const TString escapedUserName = EscapeLdapFilterValue(userName);
    if (!Settings.GetSearchFilter().empty()) {
        return GetFormatSearchFilter(escapedUserName);
    } else if (!Settings.GetSearchAttribute().empty()) {
        return Settings.GetSearchAttribute() + "=" + escapedUserName;
    }
    return "uid=" + escapedUserName;
}

TString TSearchFilterCreator::GetFormatSearchFilter(const TString& escapedUserName) const {
    const TStringBuf namePlaceHolder = "$username";
    const TString& searchFilter = Settings.GetSearchFilter();
    size_t n = searchFilter.find(namePlaceHolder);
    if (n == TString::npos) {
        return searchFilter;
    }
    TStringStream result;
    size_t pos = 0;
    while (n != TString::npos) {
        result << searchFilter.substr(pos, n - pos) << escapedUserName;
        pos = n + namePlaceHolder.size();
        n = searchFilter.find(namePlaceHolder, pos);
    }
    result << searchFilter.substr(pos);
    return result.Str();
}

TLdapUrisCreator::TLdapUrisCreator(const NKikimrProto::TLdapAuthentication& settings, ui32 configuredPort)
    : Settings(settings)
    , Scheme(Settings.GetScheme() == "ldaps" ? Settings.GetScheme() : "ldap")
    , ConfiguredPort(configuredPort)
{}

TString TLdapUrisCreator::GetUris() const {
    if (Uris.empty()) {
        Uris = CreateUrisList();
    }
    return Uris;
}

ui32 TLdapUrisCreator::GetConfiguredPort() const {
    return ConfiguredPort;
}

TString TLdapUrisCreator::CreateUrisList() const {
    TStringBuilder uris;
    if (Settings.HostsSize() > 0) {
        for (const auto& host : Settings.GetHosts()) {
            uris << CreateUri(host) << " ";
        }
        uris.remove(uris.size() - 1);
    } else {
        uris << CreateUri(Settings.GetHost());
    }
    return uris;
}

TString TLdapUrisCreator::CreateUri(const TString& address) const {
    TString hostname;
    ui32 port = 0;
    size_t first_colon_pos = address.find(':');
    if (first_colon_pos != TString::npos) {
        size_t last_colon_pos = address.rfind(':');
        if (last_colon_pos == first_colon_pos) {
            // only one colon, simple case
            try {
                port = FromString<ui32>(address.substr(first_colon_pos + 1));
            } catch (TFromStringException& ex) {
                port = 0;
            }
            hostname = address.substr(0, first_colon_pos);
        } else {
            // ipv6?
            size_t closing_bracket_pos = address.rfind(']');
            if (closing_bracket_pos == TString::npos || closing_bracket_pos > last_colon_pos) {
                // whole address is ipv6 host
                hostname = address;
            } else {
                try {
                    port = FromString<ui32>(address.substr(last_colon_pos + 1));
                } catch (TFromStringException& ex) {
                    port = 0;
                }
                hostname = address.substr(0, last_colon_pos);
            }
        }
    } else {
        hostname = address;
    }
    port = (port != 0) ? port : ConfiguredPort;
    return TStringBuilder() << Scheme << "://" << hostname << ':' << port;
}

} // namespace NKikimr
