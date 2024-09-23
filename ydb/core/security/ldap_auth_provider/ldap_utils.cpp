#include <util/stream/str.h>
#include "ldap_utils.h"

namespace NKikimr {

TSearchFilterCreator::TSearchFilterCreator(const NKikimrProto::TLdapAuthentication& settings)
    : Settings(settings)
    {}

TString TSearchFilterCreator::GetFilter(const TString& userName) const {
    if (!Settings.GetSearchFilter().empty()) {
        return GetFormatSearchFilter(userName);
    } else if (!Settings.GetSearchAttribute().empty()) {
        return Settings.GetSearchAttribute() + "=" + userName;
    }
    return "uid=" + userName;
}

TString TSearchFilterCreator::GetFormatSearchFilter(const TString& userName) const {
    const TStringBuf namePlaceHolder = "$username";
    const TString& searchFilter = Settings.GetSearchFilter();
    size_t n = searchFilter.find(namePlaceHolder);
    if (n == TString::npos) {
        return searchFilter;
    }
    TStringStream result;
    size_t pos = 0;
    while (n != TString::npos) {
        result << searchFilter.substr(pos, n - pos) << userName;
        pos = n + namePlaceHolder.size();
        n = searchFilter.find(namePlaceHolder, pos);
    }
    result << searchFilter.substr(pos);
    return result.Str();
}

} // namespace NKikimr
