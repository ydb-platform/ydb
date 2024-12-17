#include <queue>
#include "ldap_defines.h"

namespace LdapMock {

namespace {

bool checkFilters(const TSearchRequestInfo::TSearchFilter& filter1, const TSearchRequestInfo::TSearchFilter& filter2) {
    if (filter1.Type != filter2.Type) {
        return false;
    }
    if (filter1.Attribute != filter2.Attribute) {
        return false;
    }
    if (filter1.Value != filter2.Value) {
        return false;
    }
    if (filter1.Type == EFilterType::LDAP_FILTER_EXT) {
        if (filter1.MatchingRule != filter2.MatchingRule) {
            return false;
        }
        if (filter1.DnAttributes != filter2.DnAttributes) {
            return false;
        }
    }
    if (filter1.NestedFilters.size() != filter2.NestedFilters.size()) {
        return false;
    }
    return true;
}

bool AreFiltersEqual(const TSearchRequestInfo::TSearchFilter& filter1, const TSearchRequestInfo::TSearchFilter& filter2) {
    if (!checkFilters(filter1, filter2)) {
        return false;
    }
    std::queue<std::shared_ptr<TSearchRequestInfo::TSearchFilter>> q1;
    for (const auto& filter : filter1.NestedFilters) {
        q1.push(filter);
    }
    std::queue<std::shared_ptr<TSearchRequestInfo::TSearchFilter>> q2;
    for (const auto& filter : filter2.NestedFilters) {
        q2.push(filter);
    }
    while (!q1.empty() && !q2.empty()) {
        const auto filterQ1 = q1.front();
        q1.pop();
        const auto filterQ2 = q2.front();
        q2.pop();
        if (!checkFilters(*filterQ1, *filterQ2)) {
            return false;
        }
        for (const auto& filter : filterQ1->NestedFilters) {
            q1.push(filter);
        }
        for (const auto& filter : filterQ2->NestedFilters) {
            q2.push(filter);
        }
    }
    return true;
}
} // namespace

TBindRequestInfo::TBindRequestInfo(const TString& login, const TString& password)
    : Login(login)
    , Password(password)
{}

TBindRequestInfo::TBindRequestInfo(const TInitializeList& list)
    : TBindRequestInfo(list.Login, list.Password)
{}

bool TBindRequestInfo::operator==(const TBindRequestInfo& otherRequest) const {
    return (this->Login == otherRequest.Login && this->Password == otherRequest.Password);
}

TSearchRequestInfo::TSearchRequestInfo(const TString& baseDn,
                       size_t scope,
                       size_t derefAliases,
                       bool typesOnly,
                       const TSearchFilter& filter,
                       const std::vector<TString>& attributes)
    : BaseDn(baseDn)
    , Scope(scope)
    , DerefAliases(derefAliases)
    , TypesOnly(typesOnly)
    , Filter(filter)
    , Attributes(attributes)
{}

TSearchRequestInfo::TSearchRequestInfo(const TInitializeList& list)
    : TSearchRequestInfo(list.BaseDn,
                         list.Scope,
                         list.DerefAliases,
                         list.TypesOnly,
                         list.Filter,
                         list.Attributes)
{}

bool TSearchRequestInfo::operator==(const TSearchRequestInfo& otherRequest) const {
    if (this->BaseDn != otherRequest.BaseDn) {
        return false;
    }
    if (this->Scope != otherRequest.Scope) {
        return false;
    }
    if (this->DerefAliases != otherRequest.DerefAliases) {
        return false;
    }
    if (this->TypesOnly != otherRequest.TypesOnly) {
        return false;
    }
    const auto& filter = this->Filter;
    const auto& expectedFilter = otherRequest.Filter;
    if (!AreFiltersEqual(filter, expectedFilter)) {
        return false;
    }
    if (this->Attributes != otherRequest.Attributes) {
        return false;
    }
    return true;
}

}
