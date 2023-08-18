#include "ldap_defines.h"

namespace LdapMock {

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
    if (filter.Type != expectedFilter.Type) {
        return false;
    }
    if (filter.Attribute != expectedFilter.Attribute) {
        return false;
    }
    if (filter.Value != expectedFilter.Value) {
        return false;
    }
    if (this->Attributes != otherRequest.Attributes) {
        return false;
    }
    return true;
}

}
