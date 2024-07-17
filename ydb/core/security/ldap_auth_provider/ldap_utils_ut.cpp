#include <library/cpp/testing/unittest/registar.h>
#include "ldap_utils.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TLdapUtilsTest) {
    Y_UNIT_TEST(GetDefaultFilter) {
        NKikimrProto::TLdapAuthentication settings;
        TSearchFilterCreator filterCreator(settings);
        const TString login {"test_user"};
        const TString expectedFilter {"uid=" + login};
        const TString filter = filterCreator.GetFilter(login);
        UNIT_ASSERT_STRINGS_EQUAL(expectedFilter, filter);
    }

    Y_UNIT_TEST(GetFilterWithoutLoginPlaceholders) {
        NKikimrProto::TLdapAuthentication settings;
        const TString filterString {"&(uid=admin_user)(groupid=1234)"};
        settings.SetSearchFilter(filterString);
        TSearchFilterCreator filterCreator(settings);
        const TString login {"test_user"};
        const TString expectedFilter {filterString};
        const TString filter = filterCreator.GetFilter(login);
        UNIT_ASSERT_STRINGS_EQUAL(expectedFilter, filter);
    }

    Y_UNIT_TEST(GetFilterWithOneLoginPlaceholder) {
        auto getFilterString = [] (const TString& name) {
            return "&(uid=" + name + ")(groupid=1234)";
        };
        NKikimrProto::TLdapAuthentication settings;
        settings.SetSearchFilter(getFilterString("$username"));
        TSearchFilterCreator filterCreator(settings);
        const TString login {"test_user"};
        const TString expectedFilter {getFilterString(login)};
        const TString filter = filterCreator.GetFilter(login);
        UNIT_ASSERT_STRINGS_EQUAL(expectedFilter, filter);
    }

    Y_UNIT_TEST(GetFilterWithSearchAttribute) {
        NKikimrProto::TLdapAuthentication settings;
        const TString searchAttribute {"name"};
        settings.SetSearchAttribute(searchAttribute);
        TSearchFilterCreator filterCreator(settings);
        const TString login {"test_user"};
        const TString expectedFilter {searchAttribute + "=" + login};
        const TString filter = filterCreator.GetFilter(login);
        UNIT_ASSERT_STRINGS_EQUAL(expectedFilter, filter);
    }

    Y_UNIT_TEST(GetFilterWithFewLoginPlaceholders) {
        auto getFilterString = [] (const TString& name) {
            return "|(&(uid=" + name + ")(groupid=1234))(&(login=" + name + ")(groupid=9876))";
        };
        NKikimrProto::TLdapAuthentication settings;
        settings.SetSearchFilter(getFilterString("$username"));
        TSearchFilterCreator filterCreator(settings);
        const TString login {"test_user"};
        const TString expectedFilter {getFilterString(login)};
        const TString filter = filterCreator.GetFilter(login);
        UNIT_ASSERT_STRINGS_EQUAL(expectedFilter, filter);
    }
}

} // namespace NKikimr
