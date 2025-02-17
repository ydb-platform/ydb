#include <library/cpp/testing/unittest/registar.h>
#include "ldap_utils.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TLdapUtilsSearchFilterCreatorTest) {
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

Y_UNIT_TEST_SUITE(TLdapUtilsUrisCreatorTest) {
    Y_UNIT_TEST(CreateUrisFromHostnames) {
        NKikimrProto::TLdapAuthentication settings;
        *settings.AddHosts() = "test.hostname-001";
        *settings.AddHosts() = "test.hostname-002:1234";
        *settings.AddHosts() = "test.hostname-003:";

        TLdapUrisCreator urisCreator(settings, 389);
        UNIT_ASSERT_VALUES_EQUAL("ldap://test.hostname-001:389 ldap://test.hostname-002:1234 ldap://test.hostname-003:389", urisCreator.GetUris());
    }

    Y_UNIT_TEST(CreateUrisFromIpV4List) {
        NKikimrProto::TLdapAuthentication settings;
        *settings.AddHosts() = "192.168.0.1";
        *settings.AddHosts() = "192.168.0.2:1234";
        *settings.AddHosts() = "192.168.0.3:";

        TLdapUrisCreator urisCreator(settings, 389);
        UNIT_ASSERT_VALUES_EQUAL("ldap://192.168.0.1:389 ldap://192.168.0.2:1234 ldap://192.168.0.3:389", urisCreator.GetUris());
    }

    Y_UNIT_TEST(CreateUrisFromIpV6List) {
         NKikimrProto::TLdapAuthentication settings;
        *settings.AddHosts() = "[2a02:6b8:bf00::]";
        *settings.AddHosts() = "[2a02:6b8:bf01::]:1234";
        *settings.AddHosts() = "[2a02:6b8:bf02::]:";

        TLdapUrisCreator urisCreator(settings, 389);
        UNIT_ASSERT_VALUES_EQUAL("ldap://[2a02:6b8:bf00::]:389 ldap://[2a02:6b8:bf01::]:1234 ldap://[2a02:6b8:bf02::]:389", urisCreator.GetUris());
    }

    Y_UNIT_TEST(CreateUrisFromHostnamesLdapsScheme) {
        NKikimrProto::TLdapAuthentication settings;
        *settings.AddHosts() = "test.hostname-001";
        *settings.AddHosts() = "test.hostname-002:1234";
        *settings.AddHosts() = "test.hostname-003:";
        settings.SetScheme("ldaps");

        TLdapUrisCreator urisCreator(settings, 389);
        UNIT_ASSERT_VALUES_EQUAL("ldaps://test.hostname-001:389 ldaps://test.hostname-002:1234 ldaps://test.hostname-003:389", urisCreator.GetUris());
    }

    Y_UNIT_TEST(CreateUrisFromHostnamesUnknownScheme) {
        NKikimrProto::TLdapAuthentication settings;
        *settings.AddHosts() = "test.hostname-001";
        *settings.AddHosts() = "test.hostname-002:1234";
        *settings.AddHosts() = "test.hostname-003:";
        settings.SetScheme("http");

        TLdapUrisCreator urisCreator(settings, 389);
        UNIT_ASSERT_VALUES_EQUAL("ldap://test.hostname-001:389 ldap://test.hostname-002:1234 ldap://test.hostname-003:389", urisCreator.GetUris());
    }
}

} // namespace NKikimr
