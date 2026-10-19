#include <ydb/core/ydb_convert/ydb_convert.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(RevokeYdbPermission) {
    Y_UNIT_TEST(Connect) {
        using namespace NACLib;
        const TString subject = "user@builtin";
        const TString otherSubject = "other@builtin";

        TACL expected;
        expected.AddAccess(EAccessType::Allow, SelectRow, subject);
        expected.AddAccess(EAccessType::Deny, ConnectDatabase, subject, InheritNone);
        expected.AddAccess(EAccessType::Allow, ConnectDatabase, otherSubject, InheritNone);

        for (const auto& inheritanceTypes : {
            TVector<ui32>{},
            TVector<ui32>{InheritNone},
            TVector<ui32>{DefaultInheritanceType},
            TVector<ui32>{InheritNone, DefaultInheritanceType},
        }) {
            TACL acl(expected);
            for (ui32 inheritance : inheritanceTypes) {
                acl.AddAccess(EAccessType::Allow, ConnectDatabase, subject, inheritance);
            }

            TDiffACL diff;
            RevokeYdbPermission(diff, "ydb.database.connect", subject);
            acl.ApplyDiff(TDiffACL(diff.SerializeAsString()));

            UNIT_ASSERT_VALUES_EQUAL(acl.ToString(), expected.ToString());
        }
    }

    Y_UNIT_TEST(OtherPermissionsKeepInheritanceFilter) {
        using namespace NACLib;
        const TString subject = "user@builtin";

        TACL expected;
        expected.AddAccess(EAccessType::Allow, SelectRow, subject, InheritNone);
        expected.AddAccess(EAccessType::Allow, ConnectDatabase, subject, InheritNone);

        TACL acl(expected);
        acl.AddAccess(EAccessType::Allow, SelectRow, subject);

        TDiffACL diff;
        RevokeYdbPermission(diff, "ydb.granular.select_row", subject);
        acl.ApplyDiff(TDiffACL(diff.SerializeAsString()));

        UNIT_ASSERT_VALUES_EQUAL(acl.ToString(), expected.ToString());
    }
}

} // namespace NKikimr
