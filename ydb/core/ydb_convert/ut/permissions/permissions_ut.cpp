#include <ydb/core/ydb_convert/ydb_convert.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(RevokeYdbPermission) {
    Y_UNIT_TEST(ConnectRevokesLegacyAndCurrentGrants) {
        using namespace NACLib;
        const TString targetSubject = "user@builtin";
        const TString unrelatedSubject = "other@builtin";

        // These entries must survive revoking the target subject's Allow CONNECT grants.
        TACL expectedRemainingAcl;
        expectedRemainingAcl.AddAccess(EAccessType::Allow, SelectRow, targetSubject);
        expectedRemainingAcl.AddAccess(EAccessType::Deny, ConnectDatabase, targetSubject, InheritNone);
        expectedRemainingAcl.AddAccess(EAccessType::Allow, ConnectDatabase, unrelatedSubject, InheritNone);

        for (const auto& connectGrantInheritanceTypes : {
            TVector<ui32>{},
            TVector<ui32>{InheritNone},
            TVector<ui32>{DefaultInheritanceType},
            TVector<ui32>{InheritNone, DefaultInheritanceType},
        }) {
            TACL aclBeforeRevoke(expectedRemainingAcl);
            for (ui32 inheritanceType : connectGrantInheritanceTypes) {
                aclBeforeRevoke.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, inheritanceType);
            }
            UNIT_ASSERT_VALUES_EQUAL(aclBeforeRevoke.ACESize(), expectedRemainingAcl.ACESize() + connectGrantInheritanceTypes.size());

            TDiffACL revokeDiff;
            RevokeYdbPermission("ydb.database.connect", targetSubject, revokeDiff);
            TACL aclAfterRevoke(aclBeforeRevoke);
            aclAfterRevoke.ApplyDiff(TDiffACL(revokeDiff.SerializeAsString()));

            UNIT_ASSERT_VALUES_EQUAL(aclBeforeRevoke.ACESize() - aclAfterRevoke.ACESize(), connectGrantInheritanceTypes.size());
            UNIT_ASSERT_VALUES_EQUAL(aclAfterRevoke.ToString(), expectedRemainingAcl.ToString());
        }
    }

    Y_UNIT_TEST(ConnectPreservesGrantsWithCustomInheritance) {
        using namespace NACLib;
        const TString targetSubject = "user@builtin";

        for (ui32 customInheritanceType : TVector<ui32>{
            InheritObject,
            InheritContainer,
            InheritOnly,
            InheritObject | InheritOnly,
            InheritContainer | InheritOnly,
            DefaultInheritanceType | InheritOnly,
        }) {
            TACL expectedRemainingAcl;
            expectedRemainingAcl.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, customInheritanceType);

            TACL aclBeforeRevoke(expectedRemainingAcl);
            aclBeforeRevoke.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, InheritNone);
            aclBeforeRevoke.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, DefaultInheritanceType);

            TDiffACL revokeDiff;
            RevokeYdbPermission("ydb.database.connect", targetSubject, revokeDiff);
            TACL aclAfterRevoke(aclBeforeRevoke);
            aclAfterRevoke.ApplyDiff(TDiffACL(revokeDiff.SerializeAsString()));

            UNIT_ASSERT_VALUES_EQUAL_C(aclAfterRevoke.ToString(), expectedRemainingAcl.ToString(), customInheritanceType);
        }
    }

    Y_UNIT_TEST(ConnectPreservesCompositePermissionGrants) {
        using namespace NACLib;
        const TString targetSubject = "user@builtin";

        for (ui32 compositeAccessMask : TVector<ui32>{ConnectDatabase | SelectRow, GenericUse, GenericFull}) {
            TACL expectedRemainingAcl;
            expectedRemainingAcl.AddAccess(EAccessType::Allow, compositeAccessMask, targetSubject, InheritNone);
            expectedRemainingAcl.AddAccess(EAccessType::Allow, compositeAccessMask, targetSubject, DefaultInheritanceType);

            TACL aclBeforeRevoke(expectedRemainingAcl);
            aclBeforeRevoke.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, InheritNone);
            aclBeforeRevoke.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, DefaultInheritanceType);

            TDiffACL revokeDiff;
            RevokeYdbPermission("ydb.database.connect", targetSubject, revokeDiff);
            TACL aclAfterRevoke(aclBeforeRevoke);
            aclAfterRevoke.ApplyDiff(TDiffACL(revokeDiff.SerializeAsString()));

            UNIT_ASSERT_VALUES_EQUAL_C(aclAfterRevoke.ToString(), expectedRemainingAcl.ToString(), compositeAccessMask);
        }
    }

    Y_UNIT_TEST(SelectRowRevokesOnlyMatchingInheritance) {
        using namespace NACLib;
        const TString targetSubject = "user@builtin";

        TACL expectedRemainingAcl;
        expectedRemainingAcl.AddAccess(EAccessType::Allow, SelectRow, targetSubject, InheritNone);
        expectedRemainingAcl.AddAccess(EAccessType::Allow, ConnectDatabase, targetSubject, InheritNone);

        TACL aclBeforeRevoke(expectedRemainingAcl);
        aclBeforeRevoke.AddAccess(EAccessType::Allow, SelectRow, targetSubject);

        TDiffACL revokeDiff;
        RevokeYdbPermission("ydb.granular.select_row", targetSubject, revokeDiff);
        TACL aclAfterRevoke(aclBeforeRevoke);
        aclAfterRevoke.ApplyDiff(TDiffACL(revokeDiff.SerializeAsString()));

        UNIT_ASSERT_VALUES_EQUAL(aclAfterRevoke.ToString(), expectedRemainingAcl.ToString());
    }
}

} // namespace NKikimr
