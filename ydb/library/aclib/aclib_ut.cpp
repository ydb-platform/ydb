#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>
#include "aclib.h"

using namespace NACLib;

Y_UNIT_TEST_SUITE(ACLib) {
    static const TString James = "james@bookstore";
    static const TVector<TString> JamesGroups = {"Humans", "Administrators"};
    static const TString Cat = "cat@bookstore";
    static const TVector<TString> CatGroups = {"Animals", "Cats", "Readers"};
    static const TString Dog = "dog@bookstore";
    static const TVector<TString> DogGroups = {"Animals", "Dogs", "Writers"};

    Y_UNIT_TEST(TestUsers) {
        TSecurityObject rootACL(James, true);

        rootACL.AddAccess(EAccessType::Allow, EAccessRights::GenericRead, Cat, EInheritanceType::InheritContainer);
        rootACL.AddAccess(EAccessType::Allow, EAccessRights::GenericFull, Dog, EInheritanceType::InheritContainer);

        TUserToken jamesToken(James, TVector<TSID>());
        TUserToken catToken(Cat, TVector<TSID>());
        TUserToken dogToken(Dog, TVector<TSID>());

        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == true);
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == true);
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject dogFilesACL(Dog, true);
        TSecurityObject effectiveDogFilesACL(dogFilesACL.MergeWithParent(rootACL));
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject catFilesACL(Cat, true);
        TSecurityObject effectiveCatFilesACL(catFilesACL.MergeWithParent(rootACL));
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject dogFileACL(Dog, true);
        TSecurityObject effectiveDogFileACL(dogFileACL.MergeWithParent(dogFilesACL));
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject catFileACL(Cat, true);
        TSecurityObject effectiveCatFileACL(catFileACL.MergeWithParent(catFilesACL));
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, dogToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == false);
    }

    Y_UNIT_TEST(TestGroups) {
        TSecurityObject rootACL(James, true);

        rootACL.AddAccess(EAccessType::Allow, EAccessRights::GenericRead, "Readers", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        rootACL.AddAccess(EAccessType::Allow, EAccessRights::GenericFull, "Writers", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);

        TUserToken jamesToken(James, JamesGroups);
        TUserToken catToken(Cat, CatGroups);
        TUserToken dogToken(Dog, DogGroups);

        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == true); // owner
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, catToken) == true); // readers
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true); // writers
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == true); // owner
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false); // readers
        UNIT_ASSERT(rootACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true); // writers

        TSecurityObject dogFilesACL(Dog, true);
        TSecurityObject effectiveDogFilesACL(dogFilesACL.MergeWithParent(rootACL));
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject catFilesACL(Cat, true);
        TSecurityObject effectiveCatFilesACL(catFilesACL.MergeWithParent(rootACL));
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject dogFileACL(Dog, true);
        TSecurityObject effectiveDogFileACL(dogFileACL.MergeWithParent(dogFilesACL));
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(effectiveDogFileACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);

        TSecurityObject catFileACL(Cat, true);
        TSecurityObject effectiveCatFileACL(catFileACL.MergeWithParent(catFilesACL));
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::SelectRow, dogToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(effectiveCatFileACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == false);

        TSecurityObject raceFilesACL("Root", true);
        raceFilesACL.AddAccess(EAccessType::Allow, EAccessRights::GenericRead, "Animals", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        raceFilesACL.AddAccess(EAccessType::Allow, EAccessRights::GenericRead, "Humans", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        TSecurityObject effectiveRaceFilesACL(raceFilesACL.MergeWithParent(rootACL));

        TSecurityObject humanFilesACL("SomeHuman", true);
        humanFilesACL.AddAccess(EAccessType::Allow, EAccessRights::GenericFull, "Humans", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        humanFilesACL.AddAccess(EAccessType::Deny, EAccessRights::GenericFull, "Animals", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        TSecurityObject effectiveHumanFilesACL(humanFilesACL.MergeWithParent(raceFilesACL));
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == true);
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == false);
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == false);
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == true);
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(effectiveHumanFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == false);

        TSecurityObject animalFilesACL("SomeAnimal", true);
        animalFilesACL.AddAccess(EAccessType::Allow, EAccessRights::GenericFull, "Animals", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        animalFilesACL.AddAccess(EAccessType::Deny, EAccessRights::GenericFull, "Humans", EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        TSecurityObject effectiveAnimalFilesACL(animalFilesACL.MergeWithParent(raceFilesACL));

        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::SelectRow, jamesToken) == false);
        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::UpdateRow, jamesToken) == false);
        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(effectiveAnimalFilesACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);
    }

    Y_UNIT_TEST(TestDiffACL) {
        TUserToken jamesToken(James, TVector<TSID>());
        TUserToken catToken(Cat, TVector<TSID>());
        TUserToken dogToken(Dog, TVector<TSID>());

        TSecurityObject objACL(James, true /* container */);
        TDiffACL addAccessDiff;
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::AlterSchema, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Dog, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Dog, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Dog, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::AlterSchema, Dog, EInheritanceType::InheritContainer);

        objACL.ApplyDiff(addAccessDiff);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::CreateQueue, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::AlterSchema, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::ReadAttributes, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::WriteAttributes, catToken) == false);

        TDiffACL removeAccessDiff;
        removeAccessDiff.RemoveAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        objACL.ApplyDiff(removeAccessDiff);

        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::UpdateRow, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::AlterSchema, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::SelectRow, catToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::ReadAttributes, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::WriteAttributes, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::CreateQueue, catToken) == false);

        TDiffACL removeAccessDiff2;
        removeAccessDiff2.RemoveAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        removeAccessDiff2.RemoveAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat, EInheritanceType::InheritContainer);
        removeAccessDiff2.RemoveAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);
        removeAccessDiff2.RemoveAccess(EAccessType::Allow, EAccessRights::AlterSchema, Cat, EInheritanceType::InheritContainer);
        objACL.ApplyDiff(removeAccessDiff2);

        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::CreateQueue, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::UpdateRow, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::SelectRow, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::AlterSchema, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::ReadAttributes, catToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::WriteAttributes, catToken) == false);

        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::CreateQueue, dogToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::SelectRow, dogToken) == true);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::AlterSchema, dogToken) == true);

        TDiffACL removeAccessDiff3;
        removeAccessDiff3.ClearAccessForSid(Dog);
        objACL.ApplyDiff(removeAccessDiff3);

        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::CreateQueue, dogToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::UpdateRow, dogToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::SelectRow, dogToken) == false);
        UNIT_ASSERT(objACL.CheckAccess(EAccessRights::AlterSchema, dogToken) == false);
    }

    Y_UNIT_TEST(TestInhertACL) {
        TSecurityObject objParent(James, true /* container */);

        objParent.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        objParent.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat, EInheritanceType::InheritContainer);
        objParent.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);

        TSecurityObject objChild(James, true /* container */);

        objChild.AddAccess(EAccessType::Allow, EAccessRights::CreateTable, Cat, EInheritanceType::InheritContainer);
        objChild.AddAccess(EAccessType::Allow, EAccessRights::RemoveSchema, Cat, EInheritanceType::InheritContainer);

        objChild = objChild.MergeWithParent(objParent);

        UNIT_ASSERT(objChild.GetACL().ACESize() == 5);
        UNIT_ASSERT(objChild.GetImmediateACL().ACESize() == 2);
    }

    Y_UNIT_TEST(TestGrantACL) {
        TSecurityObject objACL(James, true /* container */);
        TUserToken catToken(Cat, TVector<TSID>());

        objACL.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        objACL.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat, EInheritanceType::InheritContainer);
        objACL.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);
        objACL.AddAccess(EAccessType::Allow, EAccessRights::GrantAccessRights, Cat, EInheritanceType::InheritContainer);

        TDiffACL addAccessDiff;
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat, EInheritanceType::InheritContainer);
        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);

        UNIT_ASSERT_EQUAL(objACL.CheckGrantAccess(addAccessDiff, catToken), true);

        addAccessDiff.AddAccess(EAccessType::Allow, EAccessRights::AlterSchema, Cat, EInheritanceType::InheritContainer);

        UNIT_ASSERT_EQUAL(objACL.CheckGrantAccess(addAccessDiff, catToken), false);
    }

    Y_UNIT_TEST(TestClearAccess) {
        TSecurityObject objACL(James, true /* container */);
        TUserToken catToken(Cat, TVector<TSID>());

        objACL.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue, Cat);

        TDiffACL accessDiff;
        accessDiff.ClearAccess();
        accessDiff.AddAccess(EAccessType::Allow, EAccessRights::UpdateRow, Cat);
        accessDiff.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, Cat);
        objACL.ApplyDiff(accessDiff);

        UNIT_ASSERT_EQUAL(objACL.CheckAccess(EAccessRights::CreateQueue, catToken), false);
        UNIT_ASSERT_EQUAL(objACL.CheckAccess(EAccessRights::UpdateRow, catToken), true);
        UNIT_ASSERT_EQUAL(objACL.CheckAccess(EAccessRights::SelectRow, catToken), true);
    }

    Y_UNIT_TEST(TestStrings) {
        TACL objACL;

        objACL.AddAccess(EAccessType::Allow, EAccessRights::CreateQueue | EAccessRights::UpdateRow | EAccessRights::SelectRow, Cat, EInheritanceType::InheritContainer);

        TString str = objACL.ToString();
        NACLibProto::TACE newACE;
        TACL::FromString(newACE, str);
        UNIT_ASSERT_EQUAL(newACE.GetAccessType(), (ui32)EAccessType::Allow);
        UNIT_ASSERT_EQUAL(newACE.GetAccessRight(), EAccessRights::CreateQueue | EAccessRights::UpdateRow | EAccessRights::SelectRow);
        UNIT_ASSERT_EQUAL(newACE.GetInheritanceType(), EInheritanceType::InheritContainer);
    }

    Y_UNIT_TEST(CheckACL) {
        TString ACL = "CiEIARD/DxoYcm9ib3QtcWxvdWQta2lraW1yQHN0YWZmIAMKGggBEP8PGhFyb2JvdC1xbG91ZEBzdGFmZiADCiEIARD/DxoYcm9ib3QtcWxvdWQtY2xpZW50QHN0YWZmIAMKFwgBEP8PGg54ZW5veGVub0BzdGFmZiAD";
        TSecurityObject securityObject("owner@builtin", false);
        Y_PROTOBUF_SUPPRESS_NODISCARD securityObject.MutableACL()->ParseFromString(Base64Decode(ACL));
        TUserToken user("xenoxeno@staff", {});
        ui32 access = EAccessRights::DescribeSchema;
        UNIT_ASSERT(securityObject.CheckAccess(access, user));
    }
}
