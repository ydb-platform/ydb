#include <library/cpp/testing/unittest/registar.h>

#include "auth.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(AuthTokenAllowed) {

    const TVector<TString> EmptyList;

    // Empty list allows empty token (regardless of its kind)
    Y_UNIT_TEST(PassOnEmptyListAndEmptyToken) {
        NACLib::TUserToken token(NACLib::TUserToken::TUserTokenInitFields{});
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, EmptyList), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), EmptyList), true);
    }
    Y_UNIT_TEST(PassOnEmptyListAndTokenWithEmptyUserSid) {
        NACLib::TUserToken token({ .UserSID = "" });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, EmptyList), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), EmptyList), true);
    }
    Y_UNIT_TEST(PassOnEmptyListAndTokenWithEmptyUserSidAndGroups) {
        NACLib::TUserToken token({ .UserSID = "", .GroupSIDs = {"group1"} });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, EmptyList), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), EmptyList), true);
    }
    Y_UNIT_TEST(PassOnEmptyListAndNoToken) {
        UNIT_ASSERT_EQUAL(IsTokenAllowed(nullptr, EmptyList), true);
    }
    Y_UNIT_TEST(PassOnEmptyListAndToken) {
        NACLib::TUserToken token({ .UserSID = "user1" });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, EmptyList), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), EmptyList), true);
    }
    Y_UNIT_TEST(PassOnEmptyListAndInvalidTokenSerialized) {
        UNIT_ASSERT_EQUAL(IsTokenAllowed("invalid-serialized-protobuf", EmptyList), true);
    }

    // Non empty list forbids empty token (regardless of its kind)
    Y_UNIT_TEST(FailOnListAndEmptyToken) {
        NACLib::TUserToken token(NACLib::TUserToken::TUserTokenInitFields{});
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"entry"}), false);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"entry"}), false);
    }
    Y_UNIT_TEST(FailOnListAndTokenWithEmptyUserSid) {
        NACLib::TUserToken token({ .UserSID = "" });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"entry"}), false);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"entry"}), false);
    }
    Y_UNIT_TEST(FailOnListAndTokenWithEmptyUserSidAndGroups) {
        NACLib::TUserToken token({ .UserSID = "", .GroupSIDs = {"group1"} });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"entry"}), false);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"entry"}), false);
    }
    Y_UNIT_TEST(FailOnListAndNoToken) {
        UNIT_ASSERT_EQUAL(IsTokenAllowed(nullptr, {"entry"}), false);
    }

    // List matches token
    Y_UNIT_TEST(PassOnListMatchUserSid) {
        NACLib::TUserToken token({ .UserSID = "user1" });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"group1", "group2", "user1", "user2"}), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"group1", "group2", "user1", "user2"}), true);
    }
    Y_UNIT_TEST(PassOnListMatchUserSidWithGroup) {
        NACLib::TUserToken token({ .UserSID = "user1", .GroupSIDs = {"no-match-group"} });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"group1", "group2", "user1", "user2"}), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"group1", "group2", "user1", "user2"}), true);
    }
    Y_UNIT_TEST(PassOnListMatchGroupSid) {
        NACLib::TUserToken token({ .UserSID = "no-match-user", .GroupSIDs = {"group2"} });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"group1", "group2", "user1", "user2"}), true);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"group1", "group2", "user1", "user2"}), true);
    }

    // List does not matchs token
    Y_UNIT_TEST(FailOnListMatchGroupSid) {
        NACLib::TUserToken token({ .UserSID = "no-match-user", .GroupSIDs = {"no-match-group"} });
        UNIT_ASSERT_EQUAL(IsTokenAllowed(&token, {"group1", "group2", "user1", "user2"}), false);
        UNIT_ASSERT_EQUAL(IsTokenAllowed(token.SerializeAsString(), {"group1", "group2", "user1", "user2"}), false);
    }

}
