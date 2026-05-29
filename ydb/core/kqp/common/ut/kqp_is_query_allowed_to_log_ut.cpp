#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(IsQueryAllowedToLogTest) {

    Y_UNIT_TEST(RegularSelectIsAllowed) {
        UNIT_ASSERT(IsQueryAllowedToLog("SELECT 1"));
    }

    Y_UNIT_TEST(RegularInsertIsAllowed) {
        UNIT_ASSERT(IsQueryAllowedToLog("INSERT INTO t (a) VALUES (1)"));
    }

    Y_UNIT_TEST(CreateUserWithPasswordIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE USER foo PASSWORD 'secret123'"));
    }

    Y_UNIT_TEST(AlterUserWithPasswordIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("ALTER USER foo WITH PASSWORD 'newsecret'"));
    }

    Y_UNIT_TEST(CaseInsensitive) {
        UNIT_ASSERT(!IsQueryAllowedToLog("create user foo password 'secret'"));
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE USER foo PASSWORD 'secret'"));
        UNIT_ASSERT(!IsQueryAllowedToLog("Create User foo Password 'secret'"));
    }

    Y_UNIT_TEST(UserWithoutPasswordIsAllowed) {
        UNIT_ASSERT(IsQueryAllowedToLog("CREATE USER foo"));
    }

    Y_UNIT_TEST(PasswordWithoutUserIsAllowed) {
        UNIT_ASSERT(IsQueryAllowedToLog("SELECT password FROM t"));
    }

    Y_UNIT_TEST(EmptyStringIsAllowed) {
        UNIT_ASSERT(IsQueryAllowedToLog(""));
    }

    Y_UNIT_TEST(EncryptedPasswordIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE USER foo ENCRYPTED PASSWORD 'secret'"));
    }

    Y_UNIT_TEST(UserBeforePasswordRequired) {
        // "password" before "user" should be allowed (the function searches password after user)
        UNIT_ASSERT(IsQueryAllowedToLog("SELECT password FROM user_table"));
    }

    Y_UNIT_TEST(UserAndPasswordInDifferentStatements) {
        // user appears first, then password later — not allowed
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE USER foo; ALTER USER bar PASSWORD 'x'"));
    }

    Y_UNIT_TEST(CreateSecretIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE SECRET name WITH (value = 'test_value')"));
    }

    Y_UNIT_TEST(AlterSecretIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("ALTER SECRET name WITH (value = 'test_value')"));
    }

    Y_UNIT_TEST(CreateObjectTypeSecretIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE OBJECT name (TYPE SECRET) WITH value = \"secretvalue\""));
    }

    Y_UNIT_TEST(UpsertObjectTypeSecretIsNotAllowed) {
        UNIT_ASSERT(!IsQueryAllowedToLog("UPSERT OBJECT name (TYPE SECRET) WITH value = \"secretvalue\""));
    }

    Y_UNIT_TEST(SecretCaseInsensitive) {
        UNIT_ASSERT(!IsQueryAllowedToLog("create secret name WITH (value = 'val')"));
        UNIT_ASSERT(!IsQueryAllowedToLog("CREATE SECRET name WITH (value = 'val')"));
        UNIT_ASSERT(!IsQueryAllowedToLog("Create Secret name WITH (value = 'val')"));
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NKqp
