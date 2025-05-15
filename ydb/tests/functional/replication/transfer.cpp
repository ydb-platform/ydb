#include "utils.h"

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(CreateTransfer_EnterpiseVersion)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
            $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64)
                        |>
                    ];
                };
        )", MainTestCase::CreateTransferSettings::WithExpectedError("The transfer is only available in the Enterprise version"));

        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CreateTransfer_TargetNotFound)
    {
        MainTestCase testCase;
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
            $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64)
                        |>
                    ];
                };
        )", MainTestCase::CreateTransferSettings::WithExpectedError(TStringBuilder() << "Path does not exist"));

        testCase.DropTopic();
    }

    void CheckPermissionOnCreateCase(const std::vector<std::string> permissions, bool success) {
        auto id = RandomNumber<ui16>();
        auto username = TStringBuilder() << "u" << id;

        MainTestCase permissionSetup;
        permissionSetup.ExecuteDDL(Sprintf(R"(
                CREATE USER %s
            )", username.data()));
        permissionSetup.Grant("", username, {"ydb.granular.create_table", "ydb.granular.create_queue"});

        MainTestCase testCase(username);
        permissionSetup.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )", testCase.TableName.data()));
        if (!permissions.empty()) {
            permissionSetup.Grant(testCase.TableName, username, permissions);
        }

        testCase.CreateTopic(1);
        permissionSetup.Grant(testCase.TopicName, username, {"ALL"});

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithExpectedError(success ? "The transfer is only available in the Enterprise version" : "Access denied for scheme request"));

        testCase.DropTopic();
    }

    Y_UNIT_TEST(Create_WithPermission)
    {
        CheckPermissionOnCreateCase({"ydb.generic.write", "ydb.generic.read"}, true);
    }

    Y_UNIT_TEST(Create_WithoutTablePermission)
    {
        CheckPermissionOnCreateCase({"ydb.generic.read"}, false);
    }
}

