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
        )", MainTestCase::CreateTransferSettings::WithExpectedError(TStringBuilder() << "The transfer destination path '/local/" << testCase.TableName << "' not found"));

        testCase.DropTopic();
    }
}

