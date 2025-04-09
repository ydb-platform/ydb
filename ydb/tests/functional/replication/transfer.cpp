#include "utils.h"

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(CreateTransfer)
    {
        MainTestCase testCase;
        testCase.ExecuteDDL(Sprintf(R"(
            $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64)
                        |>
                    ];
                };

            CREATE TRANSFER `%s`
            FROM `SourceTopic` TO `TargetTable` USING $l
            WITH (
                CONNECTION_STRING = 'grpc://localhost'
            );
        )", testCase.TransferName.data()), true, "The transfer is only available in the Enterprise version");
    }
}

