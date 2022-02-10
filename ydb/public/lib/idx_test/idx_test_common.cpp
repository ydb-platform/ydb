#include "idx_test_common.h"

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace NIdxTest {

TMaybe<TTableDescription> DescribeTable(const TString& tableName, TTableClient client) {
    TMaybe<TTableDescription> res;
    auto describeFn = [&res, tableName](TSession session) mutable {
        auto describeResult = session.DescribeTable(
            tableName,
            TDescribeTableSettings()
                .WithKeyShardBoundary(true)
                .ClientTimeout(TDuration::Seconds(5))
            ).GetValueSync();
        if (!describeResult.IsSuccess())
            return describeResult;
        res = describeResult.GetTableDescription();
        return describeResult;
    };
    auto status = client.RetryOperationSync(describeFn);
    ThrowOnError(status);
    return res;
}

} // namespace NIdxTest
