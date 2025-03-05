#include "transaction.h"
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb::inline V2::NTopic {

TTransactionId MakeTransactionId(const NTable::TTransaction& tx)
{
    return {tx.GetSession().GetId(), tx.GetId()};
}

TStatus MakeStatus(EStatus code, NYql::TIssues&& issues)
{
    return {code, std::move(issues)};
}

TStatus MakeSessionExpiredError()
{
    return MakeStatus(EStatus::SESSION_EXPIRED, {});
}

TStatus MakeCommitTransactionSuccess()
{
    return MakeStatus(EStatus::SUCCESS, {});
}

}
