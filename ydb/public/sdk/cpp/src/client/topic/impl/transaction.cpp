#include "transaction.h"
#include <ydb-cpp-sdk/client/table/table.h>

namespace NYdb::inline Dev::NTopic {

TTransactionId MakeTransactionId(const NTable::TTransaction& tx)
{
    return {tx.GetSession().GetId(), tx.GetId()};
}

TStatus MakeStatus(EStatus code, NYdb::NIssue::TIssues&& issues)
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
