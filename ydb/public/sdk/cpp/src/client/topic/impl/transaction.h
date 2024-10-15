#pragma once

#include <ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::NTable {

class TTransaction;

}

namespace NYdb::NTopic {

using TTransactionId = std::pair<std::string, std::string>;

inline
const std::string& GetSessionId(const TTransactionId& x)
{
    return x.first;
}

inline
const std::string& GetTxId(const TTransactionId& x)
{
    return x.second;
}

TTransactionId MakeTransactionId(const NTable::TTransaction& tx);

TStatus MakeSessionExpiredError();
TStatus MakeCommitTransactionSuccess();

}
