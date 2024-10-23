#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/string.h>

namespace NYdb::NTable {

class TTransaction;

}

namespace NYdb::NTopic {

using TTransactionId = std::pair<TString, TString>;

inline
const TString& GetSessionId(const TTransactionId& x)
{
    return x.first;
}

inline
const TString& GetTxId(const TTransactionId& x)
{
    return x.second;
}

TTransactionId MakeTransactionId(const NTable::TTransaction& tx);

TStatus MakeSessionExpiredError();
TStatus MakeCommitTransactionSuccess();

}
