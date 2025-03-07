#pragma once

#include <ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::inline Dev::NTable {

class TTransaction;

}

namespace NYdb::inline Dev::NTopic {

struct TTransactionId {
    std::string SessionId;
    std::string TxId;
};

inline
bool operator==(const TTransactionId& lhs, const TTransactionId& rhs)
{
    return (lhs.SessionId == rhs.SessionId) && (lhs.TxId == rhs.TxId);
}

inline
bool operator!=(const TTransactionId& lhs, const TTransactionId& rhs)
{
    return !(lhs == rhs);
}

TTransactionId MakeTransactionId(const NTable::TTransaction& tx);

TStatus MakeSessionExpiredError();
TStatus MakeCommitTransactionSuccess();

}

template <>
struct THash<NYdb::NTopic::TTransactionId> {
    size_t operator()(const NYdb::NTopic::TTransactionId& v) const noexcept {
        return CombineHashes(THash<std::string>()(v.SessionId), THash<std::string>()(v.TxId));
    }
};
