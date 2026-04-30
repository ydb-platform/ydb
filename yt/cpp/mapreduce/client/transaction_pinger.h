#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/http/requests.h>

#include <util/generic/ptr.h>
#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPingableTransaction;

// Don't want to include public.h to avoid polluting header with TIntrusivePtr
template <typename>
class TFuture;

////////////////////////////////////////////////////////////////////////////////

// Each registered transaction must be removed from pinger
// (using RemoveTransaction) before it is destroyed
class ITransactionPinger
    : public TThrRefBase
{
public:
    virtual ~ITransactionPinger() = default;

    virtual ITransactionPingerPtr GetChildTxPinger() = 0;

    virtual void RegisterTransaction(const TPingableTransaction& pingableTx) = 0;

    virtual bool HasTransaction(const TPingableTransaction& pingableTx) = 0;

    virtual void RemoveTransaction(const TPingableTransaction& pingableTx) = 0;

    virtual TFuture<void> AsyncAbortTransaction(const TTransactionId& transactionId) = 0;
};

ITransactionPingerPtr CreateTransactionPinger(const TConfigPtr& config, IRawClientPtr rawClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
