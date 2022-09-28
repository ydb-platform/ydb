#pragma once

#include "datashard_impl.h"
#include "operation.h"

namespace NKikimr::NDataShard {

class IReadOperation {
public:
    virtual ~IReadOperation() = default;

    // our interface for TReadUnit
    virtual EExecutionStatus Execute(TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual void Complete(const TActorContext& ctx) = 0;

    // our interface for TCheckReadUnit
    virtual void CheckRequestAndInit(TTransactionContext& txc, const TActorContext& ctx) = 0;
};

} // NKikimr::NDataShard
