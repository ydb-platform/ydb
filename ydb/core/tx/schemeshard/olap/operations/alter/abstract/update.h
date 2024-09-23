#pragma once
#include "object.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityUpdate {
private:
    bool Initialized = false;
protected:
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) = 0;
    virtual TConclusionStatus DoStart(const TUpdateStartContext& context) = 0;
    virtual TConclusionStatus DoFinish(const TUpdateFinishContext& context) = 0;
    virtual TString DoGetShardTxBodyString(const ui64 tabletId, const TMessageSeqNo& seqNo) const = 0;
    virtual std::set<ui64> DoGetShardIds() const = 0;
public:
    ISSEntityUpdate() = default;
    virtual ~ISSEntityUpdate() = default;
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const = 0;

    std::set<ui64> GetShardIds() const {
        AFL_VERIFY(Initialized);
        return DoGetShardIds();
    }

    TString GetShardTxBodyString(const ui64 tabletId, const TMessageSeqNo& seqNo) const {
        AFL_VERIFY(Initialized);
        return DoGetShardTxBodyString(tabletId, seqNo);
    }

    TConclusionStatus Start(const TUpdateStartContext& context) {
        AFL_VERIFY(Initialized);
        return DoStart(context);
    }

    TConclusionStatus Finish(const TUpdateFinishContext& context) {
        AFL_VERIFY(Initialized);
        return DoFinish(context);
    }

    TConclusionStatus Initialize(const TUpdateInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        auto result = DoInitialize(context);
        if (result.IsSuccess()) {
            Initialized = true;
        }
        return result;
    }
};

}