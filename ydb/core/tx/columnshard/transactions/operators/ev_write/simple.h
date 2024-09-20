#pragma once

#include "abstract.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TEvWriteSimpleCommitTransactionOperator: public TBaseEvWriteTransactionOperator,
                                               public TMonitoringObjectsCounter<TEvWriteSimpleCommitTransactionOperator> {
private:
    using TBase = TBaseEvWriteTransactionOperator;
    virtual bool DoParseImpl(TColumnShard& /*owner*/, const NKikimrTxColumnShard::TCommitWriteTxBody& /*commitTxBody*/) override {
        return true;
    }
    static inline auto Registrator = TFactory::TRegistrator<TEvWriteSimpleCommitTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE);

public:
    using TBase::TBase;
    virtual TString DoGetOpType() const override {
        return "EvWriteSimple";
    }
    virtual TString DoDebugString() const override {
        return "EV_WRITE_SIMPLE";
    }
};

}   // namespace NKikimr::NColumnShard
