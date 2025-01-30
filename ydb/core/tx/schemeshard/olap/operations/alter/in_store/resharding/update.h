#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreShardingUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
    NOlap::NBackground::TTxChainData TxChainData;
    virtual TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& /*seqNo*/) const override {
        AFL_VERIFY(false);
        return "";
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SHARING;
    }

    virtual TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    virtual TConclusionStatus DoStart(const TUpdateStartContext& context) override;

    virtual std::set<ui64> DoGetShardIds() const override {
        return {};
    }

public:
};

}