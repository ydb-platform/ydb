#pragma once
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/operations/abstract/update.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
    YDB_READONLY_DEF(TString, PathString);

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    TConclusionStatus DoStart(const TUpdateStartContext& context) override {
        AFL_VERIFY(IsEqualPaths(PathString, context.GetObjectPath()->PathString()))("path", PathString)(
                                                "context", context.GetObjectPath()->PathString());
        AFL_VERIFY(context.GetObjectPath()->IsResolved());
        return DoExecute(context);
    }

    TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }

protected:
    virtual TConclusionStatus DoExecute(const TUpdateStartContext& context) = 0;

    TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& /*seqNo*/) const override {
        Y_ABORT();
    }

    std::set<ui64> DoGetShardIds() const override {
        return {};
    }

    TMetadataUpdate(const TString& objectPath)
        : PathString(objectPath) {
    }

public:
    static std::shared_ptr<TMetadataUpdate> MakeUpdate(const NKikimrSchemeOp::TModifyScheme& transaction);
    virtual std::shared_ptr<ISSEntity> MakeEntity(const TPathId& pathId) const = 0;

    virtual TPathElement::EPathType GetObjectPathType() const = 0;
};

}   // namespace NKikimr::NSchemeShard::NOperations
