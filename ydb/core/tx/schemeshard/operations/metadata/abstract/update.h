#pragma once
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/operations/abstract/update.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard::NOperations {

class IDropMetadataUpdate {
public:
    class TRestoreContext {
    private:
        using TOperationContextPtr = TOperationContext*;
        using TPathPtr = const TPath*;
        YDB_READONLY_DEF(TPathPtr, ObjectPath);
        YDB_READONLY_DEF(TOperationContextPtr, SSOperationContext);

    public:
        TRestoreContext(const TPath* objectPath, TOperationContext* operationContext)
            : ObjectPath(objectPath)
            , SSOperationContext(operationContext) {
            AFL_VERIFY(objectPath);
            AFL_VERIFY(objectPath->IsResolved())("path", objectPath->PathString());
        }
    };

public:
    virtual void RestoreDrop(const TRestoreContext& context) = 0;
    virtual TConclusionStatus FinishDrop(const TUpdateFinishContext& context) = 0;
    ~IDropMetadataUpdate() = default;
};

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

    static std::shared_ptr<IDropMetadataUpdate> MakeDrop(const TPath& object);

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
    static std::shared_ptr<IDropMetadataUpdate> RestoreDrop(const IDropMetadataUpdate::TRestoreContext& context);

    virtual TPathElement::EPathType GetObjectPathType() const = 0;
};

}   // namespace NKikimr::NSchemeShard::NOperations
