#pragma once
#include "behaviour.h"
#include "object.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/operations/abstract/update.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataUpdateBase: public ISSEntityUpdate {
protected:
    IMetadataUpdateBehaviour::TPtr Behaviour;

private:
    NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override {
        if (auto status = DoInitializeImpl(context); status.IsFail()) {
            return status;
        }
        AFL_VERIFY(Behaviour);
        return TConclusionStatus::Success();
    }

    TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& /*seqNo*/) const override {
        Y_ABORT();
    }

    std::set<ui64> DoGetShardIds() const override {
        return {};
    }

protected:
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) = 0;

    static void PersistObject(const TPathId& pathId, const TMetadataObjectInfo::TPtr& object, const TUpdateStartContext& context);
};

class TMetadataUpdateCreate: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateCreate, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;

private:
    TMetadataObjectInfo::TPtr Result;

private:
    TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoStart(const TUpdateStartContext& context) override;
    TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }

public:
    virtual TPathElement::EPathType GetObjectPathType() const = 0;
    virtual TString GetStorageDirectory() const = 0;
    virtual std::shared_ptr<TMetadataEntity> MakeEntity(const TPathId& pathId) const = 0;
};

class TMetadataUpdateAlter: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateAlter, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;

private:
    TMetadataObjectInfo::TPtr Result;

private:
    TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoStart(const TUpdateStartContext& context) override;
    TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }
};

class TMetadataUpdateDrop: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateDrop, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;

private:
    TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoStart(const TUpdateStartContext& context) override;
    TConclusionStatus DoFinish(const TUpdateFinishContext& context) override;

public:
    static NKikimrSchemeOp::TModifyScheme RestoreRequest(const TPath& path);
};

}   // namespace NKikimr::NSchemeShard::NOperations
