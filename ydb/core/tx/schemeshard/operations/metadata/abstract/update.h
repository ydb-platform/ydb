#pragma once
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/operations/abstract/update.h>
#include <ydb/core/tx/schemeshard/operations/metadata/abstract/object.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataUpdateBase: public ISSEntityUpdate {
private:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

protected:
    TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& /*seqNo*/) const override {
        Y_ABORT();
    }

    std::set<ui64> DoGetShardIds() const override {
        return {};
    }

public:
};

class TMetadataUpdateCreate: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateCreate, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;

private:
    TConclusionStatus DoStart(const TUpdateStartContext& context) override {
        AFL_VERIFY(context.GetObjectPath()->IsResolved());
        return Execute(context);
    }

    TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }

protected:
    virtual TConclusionStatus Execute(const TUpdateStartContext& context) = 0;

public:
    virtual TPathElement::EPathType GetObjectPathType() const = 0;
    virtual TString GetStorageDirectory() const = 0;
    virtual std::shared_ptr<TMetadataEntity> MakeEntity(const TPathId& pathId) const = 0;
};

class TMetadataUpdateAlter: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateAlter, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;

private:
    TConclusionStatus DoStart(const TUpdateStartContext& context) override {
        AFL_VERIFY(context.GetObjectPath()->IsResolved());
        return Execute(context);
    }

    TConclusionStatus DoFinish(const TUpdateFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }

protected:
    virtual TConclusionStatus Execute(const TUpdateStartContext& context) = 0;
};

class TMetadataUpdateDrop: public TMetadataUpdateBase {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataUpdateDrop, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;
};

}   // namespace NKikimr::NSchemeShard::NOperations
