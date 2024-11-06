#pragma once
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NLoading {

class TPortionsLoadContext {
private:
    TInGranuleConstructors Constructors;

public:
    TInGranuleConstructors& MutableConstructors() {
        return Constructors;
    }
    const TInGranuleConstructors& GetConstructors() const {
        return Constructors;
    }
};

class IGranuleTxReader: public ITxReader {
private:
    using TBase = ITxReader;

protected:
    const std::shared_ptr<IBlobGroupSelector> DsGroupSelector;
    TGranuleMeta* Self = nullptr;
    const TVersionedIndex* VersionedIndex;
    const std::shared_ptr<TPortionsLoadContext> Context;

public:
    IGranuleTxReader(const TString& name, const TVersionedIndex* versionedIndex, TGranuleMeta* self, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector,
        const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name)
        , DsGroupSelector(dsGroupSelector)
        , Self(self)
        , VersionedIndex(versionedIndex)
        , Context(context) {
    }
};

class TGranulePortionsReader: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TGranuleColumnsReader: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TGranuleIndexesReader: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TGranuleFinishLoading: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NColumnShard::NLoading
