#pragma once

#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
}

namespace NKikimr::NOlap::NEngineLoading {

class IEngineTxReader: public ITxReader {
private:
    using TBase = ITxReader;

protected:
    const std::shared_ptr<IBlobGroupSelector> DsGroupSelector;
    TColumnEngineForLogs* Self = nullptr;

public:
    IEngineTxReader(const TString& name, TColumnEngineForLogs* self, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector)
        : TBase(name)
        , DsGroupSelector(dsGroupSelector)
        , Self(self) {
    }
};

class TEngineCountersReader: public IEngineTxReader {
private:
    using TBase = IEngineTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TEngineShardingInfoReader: public IEngineTxReader {
private:
    using TBase = IEngineTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TPortionsLoadContext {
private:
    TPortionConstructors Constructors;

public:
    TPortionConstructors& MutableConstructors() {
        return Constructors;
    }
    const TPortionConstructors& GetConstructors() const {
        return Constructors;
    }
};

class TEnginePortionsReader: public IEngineTxReader {
private:
    using TBase = IEngineTxReader;
    const std::shared_ptr<TPortionsLoadContext> Context;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    TEnginePortionsReader(const TString& name, TColumnEngineForLogs* self, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector,
        const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name, self, dsGroupSelector)
        , Context(context) {
    }
};

class TEngineColumnsReader: public IEngineTxReader {
private:
    using TBase = IEngineTxReader;
    const std::shared_ptr<TPortionsLoadContext> Context;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    TEngineColumnsReader(const TString& name, TColumnEngineForLogs* self, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector,
        const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name, self, dsGroupSelector)
        , Context(context) {
    }
};

class TEngineIndexesReader: public IEngineTxReader {
private:
    using TBase = IEngineTxReader;
    const std::shared_ptr<TPortionsLoadContext> Context;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    TEngineIndexesReader(const TString& name, TColumnEngineForLogs* self, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector,
        const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name, self, dsGroupSelector)
        , Context(context) {
    }
};

class TEngineLoadingFinish: public ITxReader {
private:
    using TBase = ITxReader;
    TColumnEngineForLogs* Self = nullptr;
    const std::shared_ptr<TPortionsLoadContext> Context;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    TEngineLoadingFinish(const TString& name, TColumnEngineForLogs* self, const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name)
        , Self(self)
        , Context(context) {
    }
};

}   // namespace NKikimr::NOlap::NEngineLoading
