#pragma once

#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>

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

class TEngineLoadingFinish: public ITxReader {
private:
    using TBase = ITxReader;
    TColumnEngineForLogs* Self = nullptr;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    TEngineLoadingFinish(const TString& name, TColumnEngineForLogs* self)
        : TBase(name)
        , Self(self) {
    }
};

}   // namespace NKikimr::NOlap::NEngineLoading
