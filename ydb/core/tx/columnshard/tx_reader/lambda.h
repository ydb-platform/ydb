#pragma once
#include "abstract.h"

namespace NKikimr {

class TTxLambdaReader: public ITxReader {
private:
    using TBase = ITxReader;

public:
    using TFunction = std::function<bool(NTabletFlatExecutor::TTransactionContext&, const TActorContext&)>;

private:
    TFunction PrechargeFunction;
    TFunction ExecuteFunction;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        return PrechargeFunction(txc, ctx);
    }

    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        return ExecuteFunction(txc, ctx);
    }

public:
    TTxLambdaReader(const TString& name, const TFunction& precharge, const TFunction& execute)
        : TBase(name)
        , PrechargeFunction(precharge)
        , ExecuteFunction(execute) {
    }
};

}   // namespace NKikimr
