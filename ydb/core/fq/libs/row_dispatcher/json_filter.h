#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/public/purecalc/common/fwd.h>

namespace NFq {

class TJsonFilter {
public:
    using TCallback = std::function<void(ui64, const TString&)>;

public:
    TJsonFilter(
        const TVector<TString>& columns,
        const TVector<TString>& types,
        const TString& whereFilter,
        TCallback callback,
        NYql::NPureCalc::IProgramFactoryPtr pureCalcProgramFactory);

    ~TJsonFilter();

    void Push(const TVector<ui64>& offsets, const TVector<const NKikimr::NMiniKQL::TUnboxedValueVector*>& values);
    TString GetSql();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TJsonFilter::TCallback callback,
    NYql::NPureCalc::IProgramFactoryPtr pureCalcProgramFactory);

} // namespace NFq
