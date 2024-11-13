#pragma once

#include "common.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

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
        IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
        const IPureCalcProgramFactory::TSettings& factorySettings);

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
    IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
    const IPureCalcProgramFactory::TSettings& factorySettings);

} // namespace NFq
