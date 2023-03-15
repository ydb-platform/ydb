#pragma once

#include "dq_input_channel.h"
#include "dq_columns_resolve.h"

namespace NYql::NDq {

struct TDqBillingStats {
    struct TInputStats {
        ui64 RowsConsumed;
    };
    std::vector<std::unique_ptr<TInputStats>> Inputs;

    TInputStats& AddInputs() {
        Inputs.push_back(std::make_unique<TInputStats>());
        return *Inputs.back();
    }
};

NKikimr::NUdf::TUnboxedValue CreateInputUnionValue(TVector<IDqInput::TPtr>&& inputs,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, TDqBillingStats::TInputStats*);

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory, TDqBillingStats::TInputStats*);

} // namespace NYql::NDq
