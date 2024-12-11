#pragma once

#include "dq_input_channel.h"
#include "dq_columns_resolve.h"

namespace NYql::NDq {

struct TDqMeteringStats {
    struct TInputStats {
        ui64 RowsConsumed = 0;
        ui64 BytesConsumed = 0;
    };

    struct TInputStatsMeter {
        void Add(const NKikimr::NUdf::TUnboxedValue&);
        void Add(const NKikimr::NUdf::TUnboxedValue* row, ui32 width);
        operator bool() { return Stats; }

        TInputStats* Stats = nullptr;
        NKikimr::NMiniKQL::TType* InputType = nullptr;
    };

    std::vector<std::unique_ptr<TInputStats>> Inputs;

    TInputStats& AddInputs() {
        Inputs.push_back(std::make_unique<TInputStats>());
        return *Inputs.back();
    }
};

NKikimr::NUdf::TUnboxedValue CreateInputUnionValue(const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, TDqMeteringStats::TInputStatsMeter = {});

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory,
    TDqMeteringStats::TInputStatsMeter = {});

} // namespace NYql::NDq
