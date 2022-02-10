#pragma once

#include "dq_input_channel.h"
#include "dq_columns_resolve.h"

namespace NYql::NDq {

NKikimr::NUdf::TUnboxedValue CreateInputUnionValue(TVector<IDqInput::TPtr>&& inputs, 
    const NKikimr::NMiniKQL::THolderFactory& holderFactory);

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory);

} // namespace NYql::NDq
