#include "yql_mkql_input_stream.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

////////////////////////////////////////////////////////////////////////////////////////////////

TInputStreamValue::TInputStreamValue(TMemoryUsageInfo* memInfo, IInputState* state)
    : TComputationValue<TInputStreamValue>(memInfo)
    , State_(state)
{
}

NUdf::IBoxedValuePtr TInputStreamValue::ToIndexDictImpl(const NUdf::IValueBuilder& builder) const {
    Y_UNUSED(builder);
    YQL_ENSURE(false, "Single-pass iterator cannot be used for index dict");
    return {};
}

NUdf::EFetchStatus TInputStreamValue::Fetch(NUdf::TUnboxedValue& result) {
    if (!AtStart_) {
        State_->Next();
    }
    AtStart_ = false;

    if (!State_->IsValid()) {
        return NUdf::EFetchStatus::Finish;
    }

    result = State_->GetCurrent();
    return NUdf::EFetchStatus::Ok;
}

} // NYql
