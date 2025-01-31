#include "yql_mkql_file_list.h"
#include "yql_mkql_file_input_state.h"

#include <yt/yql/providers/yt/lib/errors/yql_emergency_sleep.h>


namespace NYql {

using namespace NKikimr::NMiniKQL;

TFileListValueBase::TIterator::TIterator(TMemoryUsageInfo* memInfo, THolder<IInputState>&& state, std::optional<ui64> length)
    : TComputationValue(memInfo)
    , State_(std::move(state))
    , ExpectedLength_(std::move(length))
{
}

bool TFileListValueBase::TIterator::Next(NUdf::TUnboxedValue& value) {
    if (!AtStart_) {
        State_->Next();
    }
    AtStart_ = false;
    if (!State_->IsValid()) {
        YQL_EMERGENCY_SLEEP(!ExpectedLength_ || *ExpectedLength_ == 0, "Invalid file length, ExpectedLength=" << *ExpectedLength_ << ", State: " << State_->DebugInfo());
        return false;
    }

    if (ExpectedLength_) {
        YQL_EMERGENCY_SLEEP(*ExpectedLength_ > 0, "Invalid file length. State: " << State_->DebugInfo());
        --(*ExpectedLength_);
    }
    value = State_->GetCurrent();
    return true;
}

NUdf::TUnboxedValue TFileListValueBase::GetListIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), MakeState(), Length));
}

THolder<IInputState> TFileListValue::MakeState() const {
    return MakeHolder<TFileInputState>(Spec, HolderFactory, MakeMkqlFileInputs(FilePaths, Decompress), BlockCount, BlockSize);
}

}
