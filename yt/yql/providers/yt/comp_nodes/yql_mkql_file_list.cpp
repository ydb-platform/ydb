#include "yql_mkql_file_list.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

TFileListValueBase::TIterator::TIterator(TMemoryUsageInfo* memInfo, const TMkqlIOSpecs& spec, THolder<TFileInputState>&& state, std::optional<ui64> length)
    : TComputationValue(memInfo)
    , State_(std::move(state))
    , ExpectedLength_(std::move(length))
    , Spec_(spec)
{
}

bool TFileListValueBase::TIterator::Next(NUdf::TUnboxedValue& value) {
    if (!AtStart_) {
        State_->Next();
    }
    AtStart_ = false;
    if (!State_->IsValid()) {
        MKQL_ENSURE(!ExpectedLength_ || *ExpectedLength_ == 0, "Invalid file length, ExpectedLength=" << *ExpectedLength_ << ", State: " << State_->DebugInfo());
        return false;
    }

    value = State_->GetCurrent();
    if (ExpectedLength_) {
        MKQL_ENSURE(*ExpectedLength_ > 0, "Invalid file length. State: " << State_->DebugInfo());
        if (Spec_.UseBlockInput_) {
            auto blockSizeStructIndex = GetBlockSizeStructIndex(Spec_, State_->GetTableIndex());
            auto blockCountValue = value.GetElement(blockSizeStructIndex);
            (*ExpectedLength_) -= GetBlockCount(blockCountValue);
        } else {
            --(*ExpectedLength_);
        }
    }
    return true;
}

NUdf::TUnboxedValue TFileListValueBase::GetListIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), Spec, MakeState(), Length));
}

THolder<TFileInputState> TFileListValue::MakeState() const {
    return MakeHolder<TFileInputState>(Spec, HolderFactory, MakeMkqlFileInputs(FilePaths, Decompress), BlockCount, BlockSize);
}

}
