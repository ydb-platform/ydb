#include "yql_mkql_file_block_stream.h"
#include "yql_mkql_file_input_state.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TFileWideBlockStreamValue::TFileWideBlockStreamValue(
    TMemoryUsageInfo* memInfo,
    const TMkqlIOSpecs& spec,
    const THolderFactory& holderFactory,
    const TVector<TString>& filePaths,
    bool decompress,
    size_t blockCount,
    size_t blockSize,
    std::optional<ui64> expectedRowCount
)
    : TComputationValue(memInfo)
    , Spec_(spec)
    , HolderFactory_(holderFactory)
    , FilePaths_(filePaths)
    , Decompress_(decompress)
    , BlockCount_(blockCount)
    , BlockSize_(blockSize)
    , ExpectedRowCount_(expectedRowCount)
{
    State_ = MakeHolder<TFileInputState>(Spec_, HolderFactory_, MakeMkqlFileInputs(FilePaths_, Decompress_), BlockCount_, BlockSize_);
}

NUdf::EFetchStatus TFileWideBlockStreamValue::WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
    if (!AtStart_) {
        State_->Next();
    }
    AtStart_ = false;
    if (!State_->IsValid()) {
        MKQL_ENSURE(!ExpectedRowCount_ || *ExpectedRowCount_ == State_->GetRecordIndex(), "Invalid file row count");
        return NUdf::EFetchStatus::Finish;
    }

    auto elements = State_->GetCurrent().GetElements();
    for (ui32 i = 0; i < width; i++) {
        if (auto out = output++) {
            *out = elements[i];
        }
    }

    return NUdf::EFetchStatus::Ok;
}

}
