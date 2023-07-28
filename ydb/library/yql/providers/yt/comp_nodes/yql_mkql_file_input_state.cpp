#include "yql_mkql_file_input_state.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <util/system/fs.h>

namespace NYql {

TFileInputState::TFileInputState(const TMkqlIOSpecs& spec,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TVector<NYT::TRawTableReaderPtr>&& rawReaders,
    size_t blockCount,
    size_t blockSize)
    : Spec_(&spec)
    , HolderFactory_(holderFactory)
    , RawReaders_(std::move(rawReaders))
    , BlockCount_(blockCount)
    , BlockSize_(blockSize)
{
    YQL_ENSURE(Spec_->Inputs.size() == RawReaders_.size());
    MkqlReader_.SetSpecs(*Spec_, HolderFactory_);
    Valid_ = NextValue();
}

bool TFileInputState::NextValue() {
    for (;;) {
        if (CurrentInput_ >= RawReaders_.size()) {
            return false;
        }

        if (!MkqlReader_.IsValid()) {
            if (!RawReaders_[CurrentInput_]) {
                ++CurrentInput_;
                continue;
            }

            CurrentReader_ = std::move(RawReaders_[CurrentInput_]);
            if (CurrentInput_ > 0 && OnNextBlockCallback_) {
                OnNextBlockCallback_();
            }
            MkqlReader_.SetReader(*CurrentReader_, BlockCount_, BlockSize_, ui32(CurrentInput_), true);
            MkqlReader_.Next();
            CurrentRecord_ = 0;

            if (!MkqlReader_.IsValid()) {
                ++CurrentInput_;
                continue;
            }
        }

        CurrentValue_ = MkqlReader_.GetRow();
        if (!Spec_->InputGroups.empty()) {
            CurrentValue_ = HolderFactory_.CreateVariantHolder(CurrentValue_.Release(), Spec_->InputGroups.at(CurrentInput_));
        }

        MkqlReader_.Next();
        ++CurrentRecord_;
        return true;
    }
}

TVector<NYT::TRawTableReaderPtr> MakeMkqlFileInputs(const TVector<TString>& files, bool decompress) {
    TVector<NYT::TRawTableReaderPtr> rawReaders;
    for (auto& file: files) {
        if (!NFs::Exists(file)) {
            rawReaders.emplace_back(nullptr);
            continue;
        }
        rawReaders.emplace_back(MakeIntrusive<TMkqlInput>(MakeFileInput(file, decompress)));
    }
    return rawReaders;
}

} // NYql
