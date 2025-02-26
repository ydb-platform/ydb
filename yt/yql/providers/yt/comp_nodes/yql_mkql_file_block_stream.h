#pragma once

#include "yql_mkql_file_input_state.h"

#include <yt/yql/providers/yt/codec/yt_codec.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NYql {

class TFileWideBlockStreamValue : public NKikimr::NMiniKQL::TComputationValue<TFileWideBlockStreamValue> {
public:
    TFileWideBlockStreamValue(
        NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
        const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TVector<TString>& filePaths,
        bool decompress,
        size_t blockCount,
        size_t blockSize,
        std::optional<ui64> expectedRowCount
    );

private:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width);

private:
    const TMkqlIOSpecs& Spec_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    const TVector<TString> FilePaths_;
    const bool Decompress_;
    const size_t BlockCount_;
    const size_t BlockSize_;
    const std::optional<ui64> ExpectedRowCount_;

    bool AtStart_ = true;
    THolder<TFileInputState> State_;
};

}
