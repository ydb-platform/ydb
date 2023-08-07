#pragma once

#include "yql_mkql_input_stream.h"

#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <optional>

namespace NYql {

class TFileListValueBase : public NKikimr::NMiniKQL::TCustomListValue {
public:
    TFileListValueBase(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo, const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory, std::optional<ui64> length)
        : TCustomListValue(memInfo)
        , Spec(spec)
        , HolderFactory(holderFactory)
    {
        Length = length;
    }

protected:
    class TIterator : public NKikimr::NMiniKQL::TComputationValue<TIterator> {
    public:
        TIterator(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo, THolder<IInputState>&& state, std::optional<ui64> length);

    private:
        bool Next(NUdf::TUnboxedValue& value) override;

        bool AtStart_ = true;
        THolder<IInputState> State_;
        std::optional<ui64> ExpectedLength_;
    };

    NUdf::TUnboxedValue GetListIterator() const override;

    virtual THolder<IInputState> MakeState() const = 0;

protected:
    const TMkqlIOSpecs& Spec;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
};

class TFileListValue : public TFileListValueBase {
public:
    TFileListValue(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
        const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TVector<TString>& filePaths,
        bool decompress,
        size_t blockCount,
        size_t blockSize,
        std::optional<ui64> length)
        : TFileListValueBase(memInfo, spec, holderFactory, length)
        , FilePaths(filePaths)
        , Decompress(decompress)
        , BlockCount(blockCount)
        , BlockSize(blockSize)
    {
    }

protected:
    THolder<IInputState> MakeState() const override;

private:
    const TVector<TString> FilePaths;
    const bool Decompress;
    const size_t BlockCount;
    const size_t BlockSize;
};

}
