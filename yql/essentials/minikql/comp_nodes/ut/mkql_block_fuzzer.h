#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>

#include <util/random/random.h>
#include <util/generic/ptr.h>

// This file defines a fuzzing framework for MiniKQL blocks. It provides interfaces and implementations
// for controlled mutation of block data structures to test edge cases and ensure robustness.
// The framework supports various fuzzing strategies like removing optional bitmasks and shifting offsets.
namespace NKikimr::NMiniKQL {

struct TFuzzOptions {
    bool FuzzZeroOptionalBitmaskRemove = false;
    bool FuzzOffsetShift = false;

    static TFuzzOptions FuzzAll() {
        return TFuzzOptions{.FuzzZeroOptionalBitmaskRemove = true, .FuzzOffsetShift = true};
    }
};

class IFuzzer {
public:
    virtual ~IFuzzer() = default;

    virtual NYql::NUdf::TUnboxedValue Fuzz(NYql::NUdf::TUnboxedValue input,
                                           const THolderFactory& holderFactory,
                                           arrow::MemoryPool& memoryPool,
                                           IRandomProvider& randomProvider) const = 0;
};

using TFuzzerList = TVector<THolder<IFuzzer>>;

class TFuzzerHolder {
public:
    static constexpr ui64 EmptyFuzzerId = 0;

    TFuzzerHolder();
    ~TFuzzerHolder();

    ui64 ReserveFuzzer();

    void CreateFuzzers(TFuzzOptions options, ui64 fuzzerIndex, const TType* type, const TTypeEnvironment& env);

    void ClearFuzzers();

    NYql::NUdf::TUnboxedValue ApplyFuzzers(NYql::NUdf::TUnboxedValue input,
                                           ui64 fuzzIdx,
                                           const THolderFactory& holderFactory,
                                           arrow::MemoryPool& memoryPool,
                                           IRandomProvider& randomProvider) const;

private:
    std::map<ui64, TFuzzerList> NodeToFuzzOptions_;
    ui64 FuzzerIdx_ = 1;
};

} // namespace NKikimr::NMiniKQL
