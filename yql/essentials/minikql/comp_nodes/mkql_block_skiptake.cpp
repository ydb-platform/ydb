#include "mkql_block_skiptake.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

NUdf::TUnboxedValuePod SliceSkipBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
    const auto& datum = TArrowBlock::From(block).GetDatum();
    return datum.is_scalar() ? block : holderFactory.CreateArrowBlock(DeepSlice(*datum.array(), offset, datum.array()->length - offset), NYql::EDatumValidationMode::None);
}

NUdf::TUnboxedValuePod SliceTakeBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
    const auto& datum = TArrowBlock::From(block).GetDatum();
    return datum.is_scalar() ? block : holderFactory.CreateArrowBlock(DeepSlice(*datum.array(), 0ULL, offset), NYql::EDatumValidationMode::None);
}

template <bool Skip>
class TWideTakeSkipBlocksStreamWrapper: public TMutableComputationNode<TWideTakeSkipBlocksStreamWrapper<Skip>> {
    using TBaseComputation = TMutableComputationNode<TWideTakeSkipBlocksStreamWrapper<Skip>>;

public:
    TWideTakeSkipBlocksStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, IComputationNode* count, size_t width)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Stream_(stream)
        , Count_(count)
        , Width_(width)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(Stream_->GetValue(ctx)),
                                                      Width_,
                                                      Count_->GetValue(ctx).Get<ui64>(),
                                                      ctx.RuntimeSettings.DatumValidation.Get());
    }

private:
    class TStreamValue: public TBlockStreamValue<TStreamValue> {
        using TBase = TBlockStreamValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, NYql::NUdf::TUnboxedValue stream, size_t width, ui64 count, NYql::EDatumValidationMode validationMode)
            : TBase(memInfo, holderFactory, width)
            , HolderFactory_(holderFactory)
            , Stream_(std::move(stream))
            , Count_(count)
            , ValidationMode_(validationMode)
        {
        }

        NUdf::EFetchStatus DoWideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            if constexpr (Skip) {
                return WideFetchSkip(output, width);
            } else {
                return WideFetchTake(output, width);
            }
        }

        NUdf::EFetchStatus WideFetchTake(NUdf::TUnboxedValue* output, ui32 width) {
            if (Count_ == 0) {
                return NUdf::EFetchStatus::Finish;
            }

            if (const auto result = Stream_.WideFetch(output, width); NUdf::EFetchStatus::Ok == result) {
                if (const auto blockSize = GetBlockCount(output[width - 1]); Count_ < blockSize) {
                    output[width - 1] = MakeBlockCount(HolderFactory_, Count_, ValidationMode_);
                    for (auto i = 0U; i < width - 1; ++i) {
                        output[i] = SliceTakeBlock(HolderFactory_, output[i], Count_);
                    }
                    Count_ = 0;
                } else {
                    Count_ = Count_ - blockSize;
                }
                return NUdf::EFetchStatus::Ok;
            } else {
                return result;
            }
        }

        NUdf::EFetchStatus WideFetchSkip(NUdf::TUnboxedValue* output, ui32 width) {
            if (Count_ == 0) {
                return Stream_.WideFetch(output, width);
            }
            while (true) {
                if (const auto result = Stream_.WideFetch(output, width); NUdf::EFetchStatus::Ok != result) {
                    return result;
                }

                if (const auto blockSize = GetBlockCount(output[width - 1]); Count_ < blockSize) {
                    output[width - 1] = MakeBlockCount(HolderFactory_, blockSize - Count_, ValidationMode_);
                    for (auto i = 0U; i < width - 1; ++i) {
                        output[i] = SliceSkipBlock(HolderFactory_, output[i], Count_);
                    }
                    Count_ = 0;
                    return NUdf::EFetchStatus::Ok;
                } else {
                    Count_ -= blockSize;
                }
            }

            return Stream_.WideFetch(output, width);
        }

    private:
        const THolderFactory& HolderFactory_;
        NYql::NUdf::TUnboxedValue Stream_;
        ui64 Count_;
        const NYql::EDatumValidationMode ValidationMode_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Count_);
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    IComputationNode* const Count_;
    const size_t Width_;
};

IComputationNode* WrapSkipTake(bool skip, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream type.");
    const auto wideComponents = GetWideComponents(streamType);

    const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto input = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    if (skip) {
        return new TWideTakeSkipBlocksStreamWrapper<true>(ctx.Mutables, input, count, wideComponents.size());
    } else {
        return new TWideTakeSkipBlocksStreamWrapper<false>(ctx.Mutables, input, count, wideComponents.size());
    }
}

} // namespace

IComputationNode* WrapWideSkipBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapSkipTake(/*skip=*/true, callable, ctx);
}

IComputationNode* WrapWideTakeBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapSkipTake(/*skip=*/false, callable, ctx);
}

} // namespace NKikimr::NMiniKQL
