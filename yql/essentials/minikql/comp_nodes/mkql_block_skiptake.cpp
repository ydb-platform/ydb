#include "mkql_block_skiptake.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

NUdf::TUnboxedValuePod SliceSkipBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
    const auto& datum = TArrowBlock::From(block).GetDatum();
    return datum.is_scalar() ? block : holderFactory.CreateArrowBlock(DeepSlice(datum.array(), offset, datum.array()->length - offset));
}

NUdf::TUnboxedValuePod SliceTakeBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
    const auto& datum = TArrowBlock::From(block).GetDatum();
    return datum.is_scalar() ? block : holderFactory.CreateArrowBlock(DeepSlice(datum.array(), 0ULL, offset));
}

template <bool Skip>
class TWideTakeSkipBlocksStreamWrapper: public TMutableComputationNode<TWideTakeSkipBlocksStreamWrapper<Skip>> {
    using TBaseComputation = TMutableComputationNode<TWideTakeSkipBlocksStreamWrapper<Skip>>;

public:
    TWideTakeSkipBlocksStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, IComputationNode* count)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Stream(stream)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(Stream->GetValue(ctx)),
                                                      Count->GetValue(ctx).Get<ui64>());
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, NYql::NUdf::TUnboxedValue stream, ui64 count)
            : TBase(memInfo)
            , HolderFactory(holderFactory)
            , Stream(std::move(stream))
            , Count(count)
        {
        }

        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            if constexpr (Skip) {
                return WideFetchSkip(output, width);
            } else {
                return WideFetchTake(output, width);
            }
        }

        NUdf::EFetchStatus WideFetchTake(NUdf::TUnboxedValue* output, ui32 width) {
            if (Count == 0) {
                return NUdf::EFetchStatus::Finish;
            }

            if (const auto result = Stream.WideFetch(output, width); NUdf::EFetchStatus::Ok == result) {
                if (const auto blockSize = GetBlockCount(output[width - 1]); Count < blockSize) {
                    output[width - 1] = MakeBlockCount(HolderFactory, Count);
                    for (auto i = 0U; i < width - 1; ++i) {
                        output[i] = SliceTakeBlock(HolderFactory, output[i], Count);
                    }
                    Count = 0;
                } else {
                    Count = Count - blockSize;
                }
                return NUdf::EFetchStatus::Ok;
            } else {
                return result;
            }
        }

        NUdf::EFetchStatus WideFetchSkip(NUdf::TUnboxedValue* output, ui32 width) {
            if (Count == 0) {
                return Stream.WideFetch(output, width);
            }
            while (true) {
                if (const auto result = Stream.WideFetch(output, width); NUdf::EFetchStatus::Ok != result) {
                    return result;
                }

                if (const auto blockSize = GetBlockCount(output[width - 1]); Count < blockSize) {
                    output[width - 1] = MakeBlockCount(HolderFactory, blockSize - Count);
                    for (auto i = 0U; i < width - 1; ++i) {
                        output[i] = SliceSkipBlock(HolderFactory, output[i], Count);
                    }
                    Count = 0;
                    return NUdf::EFetchStatus::Ok;
                } else {
                    Count -= blockSize;
                }
            }

            return Stream.WideFetch(output, width);
        }

    private:
        const THolderFactory& HolderFactory;
        NYql::NUdf::TUnboxedValue Stream;
        ui64 Count;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Count);
        this->DependsOn(Stream);
    }

    IComputationNode* const Stream;
    IComputationNode* const Count;
};

IComputationNode* WrapSkipTake(bool skip, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream type.");

    const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto input = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    if (skip) {
        return new TWideTakeSkipBlocksStreamWrapper<true>(ctx.Mutables, input, count);
    } else {
        return new TWideTakeSkipBlocksStreamWrapper<false>(ctx.Mutables, input, count);
    }
}

} // namespace

IComputationNode* WrapWideSkipBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapSkipTake(true, callable, ctx);
}

IComputationNode* WrapWideTakeBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapSkipTake(false, callable, ctx);
}

} // namespace NMiniKQL
} // namespace NKikimr
