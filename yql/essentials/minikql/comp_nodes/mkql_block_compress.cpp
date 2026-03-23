#include "mkql_block_compress.h"
#include "mkql_counters.h"

#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/arrow/mkql_bit_utils.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

size_t GetBitmapPopCount(const std::shared_ptr<arrow::ArrayData>& arr) {
    size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return GetSparseBitmapPopCount(src, len);
}

struct TCompressBlocksState: public TBlockState {
    size_t InputSize_ = 0;
    size_t OutputPos_ = 0;
    bool IsFinished_ = false;

    const size_t MaxLength_;

    std::vector<std::shared_ptr<arrow::ArrayData>> Arrays_;
    std::vector<std::unique_ptr<IArrayBuilder>> Builders_;

    NYql::NUdf::TCounter CounterOutputRows_;

    TCompressBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TBlockType*>& types)
        : TBlockState(memInfo, types.size() + 1U)
        , MaxLength_(CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL, [](size_t max, const TBlockType* type) { return std::max(max, CalcMaxBlockItemSize(type->GetItemType())); })))
        , Arrays_(types.size() + 1U)
        , Builders_(types.size())
    {
        for (ui32 i = 0; i < types.size(); ++i) {
            if (types[i]->GetShape() != TBlockType::EShape::Scalar) {
                Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), types[i]->GetItemType(), ctx.ArrowMemoryPool, MaxLength_, &ctx.Builder->GetPgBuilder());
            }
        }
        if (ctx.CountersProvider) {
            // id will be assigned externally in future versions
            TString id = TString(Operator_Filter) + "0";
            CounterOutputRows_ = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, false);
        }
    }

    enum class EStep: i8 {
        Copy = -1,
        Skip = 0,
        Pass = 1
    };

    EStep Check(const NUdf::TUnboxedValuePod bitmapValue) {
        Y_ABORT_UNLESS(!IsFinished_);
        Y_ABORT_UNLESS(!InputSize_);
        auto& bitmap = Arrays_.back();
        bitmap = TArrowBlock::From(bitmapValue).GetDatum().array();

        if (!bitmap->length) {
            return EStep::Skip;
        }

        const auto popCount = GetBitmapPopCount(bitmap);

        CounterOutputRows_.Add(popCount);

        if (!popCount) {
            return EStep::Skip;
        }

        if (!OutputPos_ && ui64(bitmap->length) == popCount) {
            return EStep::Copy;
        }

        return EStep::Pass;
    }

    bool Sparse() {
        auto& bitmap = Arrays_.back();
        if (!InputSize_) {
            InputSize_ = bitmap->length;
            for (size_t i = 0; i < Builders_.size(); ++i) {
                if (Builders_[i]) {
                    Arrays_[i] = TArrowBlock::From(Values[i]).GetDatum().array();
                    Y_ABORT_UNLESS(ui64(Arrays_[i]->length) == InputSize_);
                }
            }
        }

        size_t outputAvail = MaxLength_ - OutputPos_;
        size_t takeInputLen = 0;
        size_t takeInputPopcnt = 0;

        const auto bitmapData = bitmap->GetValues<ui8>(1);
        while (takeInputPopcnt < outputAvail && takeInputLen < InputSize_) {
            takeInputPopcnt += bitmapData[takeInputLen++];
        }
        Y_ABORT_UNLESS(takeInputLen > 0);
        for (size_t i = 0; i < Builders_.size(); ++i) {
            if (Builders_[i]) {
                auto& arr = Arrays_[i];
                auto& builder = Builders_[i];
                auto slice = Chop(arr, takeInputLen);
                builder->AddMany(*slice, takeInputPopcnt, bitmapData, takeInputLen);
            }
        }

        Chop(bitmap, takeInputLen);
        OutputPos_ += takeInputPopcnt;
        InputSize_ -= takeInputLen;
        return MaxLength_ > OutputPos_;
    }

    void FlushBuffers(const THolderFactory& holderFactory) {
        for (ui32 i = 0; i < Builders_.size(); ++i) {
            if (Builders_[i]) {
                Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(IsFinished_));
            }
        }

        Values.back() = MakeBlockCount(holderFactory, OutputPos_);
        OutputPos_ = 0;
        FillArrays();
    }
};

enum class ECompressType {
    BitmapIsScalar,
    AllScalars,
    Blocks
};

template <ECompressType CompressType>
class TCompressWrapper: public TMutableComputationNode<TCompressWrapper<CompressType>> {
    using TBaseComputation = TMutableComputationNode<TCompressWrapper<CompressType>>;

public:
    TCompressWrapper(TComputationMutables& mutables, IComputationNode* stream, ui32 bitmapIndex, ui32 inputWidth, TVector<TBlockType*>&& types)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , BitmapIndex_(bitmapIndex)
        , InputWidth_(inputWidth)
        , Types_(std::move(types))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Types_.size() + 2U))
    {
    }

    NYql::NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        NYql::NUdf::TUnboxedValue state;
        if constexpr (CompressType == ECompressType::Blocks) {
            state = ctx.HolderFactory.Create<TCompressBlocksState>(ctx, Types_);
        } else {
            state = NYql::NUdf::TUnboxedValuePod();
        }
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(Stream_->GetValue(ctx)),
                                                      std::move(state),
                                                      BitmapIndex_,
                                                      Types_,
                                                      InputWidth_);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, NYql::NUdf::TUnboxedValue stream, NYql::NUdf::TUnboxedValue state, ui32 bitmapIndex, const TVector<TBlockType*>& types, ui32 inputWidth)
            : TBase(memInfo)
            , HolderFactory_(holderFactory)
            , Stream_(std::move(stream))
            , State_(std::move(state))
            , BitmapIndex_(bitmapIndex)
            , Types_(types)
            , Input_(inputWidth, NYql::NUdf::TUnboxedValuePod())
        {
        }

        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
            if constexpr (CompressType == ECompressType::BitmapIsScalar) {
                return BitmapIsScalar(output, width);
            } else if constexpr (CompressType == ECompressType::AllScalars) {
                return AllScalars(output, width);
            } else if constexpr (CompressType == ECompressType::Blocks) {
                return Blocks(output, width);
            } else {
                static_assert(0, "Unhandled case");
            }
        }

    private:
        void MoveAllExceptBitmap(NYql::NUdf::TUnboxedValue* output) {
            for (ui32 i = 0, outIndex = 0; i < Input_.size(); ++i) {
                if (i == BitmapIndex_) {
                    continue;
                }
                output[outIndex++] = std::move(Input_[i]);
            }
        }

        NUdf::EFetchStatus BitmapIsScalar(NUdf::TUnboxedValue* output, ui32 width) {
            MKQL_ENSURE(width == OutputWidth(), "Width must be the same as output width.");
            if (State_.IsFinish()) {
                return NUdf::EFetchStatus::Finish;
            }

            if (const auto result = Stream_.WideFetch(Input_.data(), Input_.size()); NUdf::EFetchStatus::Ok != result) {
                return result;
            }
            MoveAllExceptBitmap(output);

            auto& bitmap = Input_[BitmapIndex_];
            const bool bitmapValue = GetBitmapScalarValue(bitmap) & 1;
            State_ = bitmapValue ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod::MakeFinish();

            return bitmapValue ? NUdf::EFetchStatus::Ok : NUdf::EFetchStatus::Finish;
        }

        NUdf::EFetchStatus AllScalars(NUdf::TUnboxedValue* output, ui32 width) {
            MKQL_ENSURE(width == OutputWidth(), "Width must be the same as output width.");

            auto& bitmap = Input_[BitmapIndex_];
            for (;;) {
                if (const auto result = Stream_.WideFetch(Input_.data(), Input_.size()); NUdf::EFetchStatus::Ok != result) {
                    return result;
                }
                MoveAllExceptBitmap(output);

                if (const auto popCount = GetBitmapPopCountCount(bitmap)) {
                    output[Input_.size() - 2] = HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(popCount)));
                    break;
                }
            }
            return NUdf::EFetchStatus::Ok;
        }

        NUdf::EFetchStatus Blocks(NUdf::TUnboxedValue* output, ui32 width) {
            MKQL_ENSURE(width == OutputWidth(), "Width must be the same as output width.");
            Y_DEBUG_ABORT_UNLESS(OutputWidth() == Input_.size() - 1);
            Y_DEBUG_ABORT_UNLESS(OutputWidth() == Types_.size() + 1);

            auto& state = *static_cast<TCompressBlocksState*>(State_.AsBoxed().Get());
            Y_DEBUG_ABORT_UNLESS(state.Values.size() == OutputWidth());

            auto& bitmap = Input_[BitmapIndex_];

            if (!state.Count) {
                do {
                    if (!state.InputSize_) {
                        state.ClearValues();
                        auto wideFetchResult = Stream_.WideFetch(Input_.data(), Input_.size());
                        MoveAllExceptBitmap(state.Values.data());

                        switch (wideFetchResult) {
                            case NUdf::EFetchStatus::Yield:
                                return NUdf::EFetchStatus::Yield;
                            case NUdf::EFetchStatus::Finish:
                                state.IsFinished_ = true;
                                break;
                            case NUdf::EFetchStatus::Ok:
                                switch (state.Check(bitmap)) {
                                    case TCompressBlocksState::EStep::Copy:
                                        for (ui32 i = 0; i < state.Values.size(); ++i) {
                                            output[i] = state.Values[i];
                                        }
                                        return NUdf::EFetchStatus::Ok;
                                    case TCompressBlocksState::EStep::Skip:
                                        continue;
                                    case TCompressBlocksState::EStep::Pass:
                                        break;
                                }
                                break;
                        }
                    }
                } while (!state.IsFinished_ && state.Sparse());

                if (state.OutputPos_) {
                    state.FlushBuffers(HolderFactory_);
                } else {
                    return NUdf::EFetchStatus::Finish;
                }
            }

            const auto sliceSize = state.Slice();
            for (size_t i = 0; i < OutputWidth(); ++i) {
                output[i] = state.Get(sliceSize, HolderFactory_, i);
            }

            return NUdf::EFetchStatus::Ok;
        }

        ui32 OutputWidth() {
            return Input_.size() - 1;
        }

        const THolderFactory& HolderFactory_;
        NYql::NUdf::TUnboxedValue Stream_;
        NUdf::TUnboxedValue State_;
        ui32 BitmapIndex_;
        const TVector<TBlockType*>& Types_;
        TUnboxedValueVector Input_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const ui32 BitmapIndex_;
    const ui32 InputWidth_;
    const TVector<TBlockType*> Types_;
    const size_t WideFieldsIndex_;
};

} // namespace

IComputationNode* WrapBlockCompress(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args, got " << callable.GetInputsCount());

    const auto streamOrFlowType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamOrFlowType->IsStream(), "Expected stream type.");

    const auto wideComponents = GetWideComponents(streamOrFlowType);
    const ui32 inputWidth = wideComponents.size();
    MKQL_ENSURE(inputWidth > 1, "Expected at least two columns");

    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto bitmapIndex = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(bitmapIndex < inputWidth - 1, "Bad bitmap index");

    TVector<TBlockType*> types;
    types.reserve(inputWidth - 2U);
    bool bitmapIsScalar = false;
    bool allScalars = true;
    for (ui32 i = 0; i < inputWidth; ++i) {
        types.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        const bool isScalar = types.back()->GetShape() == TBlockType::EShape::Scalar;
        if (i == inputWidth - 1) {
            MKQL_ENSURE(isScalar, "Expecting scalar block size as last column");
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Uint64, "Expecting Uint64 as last column");
            types.pop_back();
        } else if (i == bitmapIndex) {
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Bool, "Expecting Bool as bitmap column");
            bitmapIsScalar = isScalar;
            types.pop_back();
        } else {
            allScalars = allScalars && isScalar;
        }
    }

    const auto compressArg = LocateNode(ctx.NodeLocator, callable, 0);
    if (bitmapIsScalar) {
        return new TCompressWrapper<ECompressType::BitmapIsScalar>(ctx.Mutables, compressArg, bitmapIndex, inputWidth, std::move(types));
    } else if (allScalars) {
        return new TCompressWrapper<ECompressType::AllScalars>(ctx.Mutables, compressArg, bitmapIndex, inputWidth, std::move(types));
    }
    return new TCompressWrapper<ECompressType::Blocks>(ctx.Mutables, compressArg, bitmapIndex, inputWidth, std::move(types));
}

} // namespace NMiniKQL
} // namespace NKikimr
