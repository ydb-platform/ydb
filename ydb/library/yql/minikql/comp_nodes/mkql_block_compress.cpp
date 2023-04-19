#include "mkql_block_compress.h"
#include "mkql_block_builder.h"
#include "mkql_block_impl.h"

#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCompressWithScalarBitmap : public TStatefulWideFlowBlockComputationNode<TCompressWithScalarBitmap> {
public:
    TCompressWithScalarBitmap(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, width - 1)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Width_(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.SkipAll_.Defined() && *s.SkipAll_) {
            return EFetchResult::Finish;
        }

        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            if (i != BitmapIndex_) {
                s.ValuePointers_[i] = output[outIndex++];
            }
        }

        EFetchResult result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
        if (result == EFetchResult::One) {
            bool bitmapValue = TArrowBlock::From(s.Bitmap_).GetDatum().scalar_as<arrow::UInt8Scalar>().value & 1;
            if (!s.SkipAll_.Defined()) {
                s.SkipAll_ = !bitmapValue;
            } else {
                Y_VERIFY(bitmapValue != *s.SkipAll_);
            }
            if (*s.SkipAll_) {
                result = EFetchResult::Finish;
            }
        }
        return result;
    }
private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        NUdf::TUnboxedValue Bitmap_;
        TMaybe<bool> SkipAll_;

        TState(TMemoryUsageInfo* memInfo, ui32 width, ui32 bitmapIndex)
            : TComputationValue(memInfo)
            , ValuePointers_(width, nullptr)
        {
            ValuePointers_[bitmapIndex] = &Bitmap_;
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, BitmapIndex_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const ui32 BitmapIndex_;
    const ui32 Width_;
};

size_t GetBitmapPopCount(const std::shared_ptr<arrow::ArrayData>& arr) {
    size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return GetSparseBitmapPopCount(src, len);
}

class TCompressScalars : public TStatefulWideFlowBlockComputationNode<TCompressScalars> {
public:
    TCompressScalars(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, width - 1)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Width_(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);

        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            if (i != BitmapIndex_) {
                s.ValuePointers_[i] = output[outIndex++];
            }
        }

        EFetchResult result;
        for (;;) {
            result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result != EFetchResult::One) {
                break;
            }

            const ui64 popCount = GetBitmapPopCount(TArrowBlock::From(s.Bitmap_).GetDatum().array());
            if (popCount != 0) {
                MKQL_ENSURE(output[Width_ - 2], "Block size should not be marked as unused");
                *output[Width_ - 2] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(popCount)));
                break;
            }
        }
        return result;
    }
private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        NUdf::TUnboxedValue Bitmap_;

        TState(TMemoryUsageInfo* memInfo, ui32 width, ui32 bitmapIndex)
            : TComputationValue(memInfo)
            , ValuePointers_(width)
        {
            ValuePointers_[bitmapIndex] = &Bitmap_;
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, BitmapIndex_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const ui32 BitmapIndex_;
    const ui32 Width_;
};

class TCompressBlocks : public TStatefulWideFlowBlockComputationNode<TCompressBlocks> {
public:
    TCompressBlocks(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, TVector<TBlockType*>&& types)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, types.size() - 1)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx, output);

        EFetchResult result = EFetchResult::One;
        for (;;) {
            if (!s.InputSize_) {
                result = s.Finish_ ? EFetchResult::Finish : Flow_->FetchValues(ctx, s.ValuePointers_.data());
                if (result != EFetchResult::One) {
                    break;
                }
                auto bitmap = TArrowBlock::From(s.InputValues_[BitmapIndex_]).GetDatum().array();
                Y_VERIFY(bitmap);
                if (!bitmap->length) {
                    // skip empty input
                    continue;
                }

                // check if we can pass input as-is
                if (s.OutputPos_ == 0 || !s.HaveBlocks_) {
                    auto popCount = GetBitmapPopCount(bitmap);
                    if (!popCount) {
                        // all entries are filtered
                        continue;
                    }
                    bool copyAsIs = !s.HaveBlocks_ || popCount == bitmap->length;
                    if (copyAsIs) {
                        // client is not interested in any block columns or there is nothing to filter
                        s.OutputPos_ = popCount;
                        CopyAsIs(s, ctx, output);
                        break;
                    }
                }
                s.InputSize_ = (size_t)bitmap->length;
                for (size_t i = 0; i < Width_; ++i) {
                    if (s.Builders_[i] || i == BitmapIndex_) {
                        s.Arrays_[i] = TArrowBlock::From(s.InputValues_[i]).GetDatum().array();
                        Y_VERIFY(s.Arrays_[i]->length == s.InputSize_);
                    }
                }
            }

            Y_VERIFY(s.OutputPos_ <= s.OutputSize_);
            size_t outputAvail = s.OutputSize_ - s.OutputPos_;
            if (!outputAvail) {
                FlushBuffers(s, ctx, output);
                break;
            }

            size_t takeInputLen = 0;
            size_t takeInputPopcnt = 0;
            auto& bitmap = s.Arrays_[BitmapIndex_];
            Y_VERIFY(bitmap);
            const ui8* bitmapData = bitmap->GetValues<ui8>(1);
            while (takeInputPopcnt < outputAvail && takeInputLen < s.InputSize_) {
                takeInputPopcnt += bitmapData[takeInputLen++];
            }
            Y_VERIFY(takeInputLen > 0);
            for (size_t i = 0; i < Width_; ++i) {
                if (s.Builders_[i]) {
                    auto& arr = s.Arrays_[i];
                    auto& builder = s.Builders_[i];
                    auto slice = Chop(arr, takeInputLen);
                    builder->AddMany(*slice, takeInputPopcnt, bitmapData, takeInputLen);
                }
            }

            Chop(bitmap, takeInputLen);
            s.OutputPos_ += takeInputPopcnt;
            s.InputSize_ -= takeInputLen;
        }

        if (result == EFetchResult::Finish) {
            s.Finish_ = true;
            if (s.OutputPos_ > 0) {
                FlushBuffers(s, ctx, output);
                result = EFetchResult::One;
            }
        }
        return result;
    }
private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<NUdf::TUnboxedValue> InputValues_;
        TVector<std::shared_ptr<arrow::ArrayData>> Arrays_;
        TVector<std::unique_ptr<IArrayBuilder>> Builders_;
        size_t InputSize_ = 0;
        size_t OutputPos_ = 0;
        size_t OutputSize_ = 0;

        bool Finish_ = false;
        bool HaveBlocks_ = false;

        TState(TMemoryUsageInfo* memInfo, ui32 width, ui32 bitmapIndex, NUdf::TUnboxedValue*const* output,
            const TVector<TBlockType*>& types, arrow::MemoryPool& pool, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , ValuePointers_(width)
            , InputValues_(width)
            , Arrays_(width)
            , Builders_(width)
        {
            size_t maxBlockItemSize = 0;
            for (ui32 i = 0, outIndex = 0; i < width; ++i) {
                if (i != bitmapIndex) {
                    if (types[i]->GetShape() != TBlockType::EShape::Scalar && output[outIndex] != nullptr) {
                        HaveBlocks_ = true;
                        maxBlockItemSize = std::max(maxBlockItemSize, CalcMaxBlockItemSize(types[i]->GetItemType()));
                    }
                    if (output[outIndex] != nullptr) {
                        ValuePointers_[i] = &InputValues_[i];
                    }
                    outIndex++;
                } else {
                    ValuePointers_[i] = &InputValues_[i];
                }
            }
            const size_t maxBlockLen = CalcBlockLen(maxBlockItemSize);
            for (ui32 i = 0, outIndex = 0; i < width; ++i) {
                if (i != bitmapIndex) {
                    if (types[i]->GetShape() != TBlockType::EShape::Scalar && output[outIndex] != nullptr) {
                        Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), types[i]->GetItemType(), pool, maxBlockLen, &pgBuilder);
                    }
                    outIndex++;
                }
            }
            OutputSize_ = maxBlockLen;
        }
    };

    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, BitmapIndex_, output, Types_, ctx.ArrowMemoryPool, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    void WriteOutputBlockSize(size_t size, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        MKQL_ENSURE(output[Width_ - 2], "Block size should not be marked as unused");
        *output[Width_ - 2] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(size)));
    }

    void FlushBuffers(TState& s, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY(s.OutputPos_ > 0);
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            if (i != BitmapIndex_) {
                NUdf::TUnboxedValue result = s.Builders_[i] ? ctx.HolderFactory.CreateArrowBlock(s.Builders_[i]->Build(s.Finish_)) : s.InputValues_[i];
                if (output[outIndex]) {
                    *output[outIndex] = result;
                }
                outIndex++;
            }
        }

        WriteOutputBlockSize(s.OutputPos_, ctx, output);
        s.OutputPos_ = 0;
    }

    void CopyAsIs(TState& s, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY(s.OutputPos_ > 0);
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            if (i != BitmapIndex_ && output[outIndex]) {
                *output[outIndex] = s.InputValues_[i];
            }
            if (i != BitmapIndex_) {
                outIndex++;
            }
        }

        WriteOutputBlockSize(s.OutputPos_, ctx, output);
        s.InputSize_ = s.OutputPos_ = 0;
    }
private:
    IComputationWideFlowNode* Flow_;
    const ui32 BitmapIndex_;
    const TVector<TBlockType*> Types_;
    const ui32 Width_;
};

} // namespace

IComputationNode* WrapBlockCompress(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    const ui32 width = wideComponents.size();
    MKQL_ENSURE(width > 1, "Expected at least two columns");

    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < width - 1, "Bad bitmap index");

    TVector<TBlockType*> types;
    bool bitmapIsScalar = false;
    bool allScalars = true;
    for (ui32 i = 0; i < width; ++i) {
        types.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        bool isScalar = types.back()->GetShape() == TBlockType::EShape::Scalar;
        if (i == width - 1) {
            MKQL_ENSURE(isScalar, "Expecting scalar block size as last column");
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Uint64, "Expecting Uint64 as last column");
        } else if (i == index) {
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Bool, "Expecting Bool as bitmap column");
            bitmapIsScalar = isScalar;
        } else {
            allScalars = allScalars && isScalar;
        }
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    if (bitmapIsScalar) {
        return new TCompressWithScalarBitmap(ctx.Mutables, wideFlow, index, width);
    } else if (allScalars) {
        return new TCompressScalars(ctx.Mutables, wideFlow, index, width);
    }

    return new TCompressBlocks(ctx.Mutables, wideFlow, index, std::move(types));
}


} // namespace NMiniKQL
} // namespace NKikimr
