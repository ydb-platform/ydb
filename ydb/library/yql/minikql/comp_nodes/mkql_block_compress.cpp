#include "mkql_block_compress.h"
#include "mkql_bit_utils.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/util/bitmap.h>
#include <arrow/util/bit_util.h>
#include <arrow/array/array_primitive.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr size_t MaxBlockSizeInBytes = 1_MB;

class TCompressWithScalarBitmap : public TStatefulWideFlowComputationNode<TCompressWithScalarBitmap> {
public:
    TCompressWithScalarBitmap(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
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

class TCompressScalars : public TStatefulWideFlowComputationNode<TCompressScalars> {
public:
    TCompressScalars(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
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

struct TBlockCompressDescriptor {
    NUdf::EDataSlot Slot_;
    bool IsOptional_;
    std::shared_ptr<arrow::DataType> ArrowType_;
    bool IsScalar_;
    TMaybe<size_t> BitWidth_; // only set for arrays with fixed-width data
};

class TCompressBlocks : public TStatefulWideFlowComputationNode<TCompressBlocks> {
public:
    TCompressBlocks(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, TVector<TBlockCompressDescriptor>&& descriptors)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Descriptors_(std::move(descriptors))
        , Width_(Descriptors_.size())
        , DesiredLen_(CalcDesiredLen(Descriptors_, BitmapIndex_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx, output);

        EFetchResult result = EFetchResult::One;
        for (;;) {
            if (!s.InputPopCount_) {
                result = s.Finish_ ? EFetchResult::Finish : Flow_->FetchValues(ctx, s.ValuePointers_.data());
                if (result != EFetchResult::One) {
                    break;
                }
                auto& bitmap = s.InputValues_[BitmapIndex_];
                const auto arr = TArrowBlock::From(bitmap).GetDatum().array();
                s.InputSize_ = (size_t)arr->length;
                s.InputPopCount_ = GetBitmapPopCount(arr);
                continue;
            }

            if (!s.HaveBlocks_) {
                // client is not interested in any block columns
                Y_VERIFY_DEBUG(s.OutputPos_ == 0);
                Y_VERIFY_DEBUG(s.OutputSize_ == 0);
                WriteOutputBlockSize(s.InputPopCount_, ctx, output);
                s.InputSize_ = s.InputPopCount_ = 0;
                break;
            }

            if (s.OutputSize_ == 0) {
                AllocateBuffers(s, ctx, output);
            }

            if (s.InputPopCount_ + s.OutputPos_ <= s.OutputSize_) {
                AppendInput(s, ctx, output);
            } else {
                FlushBuffers(s, ctx, output);
                break;
            }
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
    struct TOutputBuffers {
        std::shared_ptr<arrow::Buffer> Data_;
        std::shared_ptr<arrow::Buffer> Nulls_;
        size_t MaxObjCount_ = 0;

        static std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, TComputationContext& ctx) {
            // align up to 64 bit
            bitCount = (bitCount + 63u) & ~size_t(63u);
            // this simplifies code compression code - we can write single 64 bit word after array boundaries
            bitCount += 64;
            return ARROW_RESULT(arrow::AllocateBitmap(bitCount, &ctx.ArrowMemoryPool));
        }

        static std::shared_ptr<arrow::Buffer> AllocateBufferWithReserve(size_t objCount, size_t bitSize, TComputationContext& ctx) {
            // this simplifies code compression code - we can write single object after array boundaries
            Y_VERIFY(bitSize >= 8);
            size_t sz = arrow::BitUtil::BytesForBits((objCount + 1) * bitSize);
            return ARROW_RESULT(arrow::AllocateBuffer(sz, &ctx.ArrowMemoryPool));
        }

        void Allocate(size_t count, size_t bitSize, TComputationContext& ctx) {
            Y_VERIFY_DEBUG(!Data_);
            Y_VERIFY_DEBUG(!Nulls_);
            Y_VERIFY_DEBUG(!MaxObjCount_);
            MaxObjCount_ = count;
            Data_ = AllocateBufferWithReserve(count, bitSize, ctx);
            // Nulls_ allocation will be delayed until actual nulls are encountered
        }

        NUdf::TUnboxedValue PullUnboxedValue(std::shared_ptr<arrow::DataType> type, size_t length, TComputationContext& ctx) {
            auto arrayData = arrow::ArrayData::Make(type, length, { Nulls_, Data_ });
            Data_.reset();
            Nulls_.reset();
            MaxObjCount_ = 0;
            return ctx.HolderFactory.CreateArrowBlock(arrow::Datum(arrayData));
        }
    };

    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<NUdf::TUnboxedValue> InputValues_;
        size_t InputPopCount_ = 0;
        size_t InputSize_ = 0;

        TVector<TOutputBuffers> OutputBuffers_;
        size_t OutputPos_ = 0;
        size_t OutputSize_ = 0;

        bool Finish_ = false;
        bool HaveBlocks_ = false;

        TState(TMemoryUsageInfo* memInfo, ui32 width, ui32 bitmapIndex, NUdf::TUnboxedValue*const* output,
            const TVector<TBlockCompressDescriptor>& descriptors)
            : TComputationValue(memInfo)
            , ValuePointers_(width)
            , InputValues_(width)
            , OutputBuffers_(width)
        {
            for (ui32 i = 0, outIndex = 0; i < width; ++i) {
                ValuePointers_[i] = &InputValues_[i];
                if (i != bitmapIndex) {
                    HaveBlocks_ = HaveBlocks_ || (!descriptors[i].IsScalar_ && output[outIndex] != nullptr);
                    outIndex++;
                }
            }
        }
    };

    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, BitmapIndex_, output, Descriptors_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    static size_t CalcDesiredLen(const TVector<TBlockCompressDescriptor>& descriptors, ui32 bitmapIndex) {
        size_t result = std::numeric_limits<size_t>::max();
        for (ui32 i = 0; i < descriptors.size(); ++i) {
            if (i != bitmapIndex && !descriptors[i].IsScalar_) {
                size_t len  = 8 * MaxBlockSizeInBytes / *descriptors[i].BitWidth_;
                result = std::min(result, len);
            }
        }

        MKQL_ENSURE(result != std::numeric_limits<size_t>::max(), "Missing block columns - TCompressScalars should be used");
        return result;
    }

    void WriteOutputBlockSize(size_t size, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        MKQL_ENSURE(output[Width_ - 2], "Block size should not be marked as unused");
        *output[Width_ - 2] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(size)));
    }

    void AllocateBuffers(TState& s, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY_DEBUG(s.OutputSize_ == 0);
        Y_VERIFY_DEBUG(s.OutputPos_ == 0);
        Y_VERIFY_DEBUG(s.InputPopCount_ > 0);
        const size_t count = std::max(s.InputPopCount_, DesiredLen_);
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            auto& desc = Descriptors_[i];
            if (i != BitmapIndex_ && !desc.IsScalar_ && output[outIndex]) {
                auto& buffers = s.OutputBuffers_[i];
                buffers.Allocate(count, *desc.BitWidth_, ctx);
            }
            if (i != BitmapIndex_) {
                outIndex++;
            }
        }
        s.OutputSize_ = count;
    }

    void FlushBuffers(TState& s, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY_DEBUG(s.OutputPos_ > 0);
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            auto& desc = Descriptors_[i];
            if (i != BitmapIndex_ && output[outIndex]) {
                *output[outIndex] = desc.IsScalar_ ? s.InputValues_[i] : s.OutputBuffers_[i].PullUnboxedValue(desc.ArrowType_, s.OutputPos_, ctx);
            }
            if (i != BitmapIndex_) {
                outIndex++;
            }
        }

        WriteOutputBlockSize(s.OutputPos_, ctx, output);
        s.OutputPos_ = s.OutputSize_ = 0;
    }

    void AppendInput(TState& s, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY_DEBUG(s.InputPopCount_ > 0);
        Y_VERIFY_DEBUG(s.InputSize_ >= s.InputPopCount_);

        auto bitmap = TArrowBlock::From(s.InputValues_[BitmapIndex_]).GetDatum().array();
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            auto& desc = Descriptors_[i];
            if (i != BitmapIndex_ && !desc.IsScalar_ && output[outIndex]) {
                auto array = TArrowBlock::From(s.InputValues_[i]).GetDatum().array();
                Y_VERIFY_DEBUG(array->length == s.InputSize_);
                AppendArray(array, bitmap, s.OutputBuffers_[i], s.OutputPos_, Descriptors_[i].Slot_, ctx);
            }

            if (i != BitmapIndex_) {
                outIndex++;
            }
        }

        s.OutputPos_ += s.InputPopCount_;
        s.InputSize_ = s.InputPopCount_ = 0;
    }

    void AppendArray(const std::shared_ptr<arrow::ArrayData>& src, const std::shared_ptr<arrow::ArrayData>& bitmap,
        TOutputBuffers& dst, size_t dstPos, NUdf::EDataSlot slot, TComputationContext& ctx) const
    {
        const ui8* bitmapData = bitmap->GetValues<ui8>(1);
        Y_VERIFY_DEBUG(bitmap->length == src->length);
        Y_VERIFY_DEBUG(bitmap->offset == src->offset);
        const size_t length = src->length;
        ui8* mutDstBase = dst.Data_->mutable_data();

        switch (slot) {
        case NUdf::EDataSlot::Int8:
            CompressArray(src->GetValues<i8>(1),   bitmapData, (i8*)mutDstBase   + dstPos, length); break;
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            CompressArray(src->GetValues<ui8>(1),  bitmapData, (ui8*)mutDstBase  + dstPos, length); break;
        case NUdf::EDataSlot::Int16:
            CompressArray(src->GetValues<i16>(1),  bitmapData, (i16*)mutDstBase  + dstPos, length); break;
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            CompressArray(src->GetValues<ui16>(1), bitmapData, (ui16*)mutDstBase + dstPos, length); break;
        case NUdf::EDataSlot::Int32:
            CompressArray(src->GetValues<i32>(1),  bitmapData, (i32*)mutDstBase  + dstPos, length); break;
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            CompressArray(src->GetValues<ui32>(1), bitmapData, (ui32*)mutDstBase + dstPos, length); break;
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            CompressArray(src->GetValues<i64>(1),  bitmapData, (i64*)mutDstBase  + dstPos, length); break;
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            CompressArray(src->GetValues<ui64>(1), bitmapData, (ui64*)mutDstBase + dstPos, length); break;
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }

        if (src->GetNullCount()) {
            if (!dst.Nulls_) {
                dst.Nulls_ = TOutputBuffers::AllocateBitmapWithReserve(dst.MaxObjCount_, ctx);
                arrow::BitUtil::SetBitsTo(dst.Nulls_->mutable_data(), 0, dstPos, false);
            }
            // TODO: optimize
            auto denseBitmap = ARROW_RESULT(arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool));
            CompressSparseBitmap(denseBitmap->mutable_data(), bitmapData, length);
            CompressBitmap(src->GetValues<ui8>(0, 0), src->offset, denseBitmap->data(), 0,
                           dst.Nulls_->mutable_data(), dstPos, length);
        }
    }

private:
    IComputationWideFlowNode* Flow_;
    const ui32 BitmapIndex_;
    const TVector<TBlockCompressDescriptor> Descriptors_;
    const ui32 Width_;
    const size_t DesiredLen_;
};

} // namespace

IComputationNode* WrapBlockCompress(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    const ui32 width = tupleType->GetElementsCount();
    MKQL_ENSURE(width > 1, "Expected at least two columns");

    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < width - 1, "Bad bitmap index");

    TVector<TBlockCompressDescriptor> descriptors;
    bool bitmapIsScalar = false;
    bool allScalars = true;
    for (ui32 i = 0; i < width; ++i) {
        descriptors.emplace_back();
        auto& descriptor = descriptors.back();

        const auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(i));
        TDataType* unpacked = UnpackOptionalData(blockType->GetItemType(), descriptor.IsOptional_);
        descriptor.Slot_ = *unpacked->GetDataSlot();

        bool isOptional;
        MKQL_ENSURE(ConvertArrowType(unpacked, isOptional, descriptor.ArrowType_), "Unsupported type");

        descriptor.IsScalar_ = blockType->GetShape() == TBlockType::EShape::Scalar;
        if (i == width - 1) {
            MKQL_ENSURE(descriptor.IsScalar_, "Expecting scalar block size as last column");
            MKQL_ENSURE(!descriptor.IsOptional_ && descriptor.Slot_ == NUdf::EDataSlot::Uint64, "Expecting Uint64 as last column");
        } else if (i == index) {
            MKQL_ENSURE(!descriptor.IsOptional_ && descriptor.Slot_ == NUdf::EDataSlot::Bool, "Expecting Bool as bitmap column");
            bitmapIsScalar = descriptor.IsScalar_;
        } else {
            allScalars = allScalars && descriptor.IsScalar_;
        }

        if (!descriptor.IsScalar_) {
            const auto* fixedType = dynamic_cast<const arrow::FixedWidthType*>(descriptor.ArrowType_.get());
            MKQL_ENSURE(fixedType, "Only fixed width types are currently supported");
            descriptor.BitWidth_ = (size_t)fixedType->bit_width();
        }
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    if (bitmapIsScalar) {
        return new TCompressWithScalarBitmap(ctx.Mutables, wideFlow, index, width);
    } else if (allScalars) {
        return new TCompressScalars(ctx.Mutables, wideFlow, index, width);
    }

    return new TCompressBlocks(ctx.Mutables, wideFlow, index, std::move(descriptors));
}


} // namespace NMiniKQL
} // namespace NKikimr
