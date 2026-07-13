#include "mkql_block_way.h"

#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/dense_union_scalar.h>

#include <arrow/util/bitmap_ops.h>
#include <arrow/util/checked_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsOptional>
class TWayBlockExecBase {
public:
    explicit TWayBlockExecBase(TType* resultItemType)
        : ResultItemType_(resultItemType)
    {
    }

    virtual ~TWayBlockExecBase() = default;

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        const arrow::Datum& variantDatum = batch.values[0];
        if (variantDatum.is_scalar()) {
            *res = ConvertScalar(ResultItemType_, ComputeScalarKey(*variantDatum.scalar()), *ctx->memory_pool());
            return arrow::Status::OK();
        }
        MKQL_ENSURE(variantDatum.is_array(), "Expected array datum");
        const arrow::ArrayData& inputArray = *variantDatum.array();
        const i8* typeCodes = GetUnionArray(inputArray).GetValues<i8>(1);
        return ExecArray(ctx, inputArray, typeCodes, res);
    }

protected:
    static const arrow::ArrayData& GetUnionArray(const arrow::ArrayData& inputArray) {
        if constexpr (IsOptional) {
            return *inputArray.child_data[0];
        } else {
            return inputArray;
        }
    }

private:
    virtual NUdf::TBlockItem MakeKey(ui32 alternativeIndex) const = 0;

    virtual arrow::Status ExecArray(arrow::compute::KernelContext* ctx, const arrow::ArrayData& inputArray,
                                    const i8* typeCodes, arrow::Datum* res) const = 0;

    NUdf::TBlockItem ComputeScalarKey(const arrow::Scalar& scalar) const {
        if constexpr (IsOptional) {
            if (!scalar.is_valid) {
                return NUdf::TBlockItem{};
            }
            const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
            const auto& unionScalar = arrow::internal::checked_cast<const NYql::NUdf::TDenseUnionScalar&>(*structScalar.value.front());
            return MakeKey(unionScalar.Index);
        } else {
            const auto& unionScalar = arrow::internal::checked_cast<const NYql::NUdf::TDenseUnionScalar&>(scalar);
            return MakeKey(unionScalar.Index);
        }
    }

    TType* const ResultItemType_;
};

template <bool IsOptional>
class TWayBlockExecTuple final: public TWayBlockExecBase<IsOptional> {
    using TBase = TWayBlockExecBase<IsOptional>;

public:
    using TBase::TBase;

private:
    NUdf::TBlockItem MakeKey(ui32 alternativeIndex) const final {
        return NUdf::TBlockItem(alternativeIndex);
    }

    arrow::Status ExecArray(arrow::compute::KernelContext* ctx, const arrow::ArrayData& inputArray,
                            const i8* typeCodes, arrow::Datum* res) const final {
        auto* pool = ctx->memory_pool();
        const i64 length = inputArray.length;

        std::shared_ptr<arrow::Buffer> values = ARROW_RESULT(arrow::AllocateBuffer(length * sizeof(ui32), pool));
        ui32* outValues = reinterpret_cast<ui32*>(values->mutable_data());
        for (i64 i = 0; i < length; ++i) {
            outValues[i] = static_cast<ui32>(typeCodes[i]);
        }

        std::shared_ptr<arrow::Buffer> mask;
        if (IsOptional && inputArray.buffers[0]) {
            mask = MakeDenseBitmapCopyIfOffsetDiffers(inputArray.buffers[0], length, inputArray.offset, 0, pool);
        }
        *res = arrow::ArrayData::Make(arrow::uint32(), length, {std::move(mask), std::move(values)});
        return arrow::Status::OK();
    }
};

template <bool IsOptional>
class TWayBlockExecStruct final: public TWayBlockExecBase<IsOptional> {
    using TBase = TWayBlockExecBase<IsOptional>;

public:
    TWayBlockExecStruct(TType* resultItemType, TVector<TString>&& alternativeNames)
        : TBase(resultItemType)
        , AlternativeNames_(std::move(alternativeNames))
    {
    }

private:
    NUdf::TBlockItem MakeKey(ui32 alternativeIndex) const final {
        return NUdf::TBlockItem(NUdf::TStringRef(AlternativeNames_[alternativeIndex]));
    }

    arrow::Status ExecArray(arrow::compute::KernelContext* ctx, const arrow::ArrayData& inputArray,
                            const i8* typeCodes, arrow::Datum* res) const final {
        auto* pool = ctx->memory_pool();
        arrow::Datum built = BuildNameArray(pool, inputArray, typeCodes);

        const ui8* maskBits = (IsOptional && inputArray.buffers[0]) ? inputArray.buffers[0]->data() : nullptr;
        *res = maskBits ? ReattachNullMask(pool, built, inputArray, maskBits) : std::move(built);
        return arrow::Status::OK();
    }

    arrow::Datum BuildNameArray(arrow::MemoryPool* pool, const arrow::ArrayData& inputArray, const i8* typeCodes) const {
        const i64 length = inputArray.length;
        NYql::NUdf::TStringArrayBuilder<arrow::StringType, false> builder(
            TTypeInfoHelper(), arrow::utf8(), *pool, static_cast<size_t>(length));
        for (i64 i = 0; i < length; ++i) {
            const bool isNull = IsOptional && NYql::NUdf::IsNull(inputArray, static_cast<size_t>(i));
            const NUdf::TStringRef name = isNull
                                              ? NUdf::TStringRef("", 0)
                                              : NUdf::TStringRef(AlternativeNames_[static_cast<ui32>(typeCodes[i])]);
            builder.Add(NUdf::TBlockItem(name));
        }
        return builder.Build(true);
    }

    static arrow::Datum ReattachNullMask(arrow::MemoryPool* pool, const arrow::Datum& built,
                                         const arrow::ArrayData& inputArray, const ui8* maskBits) {
        TVector<std::shared_ptr<arrow::ArrayData>> chunks;
        i64 processed = 0;
        ForEachArrayData(built, [&](const std::shared_ptr<arrow::ArrayData>& chunk) {
            auto withMask = chunk->Copy();
            MKQL_ENSURE(chunk->offset == 0, "Expected offset after string builder to be 0");
            auto chunkMask = AllocateBitmapWithReserve(chunk->length, pool);
            arrow::internal::CopyBitmap(maskBits, inputArray.offset + processed, chunk->length, chunkMask->mutable_data(), 0);
            withMask->buffers[0] = std::move(chunkMask);
            withMask->SetNullCount(arrow::kUnknownNullCount);
            processed += chunk->length;
            chunks.push_back(std::move(withMask));
        });
        return MakeArray(chunks);
    }

    const TVector<TString> AlternativeNames_;
};

template <bool IsOptional>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockWayKernel(const TVector<TType*>& argTypes, TType* resultType,
                                                                 std::shared_ptr<const TWayBlockExecBase<IsOptional>> exec) {
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(
        ConvertToInputTypes(argTypes),
        ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
        });
    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    return kernel;
}

TVector<TString> CollectStructAlternativeNames(TStructType* structType) {
    TVector<TString> names;
    names.reserve(structType->GetMembersCount());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        names.emplace_back(structType->GetMemberName(i));
    }
    return names;
}

template <bool IsOptional>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockWayKernel(const TVector<TType*>& argTypes, TType* resultType,
                                                                 TType* underlyingType) {
    TType* resultItemType = AS_TYPE(TBlockType, resultType)->GetItemType();
    std::shared_ptr<const TWayBlockExecBase<IsOptional>> exec;
    if (underlyingType->IsTuple()) {
        exec = std::make_shared<TWayBlockExecTuple<IsOptional>>(resultItemType);
    } else {
        exec = std::make_shared<TWayBlockExecStruct<IsOptional>>(
            resultItemType, CollectStructAlternativeNames(AS_TYPE(TStructType, underlyingType)));
    }
    return MakeBlockWayKernel<IsOptional>(argTypes, resultType, std::move(exec));
}

} // namespace

IComputationNode* WrapBlockWay(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");

    auto blockType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    bool isOptional;
    auto variantItemType = UnpackOptional(blockType->GetItemType(), isOptional);
    auto underlyingType = AS_TYPE(TVariantType, variantItemType)->GetUnderlyingType();

    TVector<TType*> argsTypes = {blockType};
    auto resultType = callable.GetType()->GetReturnType();
    auto kernel = isOptional
                      ? MakeBlockWayKernel<true>(argsTypes, resultType, underlyingType)
                      : MakeBlockWayKernel<false>(argsTypes, resultType, underlyingType);

    TComputationNodePtrVector argsNodes = {LocateNode(ctx.NodeLocator, callable, 0)};
    return new TBlockFuncNode(ctx.Mutables,
                              ctx.RuntimeSettings->DatumValidation.Get(),
                              callable.GetType()->GetName(),
                              std::move(argsNodes),
                              argsTypes,
                              resultType,
                              *kernel,
                              kernel);
}

} // namespace NKikimr::NMiniKQL
