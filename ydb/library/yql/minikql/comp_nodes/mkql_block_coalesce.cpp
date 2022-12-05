#include "mkql_block_coalesce.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/util/bitmap_ops.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCoalesceBlockWrapper : public TMutableComputationNode<TCoalesceBlockWrapper> {
public:
    TCoalesceBlockWrapper(TComputationMutables& mutables, IComputationNode* first, IComputationNode* second, NUdf::EDataSlot slot)
        : TMutableComputationNode(mutables)
        , First(first)
        , Second(second)
        , Slot(slot)
    {
        MKQL_ENSURE(ConvertArrowType(slot, Type), "Unsupported type of data");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto first = First->GetValue(ctx);
        auto second = Second->GetValue(ctx);

        const auto& firstDatum = TArrowBlock::From(first).GetDatum();
        const auto& secondDatum = TArrowBlock::From(second).GetDatum();

        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            return (firstDatum.null_count() == 0) ? first.Release() : second.Release();
        }

        size_t len = firstDatum.is_scalar() ? (size_t)secondDatum.length() : (size_t)firstDatum.length();
        if (firstDatum.null_count() == firstDatum.length()) {
            return secondDatum.is_scalar() ? CopyAsArray(ctx, secondDatum, len) : second.Release();
        } else if (firstDatum.null_count() == 0) {
            return firstDatum.is_scalar() ? CopyAsArray(ctx, firstDatum, len) : first.Release();
        }

        Y_VERIFY(firstDatum.is_array());
        if (secondDatum.null_count() == secondDatum.length()) {
            return first.Release();
        }

        return secondDatum.is_scalar() ?
            Coalesce(ctx, firstDatum.array(), secondDatum) :
            Coalesce(ctx, firstDatum.array(), secondDatum.array());
    }

private:
    NUdf::TUnboxedValuePod CopyAsArray(TComputationContext& ctx, const arrow::Datum& scalar, size_t len) const {
        Y_VERIFY(scalar.is_scalar());
        const auto* fixedType = dynamic_cast<const arrow::FixedWidthType*>(Type.get());
        MKQL_ENSURE(fixedType, "Only fixed width types are currently supported");
        size_t dataSize = (size_t)arrow::BitUtil::BytesForBits(fixedType->bit_width() * len);

        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(dataSize, &ctx.ArrowMemoryPool).ValueOrDie();
        std::shared_ptr<arrow::Buffer> bitmap;
        if (scalar.scalar()->is_valid) {
            switch (Slot) {
                case NUdf::EDataSlot::Int8:
                    FillArray(scalar.scalar_as<arrow::Int8Scalar>().value, (int8_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Uint8:
                case NUdf::EDataSlot::Bool:
                    FillArray(scalar.scalar_as<arrow::UInt8Scalar>().value, (uint8_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Int16:
                    FillArray(scalar.scalar_as<arrow::Int16Scalar>().value, (int16_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Uint16:
                case NUdf::EDataSlot::Date:
                    FillArray(scalar.scalar_as<arrow::UInt16Scalar>().value, (uint16_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Int32:
                    FillArray(scalar.scalar_as<arrow::Int32Scalar>().value, (int32_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Uint32:
                case NUdf::EDataSlot::Datetime:
                    FillArray(scalar.scalar_as<arrow::UInt32Scalar>().value, (uint32_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Int64:
                case NUdf::EDataSlot::Interval:
                    FillArray(scalar.scalar_as<arrow::Int64Scalar>().value, (int64_t*)data->mutable_data(), len); break;
                case NUdf::EDataSlot::Uint64:
                case NUdf::EDataSlot::Timestamp:
                    FillArray(scalar.scalar_as<arrow::UInt64Scalar>().value, (uint64_t*)data->mutable_data(), len); break;
                default:
                    MKQL_ENSURE(false, "Unsupported data slot");
            }
        } else {
            bitmap = arrow::AllocateEmptyBitmap(len, &ctx.ArrowMemoryPool).ValueOrDie();
            std::memset(data->mutable_data(), 0, dataSize);
        }

        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(Type, len, { bitmap, data }));
    }

    template<typename T>
    static void FillArray(T value, T* dst, size_t len) {
        while (len) {
            *dst++ = value;
            len--;
        }
    }

    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const std::shared_ptr<arrow::ArrayData>& arr, const arrow::Datum& scalar) const {
        Y_VERIFY(scalar.is_scalar());
        Y_VERIFY(scalar.scalar()->is_valid);

        size_t offset = (size_t)arr->offset;
        size_t len = (size_t)arr->length;
        const ui8* arrMask = arr->GetValues<ui8>(0, 0);
        Y_VERIFY(arrMask);
        NUdf::TUnboxedValuePod result;
        switch (Slot) {
            case NUdf::EDataSlot::Int8:
                result = Coalesce(ctx, arr->GetValues<int8_t>(1), arrMask, scalar.scalar_as<arrow::Int8Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Uint8:
            case NUdf::EDataSlot::Bool:
                result = Coalesce(ctx, arr->GetValues<uint8_t>(1), arrMask, scalar.scalar_as<arrow::UInt8Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Int16:
                result = Coalesce(ctx, arr->GetValues<int16_t>(1), arrMask, scalar.scalar_as<arrow::Int16Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Date:
                result = Coalesce(ctx, arr->GetValues<uint16_t>(1), arrMask, scalar.scalar_as<arrow::UInt16Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Int32:
                result = Coalesce(ctx, arr->GetValues<int32_t>(1), arrMask, scalar.scalar_as<arrow::Int32Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Datetime:
                result = Coalesce(ctx, arr->GetValues<uint32_t>(1), arrMask, scalar.scalar_as<arrow::UInt32Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Interval:
                result = Coalesce(ctx, arr->GetValues<int64_t>(1), arrMask, scalar.scalar_as<arrow::Int64Scalar>().value, offset, len); break;
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Timestamp:
                result = Coalesce(ctx, arr->GetValues<uint64_t>(1), arrMask, scalar.scalar_as<arrow::UInt64Scalar>().value, offset, len); break;
            default:
                MKQL_ENSURE(false, "Unsupported data slot");
        }
        return result;
    }

    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const std::shared_ptr<arrow::ArrayData>& arr1, const std::shared_ptr<arrow::ArrayData>& arr2) const {
        Y_VERIFY(arr1->offset == arr2->offset);
        Y_VERIFY(arr1->length == arr2->length);

        size_t offset = (size_t)arr1->offset;
        size_t len = (size_t)arr1->length;
        const ui8* arr1Mask = arr1->GetValues<ui8>(0, 0);
        Y_ENSURE(arr1Mask);
        Y_ENSURE(arr2->buffers.size() == 2);
        std::shared_ptr<arrow::Buffer> arr2Mask = arr2->buffers[0];
        NUdf::TUnboxedValuePod result;
        switch (Slot) {
            case NUdf::EDataSlot::Int8:
                result = Coalesce(ctx, arr1->GetValues<int8_t>(1), arr1Mask, arr2->GetValues<int8_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Uint8:
            case NUdf::EDataSlot::Bool:
                result = Coalesce(ctx, arr1->GetValues<uint8_t>(1), arr1Mask, arr2->GetValues<uint8_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Int16:
                result = Coalesce(ctx, arr1->GetValues<int16_t>(1), arr1Mask, arr2->GetValues<int16_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Date:
                result = Coalesce(ctx, arr1->GetValues<uint16_t>(1), arr1Mask, arr2->GetValues<uint16_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Int32:
                result = Coalesce(ctx, arr1->GetValues<int32_t>(1), arr1Mask, arr2->GetValues<int32_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Datetime:
                result = Coalesce(ctx, arr1->GetValues<uint32_t>(1), arr1Mask, arr2->GetValues<uint32_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Interval:
                result = Coalesce(ctx, arr1->GetValues<int64_t>(1), arr1Mask, arr2->GetValues<int64_t>(1), arr2Mask, offset, len); break;
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Timestamp:
                result = Coalesce(ctx, arr1->GetValues<uint64_t>(1), arr1Mask, arr2->GetValues<uint64_t>(1), arr2Mask, offset, len); break;
            default:
                MKQL_ENSURE(false, "Unsupported data slot");
        }
        return result;
    }

    template<typename T>
    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const T* first, const ui8* firstMask, T second, size_t offset, size_t len) const {
        const auto* fixedType = dynamic_cast<const arrow::FixedWidthType*>(Type.get());
        MKQL_ENSURE(fixedType, "Only fixed width types are currently supported");
        size_t size = (size_t)arrow::BitUtil::BytesForBits(fixedType->bit_width());
        Y_VERIFY(size > 0);

        std::shared_ptr<arrow::Buffer> result = arrow::AllocateBuffer(len * size, &ctx.ArrowMemoryPool).ValueOrDie();

        T* output = (T*)result->mutable_data();
        for (size_t i = 0; i < len; ++i) {
            T m1 = T(((firstMask[offset >> 3] >> (offset & 0x07)) & 1) ^ 1) - T(1);

            *output++ = (*first & m1) | ((~m1) & second);
            first++;
            offset++;
        }
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(Type, len, { std::shared_ptr<arrow::Buffer>(), result }));
    }

    template<typename T>
    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const T* first, const ui8* firstMask,
        const T* second, const std::shared_ptr<arrow::Buffer>& secondMask, size_t offset, size_t len) const
    {
        const auto* fixedType = dynamic_cast<const arrow::FixedWidthType*>(Type.get());
        MKQL_ENSURE(fixedType, "Only fixed width types are currently supported");
        size_t size = (size_t)arrow::BitUtil::BytesForBits(fixedType->bit_width());
        Y_VERIFY(size > 0);

        std::shared_ptr<arrow::Buffer> result = arrow::AllocateBuffer(len * size, &ctx.ArrowMemoryPool).ValueOrDie();
        T* output = (T*)result->mutable_data();
        std::shared_ptr<arrow::Buffer> resultMask;
        if (secondMask) {
            // resultMask = m1 | m2;
            // result = (v1 & m1) | (v2 & m2 & ~m1)
            resultMask = arrow::internal::BitmapOr(&ctx.ArrowMemoryPool, firstMask, offset, secondMask->data(), offset, len, 0).ValueOrDie();
            const ui8* sm = secondMask->data();
            for (size_t i = 0; i < len; ++i) {
                T m1 = T(((firstMask[offset >> 3] >> (offset & 0x07)) & 1) ^ 1) - T(1);
                T m2 = T(((sm[offset >> 3] >> (offset & 0x07)) & 1) ^ 1) - T(1);

                *output++ = (*first & m1) | ((~m1) & *second & m2);
                first++;
                second++;
                offset++;
            }
        } else {
            for (size_t i = 0; i < len; ++i) {
                T m1 = T(((firstMask[offset >> 3] >> (offset & 0x07)) & 1) ^ 1) - T(1);
                *output++ = (*first & m1) | ((~m1) & *second);
                first++;
                second++;
                offset++;
            }
        }

        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(Type, len, { resultMask, result }));
    }

    void RegisterDependencies() const final {
        DependsOn(First);
        DependsOn(Second);
    }

    IComputationNode* const First;
    IComputationNode* const Second;
    std::shared_ptr<arrow::DataType> Type;
    const NUdf::EDataSlot Slot;
};

} // namespace

IComputationNode* WrapBlockCoalesce(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto first = callable.GetInput(0);
    auto second = callable.GetInput(1);

    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    bool firstOptional;
    auto firstItemType = UnpackOptionalData(firstType->GetItemType(), firstOptional);

    bool secondOptional;
    auto secondItemType = UnpackOptionalData(secondType->GetItemType(), secondOptional);

    MKQL_ENSURE(firstOptional, "BlockCoalesce with non-optional first argument");
    MKQL_ENSURE(firstItemType->IsSameType(*secondItemType), "Mismatch types");

    auto firstCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto secondCompute = LocateNode(ctx.NodeLocator, callable, 1);
    return new TCoalesceBlockWrapper(ctx.Mutables, firstCompute, secondCompute, *secondItemType->GetDataSlot());
}

}
}
