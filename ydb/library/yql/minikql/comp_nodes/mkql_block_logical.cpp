#include "mkql_block_logical.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/util/bitmap.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/bit_util.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/util.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {

using arrow::internal::Bitmap;

std::shared_ptr<arrow::Buffer> CopyBitmap(arrow::MemoryPool* pool, const std::shared_ptr<arrow::Buffer>& bitmap, int64_t offset, int64_t len) {
    std::shared_ptr<arrow::Buffer> result = bitmap;
    if (bitmap && offset != 0) {
        result = arrow::AllocateBitmap(len, pool).ValueOrDie();
        arrow::internal::CopyBitmap(bitmap->data(), offset, len, result->mutable_data(), 0);
    }
    return result;
}

std::shared_ptr<arrow::Buffer> CopySparseBitmap(arrow::MemoryPool* pool, const std::shared_ptr<arrow::Buffer>& bitmap, int64_t offset, int64_t len) {
    std::shared_ptr<arrow::Buffer> result = bitmap;
    if (bitmap && offset != 0) {
        result = arrow::AllocateBuffer(len, pool).ValueOrDie();
        std::memcpy(result->mutable_data(), bitmap->data() + offset, len);
    }
    return result;

}

bool IsAllEqualsTo(const arrow::Datum& datum, bool value) {
    if (datum.null_count() != 0) {
        return false;
    }
    if (datum.is_scalar()) {
        return (datum.scalar_as<arrow::UInt8Scalar>().value & 1u) == value;
    }
    size_t len = datum.array()->length;
    size_t popCnt = GetSparseBitmapPopCount(datum.array()->GetValues<ui8>(1), len);
    return popCnt == (value ? len : 0);
}

class TBlockAndWrapper : public TMutableComputationNode<TBlockAndWrapper> {
public:
    TBlockAndWrapper(TComputationMutables& mutables, IComputationNode* first, IComputationNode* second, TBlockType::EShape shape)
        : TMutableComputationNode(mutables)
        , First(first)
        , Second(second)
        , ScalarResult(shape == TBlockType::EShape::Scalar)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto first = First->GetValue(ctx);
        const auto& firstDatum = TArrowBlock::From(first).GetDatum();

        if (IsAllEqualsTo(firstDatum, false)) {
            // false AND ... = false
            if (ScalarResult == firstDatum.is_scalar()) {
                return first.Release();
            }
            Y_VERIFY(firstDatum.is_scalar());
            // need length
            auto second = Second->GetValue(ctx);
            const auto& secondDatum = TArrowBlock::From(second).GetDatum();
            return MakeFalseArray(ctx, secondDatum.length());
        }

        auto second = Second->GetValue(ctx);
        const auto& secondDatum = TArrowBlock::From(second).GetDatum();

        if (IsAllEqualsTo(secondDatum, false)) {
            // ... AND false = false
            if (ScalarResult == secondDatum.is_scalar()) {
                return second.Release();
            }
            Y_VERIFY(secondDatum.is_scalar());
            return MakeFalseArray(ctx, firstDatum.length());
        }

        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            bool first_true =  firstDatum.scalar()->is_valid && (firstDatum.scalar_as<arrow::UInt8Scalar>().value & 1u != 0);
            bool first_false = firstDatum.scalar()->is_valid && (firstDatum.scalar_as<arrow::UInt8Scalar>().value & 1u == 0);

            bool second_true =  secondDatum.scalar()->is_valid && (secondDatum.scalar_as<arrow::UInt8Scalar>().value & 1u != 0);
            bool second_false = secondDatum.scalar()->is_valid && (secondDatum.scalar_as<arrow::UInt8Scalar>().value & 1u == 0);

            auto result = std::make_shared<arrow::UInt8Scalar>(ui8(first_true && second_true));
            result->is_valid = first_false || second_false || (first_true && second_true);

            return ctx.HolderFactory.CreateArrowBlock(arrow::Datum(result));
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow::UInt8Scalar>().value & 1u;
            bool valid = firstDatum.scalar()->is_valid;
            return CalcScalarArray(ctx, value, valid, secondDatum.array());
        }

        if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow::UInt8Scalar>().value & 1u;
            bool valid = secondDatum.scalar()->is_valid;
            return CalcScalarArray(ctx, value, valid, firstDatum.array());
        }

        return CalcArrayArray(ctx, firstDatum.array(), secondDatum.array());
    }

private:
    NUdf::TUnboxedValuePod MakeFalseArray(TComputationContext& ctx, int64_t len) const {
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(len, &ctx.ArrowMemoryPool).ValueOrDie();
        std::memset(data->mutable_data(), 0, len);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arrow::uint8(), len, { std::shared_ptr<arrow::Buffer>{}, data }));
    }

    NUdf::TUnboxedValuePod CalcScalarArray(TComputationContext& ctx, ui8 value, bool valid, const std::shared_ptr<arrow::ArrayData>& arr) const {
        bool first_true = valid && value;
        bool first_false = valid && !value;

        if (first_false) {
            return MakeFalseArray(ctx, arr->length);
        }

        if (first_true) {
            return ctx.HolderFactory.CreateArrowBlock(arr);
        }

        // scalar is null -> result is valid _only_ if arr[i] == false
        //bitmap = bitmap and not data[i]
        std::shared_ptr<arrow::Buffer> bitmap = arrow::AllocateBitmap(arr->length, &ctx.ArrowMemoryPool).ValueOrDie();
        CompressSparseBitmapNegate(bitmap->mutable_data(), arr->GetValues<ui8>(1), arr->length);
        if (arr->buffers[0]) {
            bitmap = arrow::internal::BitmapAnd(&ctx.ArrowMemoryPool, arr->GetValues<ui8>(0, 0), arr->offset, bitmap->data(), 0, arr->length, 0).ValueOrDie();
        }
        std::shared_ptr<arrow::Buffer> data = CopySparseBitmap(&ctx.ArrowMemoryPool, arr->buffers[1], arr->offset, arr->length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr->type, arr->length, { bitmap, data }));
    }

    NUdf::TUnboxedValuePod CalcArrayArray(TComputationContext& ctx, const std::shared_ptr<arrow::ArrayData>& arr1,
                                          const std::shared_ptr<arrow::ArrayData>& arr2) const
    {
        Y_VERIFY(arr1->offset == arr2->offset);
        Y_VERIFY(arr1->length == arr2->length);
        auto buf1 = arr1->buffers[0];
        auto buf2 = arr2->buffers[0];
        const int64_t offset = arr1->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow::Buffer> bitmap;
        if (buf1 || buf2) {
            bitmap      = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            auto first  = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            auto second = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            CompressSparseBitmap(first->mutable_data(),  arr1->GetValues<ui8>(1), length);
            CompressSparseBitmap(second->mutable_data(), arr2->GetValues<ui8>(1), length);

            Bitmap v1(first, 0, length);
            Bitmap v2(second, 0, length);

            Bitmap b(bitmap, 0, length);
            std::array<Bitmap, 1> out{b};

            //bitmap = first_false | second_false | (first_true & second_true);
            //bitmap = (b1 & ~v1)  | (b2 & ~v2)   | (b1 & v1 & b2 & v2)
            if (buf1 && buf2) {
                Bitmap b1(buf1, offset, length);
                Bitmap b2(buf2, offset, length);

                std::array<Bitmap, 4> in{b1, v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 4>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t b2 = in[2];
                    uint64_t v2 = in[3];
                    out->at(0) = (b1 & ~v1) | (b2 & ~v2) | (b1 & v1 & b2 & v2);
                });
            } else if (buf1) {
                Bitmap b1(buf1, offset, length);

                std::array<Bitmap, 3> in{b1, v1, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (b1 & ~v1) | (~v2) | (b1 & v1 & v2);
                });
            } else {
                Bitmap b2(buf2, offset, length);

                std::array<Bitmap, 3> in{v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t v1 = in[0];
                    uint64_t b2 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (~v1) | (b2 & ~v2) | (v1 & b2 & v2);
                });
            }
        }
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(length, &ctx.ArrowMemoryPool).ValueOrDie();
        AndSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr1->type, length, { bitmap, data }));
    }

    void RegisterDependencies() const final {
        DependsOn(First);
        DependsOn(Second);
    }

    IComputationNode* const First;
    IComputationNode* const Second;
    const bool ScalarResult;
};

class TBlockOrWrapper : public TMutableComputationNode<TBlockOrWrapper> {
public:
    TBlockOrWrapper(TComputationMutables& mutables, IComputationNode* first, IComputationNode* second, TBlockType::EShape shape)
        : TMutableComputationNode(mutables)
        , First(first)
        , Second(second)
        , ScalarResult(shape == TBlockType::EShape::Scalar)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto first = First->GetValue(ctx);
        const auto& firstDatum = TArrowBlock::From(first).GetDatum();

        if (IsAllEqualsTo(firstDatum, true)) {
            // true OR ... = true
            if (ScalarResult == firstDatum.is_scalar()) {
                return first.Release();
            }
            Y_VERIFY(firstDatum.is_scalar());
            // need length
            auto second = Second->GetValue(ctx);
            const auto& secondDatum = TArrowBlock::From(second).GetDatum();
            return MakeTrueArray(ctx, secondDatum.length());
        }

        auto second = Second->GetValue(ctx);
        const auto& secondDatum = TArrowBlock::From(second).GetDatum();

        if (IsAllEqualsTo(secondDatum, true)) {
            // ... OR true = true
            if (ScalarResult == secondDatum.is_scalar()) {
                return second.Release();
            }
            Y_VERIFY(secondDatum.is_scalar());
            return MakeTrueArray(ctx, firstDatum.length());
        }

        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            bool first_true =  firstDatum.scalar()->is_valid && (firstDatum.scalar_as<arrow::UInt8Scalar>().value & 1u != 0);
            bool first_false = firstDatum.scalar()->is_valid && (firstDatum.scalar_as<arrow::UInt8Scalar>().value & 1u == 0);

            bool second_true =  secondDatum.scalar()->is_valid && (secondDatum.scalar_as<arrow::UInt8Scalar>().value & 1u != 0);
            bool second_false = secondDatum.scalar()->is_valid && (secondDatum.scalar_as<arrow::UInt8Scalar>().value & 1u == 0);

            auto result = std::make_shared<arrow::UInt8Scalar>(ui8(first_true || second_true));
            result->is_valid = first_true || second_true || (first_false && second_false);

            return ctx.HolderFactory.CreateArrowBlock(arrow::Datum(result));
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow::UInt8Scalar>().value;
            bool valid = firstDatum.scalar()->is_valid;
            return CalcScalarArray(ctx, value, valid, secondDatum.array());
        }

        if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow::UInt8Scalar>().value;
            bool valid = secondDatum.scalar()->is_valid;
            return CalcScalarArray(ctx, value, valid, firstDatum.array());
        }

        return CalcArrayArray(ctx, firstDatum.array(), secondDatum.array());
    }

private:
    NUdf::TUnboxedValuePod MakeTrueArray(TComputationContext& ctx, int64_t len) const {
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(len, &ctx.ArrowMemoryPool).ValueOrDie();
        std::memset(data->mutable_data(), 1, len);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arrow::uint8(), len, { std::shared_ptr<arrow::Buffer>{}, data }));
    }

    NUdf::TUnboxedValuePod CalcScalarArray(TComputationContext& ctx, ui8 value, bool valid, const std::shared_ptr<arrow::ArrayData>& arr) const {
        bool first_true = valid && value;
        bool first_false = valid && !value;

        if (first_true) {
            return MakeTrueArray(ctx, arr->length);
        }

        if (first_false) {
            return ctx.HolderFactory.CreateArrowBlock(arr);
        }

        // scalar is null -> result is valid _only_ if arr[i] == true
        //bitmap = bitmap and data[i]
        std::shared_ptr<arrow::Buffer> bitmap = arrow::AllocateBitmap(arr->length, &ctx.ArrowMemoryPool).ValueOrDie();
        CompressSparseBitmap(bitmap->mutable_data(), arr->GetValues<ui8>(1), arr->length);
        if (arr->buffers[0]) {
            bitmap = arrow::internal::BitmapAnd(&ctx.ArrowMemoryPool, arr->GetValues<ui8>(0, 0), arr->offset, bitmap->data(), 0, arr->length, 0).ValueOrDie();
        }
        std::shared_ptr<arrow::Buffer> data = CopySparseBitmap(&ctx.ArrowMemoryPool, arr->buffers[1], arr->offset, arr->length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr->type, arr->length, { bitmap, data }));
    }


    NUdf::TUnboxedValuePod CalcArrayArray(TComputationContext& ctx, const std::shared_ptr<arrow::ArrayData>& arr1,
                                          const std::shared_ptr<arrow::ArrayData>& arr2) const
    {
        Y_VERIFY(arr1->offset == arr2->offset);
        Y_VERIFY(arr1->length == arr2->length);
        auto buf1 = arr1->buffers[0];
        auto buf2 = arr2->buffers[0];
        const int64_t offset = arr1->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow::Buffer> bitmap;
        if (buf1 || buf2) {
            bitmap      = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            auto first  = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            auto second = arrow::AllocateBitmap(length, &ctx.ArrowMemoryPool).ValueOrDie();
            CompressSparseBitmap(first->mutable_data(),  arr1->GetValues<ui8>(1), length);
            CompressSparseBitmap(second->mutable_data(), arr2->GetValues<ui8>(1), length);

            Bitmap v1(first, 0, length);
            Bitmap v2(second, 0, length);

            Bitmap b(bitmap, 0, length);
            std::array<Bitmap, 1> out{b};

            //bitmap = first_true | second_true | (first_false & second_false);
            //bitmap = (b1 & v1)  | (b2 & v2)   | (b1 & ~v1 & b2 & ~v2)
            if (buf1 && buf2) {
                Bitmap b1(buf1, offset, length);
                Bitmap b2(buf2, offset, length);

                std::array<Bitmap, 4> in{b1, v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 4>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t b2 = in[2];
                    uint64_t v2 = in[3];
                    out->at(0) = (b1 & v1) | (b2 & v2) | (b1 & ~v1 & b2 & ~v2);
                });
            } else if (buf1) {
                Bitmap b1(buf1, offset, length);

                std::array<Bitmap, 3> in{b1, v1, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (b1 & v1) | v2 | (b1 & ~v1 & ~v2);
                });
            } else {
                Bitmap b2(buf2, offset, length);

                std::array<Bitmap, 3> in{v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t v1 = in[0];
                    uint64_t b2 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = v1 | (b2 & v2) | (~v1 & b2 & ~v2);
                });
            }
        }
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(length, &ctx.ArrowMemoryPool).ValueOrDie();
        OrSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr1->type, length, { bitmap, data }));
    }

    void RegisterDependencies() const final {
        DependsOn(First);
        DependsOn(Second);
    }

    IComputationNode* const First;
    IComputationNode* const Second;
    const bool ScalarResult;
};

class TBlockXorWrapper : public TMutableComputationNode<TBlockXorWrapper> {
public:
    TBlockXorWrapper(TComputationMutables& mutables, IComputationNode* first, IComputationNode* second, TBlockType::EShape shape)
        : TMutableComputationNode(mutables)
        , First(first)
        , Second(second)
        , ScalarResult(shape == TBlockType::EShape::Scalar)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto first = First->GetValue(ctx);
        const auto& firstDatum = TArrowBlock::From(first).GetDatum();

        if (firstDatum.null_count() == firstDatum.length()) {
            if (ScalarResult == firstDatum.is_scalar()) {
                return first.Release();
            }
            Y_VERIFY(firstDatum.is_scalar());
            // need length
            auto second = Second->GetValue(ctx);
            const auto& secondDatum = TArrowBlock::From(second).GetDatum();
            return MakeNullArray(ctx, secondDatum.length());
        }

        auto second = Second->GetValue(ctx);
        const auto& secondDatum = TArrowBlock::From(second).GetDatum();

        if (secondDatum.null_count() == secondDatum.length()) {
            return (ScalarResult == secondDatum.is_scalar()) ? second.Release() : MakeNullArray(ctx, firstDatum.length());
        }

        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            Y_VERIFY(firstDatum.scalar()->is_valid);
            Y_VERIFY(secondDatum.scalar()->is_valid);
            ui8 result = firstDatum.scalar_as<arrow::UInt8Scalar>().value ^ secondDatum.scalar_as<arrow::UInt8Scalar>().value;
            result &= 1u;
            return ctx.HolderFactory.CreateArrowBlock(arrow::Datum(result));
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow::UInt8Scalar>().value;
            return CalcScalarArray(ctx, value, secondDatum.array());
        }

        if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow::UInt8Scalar>().value;
            return CalcScalarArray(ctx, value, firstDatum.array());
        }

        return CalcArrayArray(ctx, firstDatum.array(), secondDatum.array());
    }

private:
    NUdf::TUnboxedValuePod MakeNullArray(TComputationContext& ctx, int64_t len) const {
        std::shared_ptr<arrow::Array> arr = arrow::MakeArrayOfNull(arrow::uint8(), len, &ctx.ArrowMemoryPool).ValueOrDie();
        return ctx.HolderFactory.CreateArrowBlock(arr->data());
    }

    NUdf::TUnboxedValuePod CalcScalarArray(TComputationContext& ctx, ui8 value, const std::shared_ptr<arrow::ArrayData>& arr) const {
        std::shared_ptr<arrow::Buffer> bitmap = CopyBitmap(&ctx.ArrowMemoryPool, arr->buffers[0], arr->offset, arr->length);
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(arr->length, &ctx.ArrowMemoryPool).ValueOrDie();
        XorSparseBitmapScalar(data->mutable_data(), value, arr->GetValues<ui8>(1), arr->length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr->type, arr->length, { bitmap, data }));
    }

    NUdf::TUnboxedValuePod CalcArrayArray(TComputationContext& ctx, const std::shared_ptr<arrow::ArrayData>& arr1,
        const std::shared_ptr<arrow::ArrayData>& arr2) const
    {
        Y_VERIFY(arr1->offset == arr2->offset);
        Y_VERIFY(arr1->length == arr2->length);
        auto b1 = arr1->buffers[0];
        auto b2 = arr2->buffers[0];
        const int64_t offset = arr1->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow::Buffer> bitmap;
        if (b1 && b2) {
            bitmap = arrow::internal::BitmapAnd(&ctx.ArrowMemoryPool, b1->data(), offset, b2->data(), offset, length, 0).ValueOrDie();
        } else {
            bitmap = CopyBitmap(&ctx.ArrowMemoryPool, b1 ? b1 : b2, offset, length);
        }
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(length, &ctx.ArrowMemoryPool).ValueOrDie();
        XorSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return ctx.HolderFactory.CreateArrowBlock(arrow::ArrayData::Make(arr1->type, length, { bitmap, data }));
    }

    void RegisterDependencies() const final {
        DependsOn(First);
        DependsOn(Second);
    }

    IComputationNode* const First;
    IComputationNode* const Second;
    const bool ScalarResult;
};

class TBlockNotWrapper : public TMutableComputationNode<TBlockNotWrapper> {
friend class TArrowNode;
public:
    class TArrowNode : public IArrowKernelComputationNode {
    public:
        TArrowNode(const TBlockNotWrapper* parent)
            : Parent_(parent)
            , ArgsValuesDescr_({arrow::uint8()})
            , Kernel_({arrow::uint8()}, arrow::uint8(), [parent](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                *res = parent->CalculateImpl(MakeDatumProvider(batch.values[0]), *ctx->memory_pool());
                return arrow::Status::OK();
            })
        {
            Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
            Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
        }

        TStringBuf GetKernelName() const final {
            return "Not";
        }

        const arrow::compute::ScalarKernel& GetArrowKernel() const {
            return Kernel_;
        }

        const std::vector<arrow::ValueDescr>& GetArgsDesc() const {
            return ArgsValuesDescr_;
        }

        const IComputationNode* GetArgument(ui32 index) const {
            switch (index) {
            case 0:
                return Parent_->Value;
            default:
                throw yexception() << "Bad argument index";
            }
        }

    private:
        const TBlockNotWrapper* Parent_;
        const std::vector<arrow::ValueDescr> ArgsValuesDescr_;
        arrow::compute::ScalarKernel Kernel_;
    };

public:
    TBlockNotWrapper(TComputationMutables& mutables, IComputationNode* value)
        : TMutableComputationNode(mutables)
        , Value(value)
    {
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        Y_UNUSED(ctx);
        return std::make_unique<TArrowNode>(this);
    }

    arrow::Datum CalculateImpl(const TDatumProvider& valueProv, arrow::MemoryPool& memoryPool) const {
        auto datum = valueProv();
        if (datum.null_count() == datum.length()) {
            return datum;
        }

        if (datum.is_scalar()) {
            Y_VERIFY(datum.scalar()->is_valid);
            ui8 negated = (~datum.scalar_as<arrow::UInt8Scalar>().value) & 1u;
            return arrow::Datum(negated);
        }

        auto arr = datum.array();
        std::shared_ptr<arrow::Buffer> bitmap = CopyBitmap(&memoryPool, arr->buffers[0], arr->offset, arr->length);
        std::shared_ptr<arrow::Buffer> data = arrow::AllocateBuffer(arr->length, &memoryPool).ValueOrDie();
        NegateSparseBitmap(data->mutable_data(), arr->GetValues<ui8>(1), arr->length);
        return arrow::ArrayData::Make(arr->type, arr->length, { bitmap, data });
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateArrowBlock(CalculateImpl(MakeDatumProvider(Value, ctx), ctx.ArrowMemoryPool));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Value);
    }

    IComputationNode* const Value;
};

IComputationNode* WrapBlockLogical(std::string_view name, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto firstType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto secondType = AS_TYPE(TBlockType, callable.GetInput(1).GetStaticType());

    bool isOpt1, isOpt2;
    MKQL_ENSURE(UnpackOptionalData(firstType->GetItemType(), isOpt1)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");
    MKQL_ENSURE(UnpackOptionalData(secondType->GetItemType(), isOpt2)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");

    TBlockType::EShape shape = GetResultShape({firstType, secondType});

    auto first  = LocateNode(ctx.NodeLocator, callable, 0);
    auto second = LocateNode(ctx.NodeLocator, callable, 1);

    if (name == "and") {
        return new TBlockAndWrapper(ctx.Mutables, first, second, shape);
    }
    if (name == "or") {
        return new TBlockOrWrapper(ctx.Mutables, first, second, shape);
    }
    return new TBlockXorWrapper(ctx.Mutables, first, second, shape);
}


} // namespace

IComputationNode* WrapBlockAnd(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("and", callable, ctx);
}

IComputationNode* WrapBlockOr(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("or", callable, ctx);
}

IComputationNode* WrapBlockXor(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("xor", callable, ctx);
}

IComputationNode* WrapBlockNot(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    auto dataType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    bool isOpt;
    MKQL_ENSURE(UnpackOptionalData(dataType->GetItemType(), isOpt)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");

    return new TBlockNotWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}


}
}
