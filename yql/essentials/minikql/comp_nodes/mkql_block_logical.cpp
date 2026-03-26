#include "mkql_block_logical.h"

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/mkql_bit_utils.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <arrow/util/bitmap.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/bit_util.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using arrow20::internal::Bitmap;

std::shared_ptr<arrow20::Buffer> CopyBitmap(arrow20::MemoryPool* pool, const std::shared_ptr<arrow20::Buffer>& bitmap, int64_t offset, int64_t len) {
    std::shared_ptr<arrow20::Buffer> result = bitmap;
    if (bitmap && offset != 0) {
        result = ARROW_RESULT(arrow20::AllocateBitmap(len, pool));
        arrow20::internal::CopyBitmap(bitmap->data(), offset, len, result->mutable_data(), 0);
    }
    return result;
}

std::shared_ptr<arrow20::Buffer> CopySparseBitmap(arrow20::MemoryPool* pool, const std::shared_ptr<arrow20::Buffer>& bitmap, int64_t offset, int64_t len) {
    std::shared_ptr<arrow20::Buffer> result = bitmap;
    if (bitmap && offset != 0) {
        result = ARROW_RESULT(arrow20::AllocateBuffer(len, pool));
        std::memcpy(result->mutable_data(), bitmap->data() + offset, len);
    }
    return result;
}

arrow20::Datum MakeNullArray(arrow20::MemoryPool* pool, int64_t len) {
    std::shared_ptr<arrow20::Array> arr = ARROW_RESULT(arrow20::MakeArrayOfNull(arrow20::uint8(), len, pool));
    return arr;
}

bool IsAllEqualsTo(const arrow20::Datum& datum, bool value) {
    if (datum.null_count() != 0) {
        return false;
    }
    if (datum.is_scalar()) {
        return (datum.scalar_as<arrow20::UInt8Scalar>().value & 1u) == value;
    }
    size_t len = datum.array()->length;
    size_t popCnt = GetSparseBitmapPopCount(datum.array()->GetValues<ui8>(1), len);
    return popCnt == (value ? len : 0);
}

class TAndBlockExec {
public:
    arrow20::Status Exec(arrow20::compute::KernelContext* ctx, const arrow20::compute::ExecBatch& batch, arrow20::Datum* res) const {
        auto firstDatum = batch.values[0];
        auto secondDatum = batch.values[1];
        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            *res = CalcScalarScalar(firstDatum, secondDatum);
            return arrow20::Status::OK();
        }
        if (IsAllEqualsTo(firstDatum, false)) {
            // false AND ... = false
            if (firstDatum.is_array()) {
                *res = firstDatum;
            } else {
                // need length
                *res = MakeFalseArray(ctx->memory_pool(), secondDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (IsAllEqualsTo(secondDatum, false)) {
            // ... AND false = false
            if (secondDatum.is_array()) {
                *res = secondDatum;
            } else {
                *res = MakeFalseArray(ctx->memory_pool(), firstDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow20::UInt8Scalar>().value & 1u;
            bool valid = firstDatum.scalar()->is_valid;
            *res = CalcScalarArray(ctx->memory_pool(), value, valid, secondDatum.array());
        } else if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow20::UInt8Scalar>().value & 1u;
            bool valid = secondDatum.scalar()->is_valid;
            *res = CalcScalarArray(ctx->memory_pool(), value, valid, firstDatum.array());
        } else {
            *res = CalcArrayArray(ctx->memory_pool(), firstDatum.array(), secondDatum.array());
        }

        return arrow20::Status::OK();
    }

private:
    arrow20::Datum CalcScalarScalar(const arrow20::Datum& firstDatum, const arrow20::Datum& secondDatum) const {
        const auto& first = firstDatum.scalar_as<arrow20::UInt8Scalar>();
        const auto& second = secondDatum.scalar_as<arrow20::UInt8Scalar>();

        if (first.is_valid && second.is_valid) {
            bool result = bool((first.value & second.value) & 1u);
            return MakeScalarDatum(result);
        }

        if (!first.is_valid && !second.is_valid) {
            return firstDatum;
        }

        if (!first.is_valid) {
            // null and true -> null
            // null and false -> false
            return second.value ? firstDatum : secondDatum;
        } else {
            // true and null -> null
            // false and null -> false
            return first.value ? secondDatum : firstDatum;
        }
    }

    arrow20::Datum CalcScalarArray(arrow20::MemoryPool* pool, ui8 value, bool valid, const std::shared_ptr<arrow20::ArrayData>& arr) const {
        bool first_true = valid && value;
        bool first_false = valid && !value;

        if (first_false) {
            return MakeFalseArray(pool, arr->length);
        }

        if (first_true) {
            return arr;
        }

        // scalar is null -> result is valid _only_ if arr[i] == false
        // bitmap = bitmap and not data[i]
        std::shared_ptr<arrow20::Buffer> bitmap = ARROW_RESULT(arrow20::AllocateBitmap(arr->length, pool));
        CompressSparseBitmapNegate(bitmap->mutable_data(), arr->GetValues<ui8>(1), arr->length);
        if (arr->buffers[0]) {
            bitmap = ARROW_RESULT(arrow20::internal::BitmapAnd(pool, arr->GetValues<ui8>(0, 0), arr->offset, bitmap->data(), 0, arr->length, 0));
        }
        std::shared_ptr<arrow20::Buffer> data = CopySparseBitmap(pool, arr->buffers[1], arr->offset, arr->length);
        return arrow20::ArrayData::Make(arr->type, arr->length, {bitmap, data});
    }

    arrow20::Datum CalcArrayArray(arrow20::MemoryPool* pool, const std::shared_ptr<arrow20::ArrayData>& arr1,
                                const std::shared_ptr<arrow20::ArrayData>& arr2) const {
        Y_ABORT_UNLESS(arr1->length == arr2->length);
        auto buf1 = arr1->buffers[0];
        auto buf2 = arr2->buffers[0];
        const int64_t offset1 = arr1->offset;
        const int64_t offset2 = arr2->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow20::Buffer> bitmap;
        if (buf1 || buf2) {
            bitmap = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            auto first = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            auto second = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            CompressSparseBitmap(first->mutable_data(), arr1->GetValues<ui8>(1), length);
            CompressSparseBitmap(second->mutable_data(), arr2->GetValues<ui8>(1), length);

            Bitmap v1(first, 0, length);
            Bitmap v2(second, 0, length);

            Bitmap b(bitmap, 0, length);
            std::array<Bitmap, 1> out{b};

            // bitmap = first_false | second_false | (first_true & second_true);
            // bitmap = (b1 & ~v1)  | (b2 & ~v2)   | (b1 & v1 & b2 & v2)
            if (buf1 && buf2) {
                Bitmap b1(buf1, offset1, length);
                Bitmap b2(buf2, offset2, length);

                std::array<Bitmap, 4> in{b1, v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 4>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t b2 = in[2];
                    uint64_t v2 = in[3];
                    out->at(0) = (b1 & ~v1) | (b2 & ~v2) | (b1 & v1 & b2 & v2);
                });
            } else if (buf1) {
                Bitmap b1(buf1, offset1, length);

                std::array<Bitmap, 3> in{b1, v1, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (b1 & ~v1) | (~v2) | (b1 & v1 & v2);
                });
            } else {
                Bitmap b2(buf2, offset2, length);

                std::array<Bitmap, 3> in{v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t v1 = in[0];
                    uint64_t b2 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (~v1) | (b2 & ~v2) | (v1 & b2 & v2);
                });
            }
        }
        std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(length, pool));
        AndSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return arrow20::ArrayData::Make(arr1->type, length, {bitmap, data});
    }
};

class TOrBlockExec {
public:
    arrow20::Status Exec(arrow20::compute::KernelContext* ctx, const arrow20::compute::ExecBatch& batch, arrow20::Datum* res) const {
        auto firstDatum = batch.values[0];
        auto secondDatum = batch.values[1];
        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            *res = CalcScalarScalar(firstDatum, secondDatum);
            return arrow20::Status::OK();
        }
        if (IsAllEqualsTo(firstDatum, true)) {
            // true OR ... = true
            if (firstDatum.is_array()) {
                *res = firstDatum;
            } else {
                // need length
                *res = MakeTrueArray(ctx->memory_pool(), secondDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (IsAllEqualsTo(secondDatum, true)) {
            // ... OR true = true
            if (secondDatum.is_array()) {
                *res = secondDatum;
            } else {
                *res = MakeTrueArray(ctx->memory_pool(), firstDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow20::UInt8Scalar>().value;
            bool valid = firstDatum.scalar()->is_valid;
            *res = CalcScalarArray(ctx->memory_pool(), value, valid, secondDatum.array());
        } else if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow20::UInt8Scalar>().value;
            bool valid = secondDatum.scalar()->is_valid;
            *res = CalcScalarArray(ctx->memory_pool(), value, valid, firstDatum.array());
        } else {
            *res = CalcArrayArray(ctx->memory_pool(), firstDatum.array(), secondDatum.array());
        }

        return arrow20::Status::OK();
    }

private:
    arrow20::Datum CalcScalarScalar(const arrow20::Datum& firstDatum, const arrow20::Datum& secondDatum) const {
        const auto& first = firstDatum.scalar_as<arrow20::UInt8Scalar>();
        const auto& second = secondDatum.scalar_as<arrow20::UInt8Scalar>();

        if (first.is_valid && second.is_valid) {
            bool result = bool((first.value | second.value) & 1u);
            return MakeScalarDatum(result);
        }

        if (!first.is_valid && !second.is_valid) {
            return firstDatum;
        }

        if (!first.is_valid) {
            // null or true -> true
            // null or false -> null
            return second.value ? secondDatum : firstDatum;
        } else {
            // true or null -> true
            // false or null -> null
            return first.value ? firstDatum : secondDatum;
        }
    }

    arrow20::Datum CalcScalarArray(arrow20::MemoryPool* pool, ui8 value, bool valid, const std::shared_ptr<arrow20::ArrayData>& arr) const {
        bool first_true = valid && value;
        bool first_false = valid && !value;

        if (first_true) {
            return MakeTrueArray(pool, arr->length);
        }

        if (first_false) {
            return arr;
        }

        // scalar is null -> result is valid _only_ if arr[i] == true
        // bitmap = bitmap and data[i]
        std::shared_ptr<arrow20::Buffer> bitmap = ARROW_RESULT(arrow20::AllocateBitmap(arr->length, pool));
        CompressSparseBitmap(bitmap->mutable_data(), arr->GetValues<ui8>(1), arr->length);
        if (arr->buffers[0]) {
            bitmap = ARROW_RESULT(arrow20::internal::BitmapAnd(pool, arr->GetValues<ui8>(0, 0), arr->offset, bitmap->data(), 0, arr->length, 0));
        }
        std::shared_ptr<arrow20::Buffer> data = CopySparseBitmap(pool, arr->buffers[1], arr->offset, arr->length);
        return arrow20::ArrayData::Make(arr->type, arr->length, {bitmap, data});
    }

    arrow20::Datum CalcArrayArray(arrow20::MemoryPool* pool, const std::shared_ptr<arrow20::ArrayData>& arr1,
                                const std::shared_ptr<arrow20::ArrayData>& arr2) const {
        Y_ABORT_UNLESS(arr1->length == arr2->length);
        auto buf1 = arr1->buffers[0];
        auto buf2 = arr2->buffers[0];
        const int64_t offset1 = arr1->offset;
        const int64_t offset2 = arr2->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow20::Buffer> bitmap;
        if (buf1 || buf2) {
            bitmap = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            auto first = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            auto second = ARROW_RESULT(arrow20::AllocateBitmap(length, pool));
            CompressSparseBitmap(first->mutable_data(), arr1->GetValues<ui8>(1), length);
            CompressSparseBitmap(second->mutable_data(), arr2->GetValues<ui8>(1), length);

            Bitmap v1(first, 0, length);
            Bitmap v2(second, 0, length);

            Bitmap b(bitmap, 0, length);
            std::array<Bitmap, 1> out{b};

            // bitmap = first_true | second_true | (first_false & second_false);
            // bitmap = (b1 & v1)  | (b2 & v2)   | (b1 & ~v1 & b2 & ~v2)
            if (buf1 && buf2) {
                Bitmap b1(buf1, offset1, length);
                Bitmap b2(buf2, offset2, length);

                std::array<Bitmap, 4> in{b1, v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 4>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t b2 = in[2];
                    uint64_t v2 = in[3];
                    out->at(0) = (b1 & v1) | (b2 & v2) | (b1 & ~v1 & b2 & ~v2);
                });
            } else if (buf1) {
                Bitmap b1(buf1, offset1, length);

                std::array<Bitmap, 3> in{b1, v1, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t b1 = in[0];
                    uint64_t v1 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = (b1 & v1) | v2 | (b1 & ~v1 & ~v2);
                });
            } else {
                Bitmap b2(buf2, offset2, length);

                std::array<Bitmap, 3> in{v1, b2, v2};
                Bitmap::VisitWordsAndWrite(in, &out, [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 1>* out) {
                    uint64_t v1 = in[0];
                    uint64_t b2 = in[1];
                    uint64_t v2 = in[2];
                    out->at(0) = v1 | (b2 & v2) | (~v1 & b2 & ~v2);
                });
            }
        }
        std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(length, pool));
        OrSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return arrow20::ArrayData::Make(arr1->type, length, {bitmap, data});
    }
};

class TXorBlockExec {
public:
    arrow20::Status Exec(arrow20::compute::KernelContext* ctx, const arrow20::compute::ExecBatch& batch, arrow20::Datum* res) const {
        auto firstDatum = batch.values[0];
        auto secondDatum = batch.values[1];
        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            *res = CalcScalarScalar(firstDatum, secondDatum);
            return arrow20::Status::OK();
        }
        if (firstDatum.null_count() == firstDatum.length()) {
            if (firstDatum.is_array()) {
                *res = firstDatum;
            } else {
                *res = MakeNullArray(ctx->memory_pool(), secondDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (secondDatum.null_count() == secondDatum.length()) {
            if (secondDatum.is_array()) {
                *res = secondDatum;
            } else {
                *res = MakeNullArray(ctx->memory_pool(), firstDatum.length());
            }

            return arrow20::Status::OK();
        }

        if (firstDatum.is_scalar()) {
            ui8 value = firstDatum.scalar_as<arrow20::UInt8Scalar>().value;
            *res = CalcScalarArray(ctx->memory_pool(), value, secondDatum.array());
        } else if (secondDatum.is_scalar()) {
            ui8 value = secondDatum.scalar_as<arrow20::UInt8Scalar>().value;
            *res = CalcScalarArray(ctx->memory_pool(), value, firstDatum.array());
        } else {
            *res = CalcArrayArray(ctx->memory_pool(), firstDatum.array(), secondDatum.array());
        }

        return arrow20::Status::OK();
    }

private:
    arrow20::Datum CalcScalarScalar(const arrow20::Datum& firstDatum, const arrow20::Datum& secondDatum) const {
        const auto& first = firstDatum.scalar_as<arrow20::UInt8Scalar>();
        const auto& second = secondDatum.scalar_as<arrow20::UInt8Scalar>();

        if (first.is_valid && second.is_valid) {
            bool result = bool((first.value ^ second.value) & 1u);
            return MakeScalarDatum(result);
        }

        return first.is_valid ? secondDatum : firstDatum;
    }

    arrow20::Datum CalcScalarArray(arrow20::MemoryPool* pool, ui8 value, const std::shared_ptr<arrow20::ArrayData>& arr) const {
        std::shared_ptr<arrow20::Buffer> bitmap = CopyBitmap(pool, arr->buffers[0], arr->offset, arr->length);
        std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(arr->length, pool));
        XorSparseBitmapScalar(data->mutable_data(), value, arr->GetValues<ui8>(1), arr->length);
        return arrow20::ArrayData::Make(arr->type, arr->length, {bitmap, data});
    }

    arrow20::Datum CalcArrayArray(arrow20::MemoryPool* pool, const std::shared_ptr<arrow20::ArrayData>& arr1,
                                const std::shared_ptr<arrow20::ArrayData>& arr2) const {
        Y_ABORT_UNLESS(arr1->length == arr2->length);
        auto b1 = arr1->buffers[0];
        auto b2 = arr2->buffers[0];
        const int64_t offset1 = arr1->offset;
        const int64_t offset2 = arr2->offset;
        const int64_t length = arr1->length;

        std::shared_ptr<arrow20::Buffer> bitmap;
        if (b1 && b2) {
            bitmap = ARROW_RESULT(arrow20::internal::BitmapAnd(pool, b1->data(), offset1, b2->data(), offset2, length, 0));
        } else {
            bitmap = CopyBitmap(pool, b1 ? b1 : b2, b1 ? offset1 : offset2, length);
        }
        std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(length, pool));
        XorSparseBitmaps(data->mutable_data(), arr1->GetValues<ui8>(1), arr2->GetValues<ui8>(1), length);
        return arrow20::ArrayData::Make(arr1->type, length, {bitmap, data});
    }
};

class TNotBlockExec {
public:
    arrow20::Status Exec(arrow20::compute::KernelContext* ctx, const arrow20::compute::ExecBatch& batch, arrow20::Datum* res) const {
        const auto& input = batch.values[0];
        if (input.is_scalar()) {
            const auto& arg = input.scalar_as<arrow20::UInt8Scalar>();
            *res = arg.is_valid ? MakeScalarDatum(bool(~arg.value & 1u)) : input;
            return arrow20::Status::OK();
        }
        MKQL_ENSURE(input.is_array(), "Expected array");
        const auto& arr = *input.array();
        if (arr.GetNullCount() == arr.length) {
            *res = input;
        } else {
            auto bitmap = CopyBitmap(ctx->memory_pool(), arr.buffers[0], arr.offset, arr.length);
            std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(arr.length, ctx->memory_pool()));
            ;
            NegateSparseBitmap(data->mutable_data(), arr.GetValues<ui8>(1), arr.length);
            *res = arrow20::ArrayData::Make(arr.type, arr.length, {bitmap, data});
        }

        return arrow20::Status::OK();
    }
};

template <typename TExec>
std::shared_ptr<arrow20::compute::ScalarKernel> MakeKernel(const TVector<TType*>& argTypes, TType* resultType) {
    std::shared_ptr<arrow20::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TExec>();
    auto kernel = std::make_shared<arrow20::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
                                                                 [exec](arrow20::compute::KernelContext* ctx, const arrow20::compute::ExecBatch& batch, arrow20::Datum* res) {
                                                                     return exec->Exec(ctx, batch, res);
                                                                 });

    kernel->null_handling = arrow20::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

IComputationNode* WrapBlockLogical(std::string_view name, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto firstType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto secondType = AS_TYPE(TBlockType, callable.GetInput(1).GetStaticType());

    bool isOpt1, isOpt2;
    MKQL_ENSURE(UnpackOptionalData(firstType->GetItemType(), isOpt1)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");
    MKQL_ENSURE(UnpackOptionalData(secondType->GetItemType(), isOpt2)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");

    auto compute1 = LocateNode(ctx.NodeLocator, callable, 0);
    auto compute2 = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = {compute1, compute2};
    TVector<TType*> argsTypes = {callable.GetInput(0).GetStaticType(), callable.GetInput(1).GetStaticType()};

    std::shared_ptr<arrow20::compute::ScalarKernel> kernel;
    if (name == "And") {
        kernel = MakeKernel<TAndBlockExec>(argsTypes, callable.GetType()->GetReturnType());
    } else if (name == "Or") {
        kernel = MakeKernel<TOrBlockExec>(argsTypes, callable.GetType()->GetReturnType());
    } else {
        kernel = MakeKernel<TXorBlockExec>(argsTypes, callable.GetType()->GetReturnType());
    }

    return new TBlockFuncNode(ctx.Mutables, ToDatumValidateMode(ctx.ValidateMode), name, std::move(argsNodes), argsTypes, callable.GetType()->GetReturnType(), *kernel, kernel);
}

} // namespace

IComputationNode* WrapBlockAnd(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("And", callable, ctx);
}

IComputationNode* WrapBlockOr(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("Or", callable, ctx);
}

IComputationNode* WrapBlockXor(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockLogical("Xor", callable, ctx);
}

IComputationNode* WrapBlockNot(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    auto dataType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    bool isOpt;
    MKQL_ENSURE(UnpackOptionalData(dataType->GetItemType(), isOpt)->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Requires boolean args.");

    auto compute = LocateNode(ctx.NodeLocator, callable, 0);
    TComputationNodePtrVector argsNodes = {compute};
    TVector<TType*> argsTypes = {callable.GetInput(0).GetStaticType()};

    auto kernel = MakeKernel<TNotBlockExec>(argsTypes, argsTypes[0]);
    return new TBlockFuncNode(ctx.Mutables, ToDatumValidateMode(ctx.ValidateMode), "Not", std::move(argsNodes), argsTypes, callable.GetType()->GetReturnType(), *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
