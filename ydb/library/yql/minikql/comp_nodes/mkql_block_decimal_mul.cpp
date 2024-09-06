#include "mkql_block_decimal_mul.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TDecimalMulBlockExec {
    const NYql::NDecimal::TInt128 Bound;
    const NYql::NDecimal::TInt128 Divider;

    TDecimalMulBlockExec(
        NYql::NDecimal::TInt128 bound,
        NYql::NDecimal::TInt128 divider)
        : Bound(bound)
        , Divider(divider)
    { }

    NYql::NDecimal::TInt128 Do(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        const auto mul = Divider > 1 ?
            NYql::NDecimal::MulAndDivNormalDivider(left, right, Divider):
            NYql::NDecimal::Mul(left, right);

        if (mul > -Bound && mul < +Bound)
            return mul;

        return NYql::NDecimal::IsNan(mul) ? NYql::NDecimal::Nan() : (mul > 0 ? +NYql::NDecimal::Inf() : -NYql::NDecimal::Inf());
    }

    void ArrayArrayCore(
        const NYql::NDecimal::TInt128* val1Ptr,
        const ui8* valid1,
        const NYql::NDecimal::TInt128* val2Ptr,
        const ui8* valid2,
        NYql::NDecimal::TInt128* resPtr,
        ui8* resValid,
        int64_t length,
        int64_t offset1,
        int64_t offset2) const
    {
        val1Ptr += offset1;
        val2Ptr += offset2;
        for (int64_t i = 0; i < length; ++i, ++val1Ptr, ++val2Ptr, ++resPtr) {
            if ((!valid1 || arrow::BitUtil::GetBit(valid1, i + offset1)) &&
                (!valid2 || arrow::BitUtil::GetBit(valid2, i + offset2))) {
                *resPtr = Do(*val1Ptr, *val2Ptr);
                arrow::BitUtil::SetBit(resValid, i);
            }
        }
    }

    arrow::Status ExecArrayArray(const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr1 = *arg1.array();
        const auto& arr2 = *arg2.array();
        MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
        auto length = arr1.length;
        const auto val1Ptr = reinterpret_cast<const NYql::NDecimal::TInt128*>(arr1.buffers[1]->data());
        const auto nullCount1 = arr1.GetNullCount();
        const auto valid1 = (nullCount1 == 0) ? nullptr : arr1.GetValues<uint8_t>(0);
        const auto val2Ptr = reinterpret_cast<const NYql::NDecimal::TInt128*>(arr2.buffers[1]->data());
        const auto nullCount2 = arr2.GetNullCount();
        const auto valid2 = (nullCount2 == 0) ? nullptr : arr2.GetValues<uint8_t>(0);
        auto& resArr = *res->array();
        auto resPtr = reinterpret_cast<NYql::NDecimal::TInt128*>(resArr.buffers[1]->mutable_data());
        auto resValid = res->array()->GetMutableValues<uint8_t>(0);

        ArrayArrayCore(val1Ptr, valid1, val2Ptr, valid2, resPtr, resValid, length, arr1.offset, arr2.offset);

        return arrow::Status::OK();
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        Y_UNUSED(ctx);
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (arg1.is_scalar()) {
            if (arg2.is_scalar()) {
                // scalar-scalar
            } else {
                // scalar-vector
            }
        } else {
            if (arg2.is_scalar()) {
                // vector-scalar
            } else {
                // vector-vector
                return ExecArrayArray(batch, res);
            }
        }

        return arrow::Status::OK();
    }
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockMulKernel(const TVector<TType*>& argTypes, TType* resultType) {
    MKQL_ENSURE(argTypes.size() == 2, "Require 2 arguments");
    MKQL_ENSURE(argTypes[0]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(argTypes[1]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(resultType->GetKind() == TType::EKind::Block, "Require block");

    bool isOptional = false;
    auto dataType1 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[0])->GetItemType(), isOptional);
    auto dataType2 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[1])->GetItemType(), isOptional);
    auto dataResultType = UnpackOptionalData(static_cast<TBlockType*>(resultType)->GetItemType(), isOptional);

    MKQL_ENSURE(*dataType1->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    MKQL_ENSURE(*dataType2->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    MKQL_ENSURE(*dataResultType->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");

    auto decimalType1 = static_cast<TDataDecimalType*>(dataType1);
    auto decimalType2 = static_cast<TDataDecimalType*>(dataType2);
    auto decimalResultType = static_cast<TDataDecimalType*>(dataResultType);

    MKQL_ENSURE(decimalType1->GetParams() == decimalType2->GetParams(), "Require same precision/scale");
    MKQL_ENSURE(decimalType1->GetParams() == decimalResultType->GetParams(), "Require same precision/scale");

    auto [precision, scale] = decimalType1->GetParams();
    MKQL_ENSURE(precision >= 1&& precision <= 35, TStringBuilder() << "Wrong precision: " << (int)precision);

    auto exec = std::make_shared<TDecimalMulBlockExec>(NYql::NDecimal::GetDivider(precision), NYql::NDecimal::GetDivider(scale));

    auto k = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType), 
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });
    k->null_handling = arrow::compute::NullHandling::INTERSECTION;
    return k;
}

}

IComputationNode* WrapBlockDecimalMul(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto first = callable.GetInput(0);
    auto second = callable.GetInput(1);

    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    auto firstCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto secondCompute = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = { firstCompute, secondCompute };
    TVector<TType*> argsTypes = { firstType, secondType };

    std::shared_ptr<arrow::compute::ScalarKernel> kernel = MakeBlockMulKernel(argsTypes, callable.GetType()->GetReturnType());
    return new TBlockFuncNode(ctx.Mutables, "DecimalMul", std::move(argsNodes), argsTypes, *kernel, kernel);
}

}
}
