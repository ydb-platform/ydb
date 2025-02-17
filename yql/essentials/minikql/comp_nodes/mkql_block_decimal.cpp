#include "mkql_block_decimal.h"

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename T, typename TRight>
struct TDecimalBlockExec {
    NYql::NDecimal::TInt128 Apply(NYql::NDecimal::TInt128 left, TRight right) const {
        return static_cast<const T*>(this)->Do(left, right);
    }

    template<typename U>
    const U* GetScalarValue(const arrow::Scalar& scalar) const {
        return reinterpret_cast<const U*>(GetPrimitiveScalarValuePtr(scalar));
    }
    
    template<>
    const NYql::NDecimal::TInt128* GetScalarValue<NYql::NDecimal::TInt128>(const arrow::Scalar& scalar) const {
        return reinterpret_cast<const NYql::NDecimal::TInt128*>(GetStringScalarValue(scalar).data());
    }
 
    void ArrayScalarCore(
        const NYql::NDecimal::TInt128* val1Ptr,
        const ui8* valid1,
        const TRight* val2Ptr,
        const ui8* valid2,
        NYql::NDecimal::TInt128* resPtr,
        ui8* resValid,
        int64_t length,
        int64_t offset1,
        int64_t offset2) const {
        val1Ptr += offset1;
        Y_UNUSED(valid2);
        Y_UNUSED(offset2);
        for (int64_t i = 0; i < length; ++i, ++val1Ptr, ++resPtr) {
            if (!valid1 || arrow::BitUtil::GetBit(valid1, i + offset1)) {
                *resPtr = Apply(*val1Ptr, *val2Ptr);
                arrow::BitUtil::SetBit(resValid, i);
            } else {
                arrow::BitUtil::ClearBit(resValid, i);
            }
        }
    }

    void ScalarArrayCore(
        const NYql::NDecimal::TInt128* val1Ptr,
        const ui8* valid1,
        const TRight* val2Ptr,
        const ui8* valid2,
        NYql::NDecimal::TInt128* resPtr,
        ui8* resValid,
        int64_t length,
        int64_t offset1,
        int64_t offset2) const {
        val2Ptr += offset2;
        Y_UNUSED(valid1);
        Y_UNUSED(offset1);
        for (int64_t i = 0; i < length; ++i, ++val2Ptr, ++resPtr) {
            if (!valid2 || arrow::BitUtil::GetBit(valid2, i + offset2)) {
                *resPtr = Apply(*val1Ptr, *val2Ptr);
                arrow::BitUtil::SetBit(resValid, i);
            } else {
                arrow::BitUtil::ClearBit(resValid, i);
            }
        }
    }

    void ArrayArrayCore(
        const NYql::NDecimal::TInt128* val1Ptr,
        const ui8* valid1,
        const TRight* val2Ptr,
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
                *resPtr = Apply(*val1Ptr, *val2Ptr);
                arrow::BitUtil::SetBit(resValid, i);
            } else {
                arrow::BitUtil::ClearBit(resValid, i);
            }
        }
    }

    arrow::Status ExecScalarScalar(arrow::compute::KernelContext* kernelCtx,
        const arrow::compute::ExecBatch& batch, arrow::Datum* res) const 
    {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
            *res = arrow::MakeNullScalar(GetPrimitiveDataType<NYql::NDecimal::TInt128>());
        } else {
            const auto val1Ptr = GetScalarValue<NYql::NDecimal::TInt128>(*arg1.scalar());
            const auto val2Ptr = GetScalarValue<TRight>(*arg2.scalar());
            std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(16, kernelCtx->memory_pool())));
            auto* mem = reinterpret_cast<NYql::NDecimal::TInt128*>(buffer->mutable_data());
            auto resDatum = arrow::Datum(std::make_shared<TPrimitiveDataType<NYql::NDecimal::TInt128>::TScalarResult>(buffer));
            *mem = Apply(*val1Ptr, *val2Ptr);
            *res = resDatum;
        }
    
        return arrow::Status::OK();
    }

    arrow::Status ExecScalarArray(const arrow::compute::ExecBatch& batch, arrow::Datum* res) const
    {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg1.scalar()->is_valid) {
            const auto val1Ptr = GetScalarValue<NYql::NDecimal::TInt128>(*arg1.scalar());
            const auto& arr2 = *arg2.array();
            auto length = arr2.length;
            const auto val2Ptr = reinterpret_cast<const TRight*>(arr2.buffers[1]->data());
            const auto nullCount2 = arr2.GetNullCount();
            const auto valid2 = (nullCount2 == 0) ? nullptr : arr2.GetValues<uint8_t>(0);
            auto resPtr = reinterpret_cast<NYql::NDecimal::TInt128*>(resArr.buffers[1]->mutable_data());
            auto resValid = res->array()->GetMutableValues<uint8_t>(0);
            ScalarArrayCore(val1Ptr, nullptr, val2Ptr, valid2, resPtr, resValid, length, 0, arr2.offset);
        } else {
            GetBitmap(resArr, 0).SetBitsTo(false);
        }
    
        return arrow::Status::OK();
    }

    arrow::Status ExecArrayScalar(const arrow::compute::ExecBatch& batch, arrow::Datum* res) const
    {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg2.scalar()->is_valid) {
            const auto& arr1 = *arg1.array();
            const auto val1Ptr = reinterpret_cast<const NYql::NDecimal::TInt128*>(arr1.buffers[1]->data());
            auto length = arr1.length;
            const auto nullCount1 = arr1.GetNullCount();
            const auto valid1 = (nullCount1 == 0) ? nullptr : arr1.GetValues<uint8_t>(0);
            const auto val2Ptr = GetScalarValue<TRight>(*arg2.scalar());
            auto resPtr = reinterpret_cast<NYql::NDecimal::TInt128*>(resArr.buffers[1]->mutable_data());
            auto resValid = res->array()->GetMutableValues<uint8_t>(0);
            ArrayScalarCore(val1Ptr, valid1, val2Ptr, nullptr, resPtr, resValid, length, arr1.offset, 0);
        } else {
            GetBitmap(resArr, 0).SetBitsTo(false);
        }

        return arrow::Status::OK();
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
        const auto val2Ptr = reinterpret_cast<const TRight*>(arr2.buffers[1]->data());
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
                return ExecScalarScalar(ctx, batch, res);
            } else {
                return ExecScalarArray(batch, res);
            }
        } else {
            if (arg2.is_scalar()) {
                return ExecArrayScalar(batch, res);
            } else {
                return ExecArrayArray(batch, res);
            }
        }

        return arrow::Status::OK();
    }
};

template<typename TRight>
struct TDecimalMulBlockExec: NYql::NDecimal::TDecimalMultiplicator<TRight>, TDecimalBlockExec<TDecimalMulBlockExec<TRight>, TRight> {
    TDecimalMulBlockExec(
        ui8 precision,
        ui8 scale)
        : NYql::NDecimal::TDecimalMultiplicator<TRight>(precision, scale)
    { }
};

template<typename TRight>
struct TDecimalDivBlockExec: NYql::NDecimal::TDecimalDivisor<TRight>, TDecimalBlockExec<TDecimalDivBlockExec<TRight>, TRight> {
    TDecimalDivBlockExec(
        ui8 precision,
        ui8 scale)
        : NYql::NDecimal::TDecimalDivisor<TRight>(precision, scale)
    { }
};

template<typename TRight>
struct TDecimalModBlockExec: NYql::NDecimal::TDecimalRemainder<TRight>, TDecimalBlockExec<TDecimalModBlockExec<TRight>, TRight> {
    TDecimalModBlockExec(
        ui8 precision,
        ui8 scale)
        : NYql::NDecimal::TDecimalRemainder<TRight>(precision, scale)
    { }
};

template<template <typename> class TExec>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockKernel(const TVector<TType*>& argTypes, TType* resultType) {
    MKQL_ENSURE(argTypes.size() == 2, "Require 2 arguments");
    MKQL_ENSURE(argTypes[0]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(argTypes[1]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(resultType->GetKind() == TType::EKind::Block, "Require block");

    bool isOptional = false;
    auto dataType1 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[0])->GetItemType(), isOptional);
    auto dataType2 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[1])->GetItemType(), isOptional);
    auto dataResultType = UnpackOptionalData(static_cast<TBlockType*>(resultType)->GetItemType(), isOptional);

    MKQL_ENSURE(*dataType1->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    MKQL_ENSURE(*dataResultType->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");

    auto decimalType1 = static_cast<TDataDecimalType*>(dataType1);
    auto decimalResultType = static_cast<TDataDecimalType*>(dataResultType);

    MKQL_ENSURE(decimalType1->GetParams() == decimalResultType->GetParams(), "Require same precision/scale");

    auto [precision, scale] = decimalType1->GetParams();
    MKQL_ENSURE(precision >= 1&& precision <= 35, TStringBuilder() << "Wrong precision: " << (int)precision);

    auto createKernel = [&](auto exec) {
        auto k = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType), 
            [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
        });
        k->null_handling = arrow::compute::NullHandling::INTERSECTION;
        return k;       
    };

    switch (dataType2->GetSchemeType()) {
    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        return createKernel(std::make_shared<TExec<NYql::NDecimal::TInt128>>(precision, scale));
    }
#define MAKE_PRIMITIVE_TYPE_MUL(type) \
    case NUdf::TDataType<type>::Id: { \
        return createKernel(std::make_shared<TExec<type>>(precision, scale)); \
    }
    INTEGRAL_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_MUL)
#undef MAKE_PRIMITIVE_TYPE_MUL    
    default:
        Y_ABORT("Unupported type.");
    }
}

template<template <typename> class TExec>
IComputationNode* WrapBlockDecimal(TStringBuf name, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto first = callable.GetInput(0);
    auto second = callable.GetInput(1);

    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    auto firstCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto secondCompute = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = { firstCompute, secondCompute };
    TVector<TType*> argsTypes = { firstType, secondType };

    std::shared_ptr<arrow::compute::ScalarKernel> kernel = MakeBlockKernel<TExec>(argsTypes, callable.GetType()->GetReturnType());
    return new TBlockFuncNode(ctx.Mutables, name, std::move(argsNodes), argsTypes, *kernel, kernel);
}

}

IComputationNode* WrapBlockDecimalMul(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockDecimal<TDecimalMulBlockExec>("DecimalMul", callable, ctx);
}

IComputationNode* WrapBlockDecimalDiv(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockDecimal<TDecimalDivBlockExec>("DecimalDiv", callable, ctx);
}

IComputationNode* WrapBlockDecimalMod(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockDecimal<TDecimalModBlockExec>("DecimalMod", callable, ctx);
}

}
}
