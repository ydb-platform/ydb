#include "mkql_block_coalesce.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/util.h>

#include <arrow/util/bitmap_ops.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCoalesceBlockExec {
public:
    TCoalesceBlockExec(const std::shared_ptr<arrow::DataType>& returnArrowType, TType* firstItemType, TType* secondItemType, bool needUnwrapFirst)
        : ReturnArrowType_(returnArrowType)
        , FirstItemType_(firstItemType)
        , SecondItemType_(secondItemType)
        , NeedUnwrapFirst_(needUnwrapFirst)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        const auto& first = batch.values[0];
        const auto& second = batch.values[1];
        MKQL_ENSURE(!first.is_scalar() || !second.is_scalar(), "Expected at least one array");
        size_t length = Max(first.length(), second.length());
        auto firstReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), FirstItemType_);
        auto secondReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), SecondItemType_);
        if (first.is_scalar()) {
            auto firstValue = firstReader->GetScalarItem(*first.scalar());
            if (firstValue) {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                builder->Add(NeedUnwrapFirst_ ? firstValue.GetOptionalValue() : firstValue, length);
                *res = builder->Build(true);
            } else {
                *res = second;
            }
        } else if (second.is_scalar()) {
            const auto& firstArray = *first.array();
            if (firstArray.GetNullCount() == 0) {
                *res = NeedUnwrapFirst_ ? Unwrap(firstArray, FirstItemType_) : first;
            } else if ((size_t)firstArray.GetNullCount() == length) {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                auto secondValue = secondReader->GetScalarItem(*second.scalar());
                builder->Add(secondValue, length);
                *res = builder->Build(true);
            } else {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                auto secondValue = secondReader->GetScalarItem(*second.scalar());
                for (size_t i = 0; i < length; ++i) {
                    auto firstItem = firstReader->GetItem(firstArray, i);
                    if (firstItem) {
                        builder->Add(NeedUnwrapFirst_ ? firstItem.GetOptionalValue() : firstItem);
                    } else {
                        builder->Add(secondValue);
                    }
                }

                *res = builder->Build(true);
            }
        } else {
            const auto& firstArray = *first.array();
            const auto& secondArray = *second.array();
            if (firstArray.GetNullCount() == 0) {
                *res = NeedUnwrapFirst_ ? Unwrap(firstArray, FirstItemType_) : first;
            } else if ((size_t)firstArray.GetNullCount() == length) {
                *res = second;
            } else {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                for (size_t i = 0; i < length; ++i) {
                    auto firstItem = firstReader->GetItem(firstArray, i);
                    if (firstItem) {
                        builder->Add(NeedUnwrapFirst_ ? firstItem.GetOptionalValue() : firstItem);
                    } else {
                        auto secondItem = secondReader->GetItem(secondArray, i);
                        builder->Add(secondItem);
                    }
                }

                *res = builder->Build(true);
            }
        }

        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType_;
    TType* const FirstItemType_;
    TType* const SecondItemType_;
    const bool NeedUnwrapFirst_;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockCoalesceKernel(const TVector<TType*>& argTypes, TType* resultType, bool needUnwrapFirst) {
    using TExec = TCoalesceBlockExec;

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TExec>(
        returnArrowType,
        AS_TYPE(TBlockType, argTypes[0])->GetItemType(),
        AS_TYPE(TBlockType, argTypes[1])->GetItemType(),
        needUnwrapFirst);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockCoalesce(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto first = callable.GetInput(0);
    auto second = callable.GetInput(1);

    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    auto firstItemType = firstType->GetItemType();
    auto secondItemType = secondType->GetItemType();
    MKQL_ENSURE(firstItemType->IsOptional() || firstItemType->IsPg(), "Expecting Optional or Pg type as first argument");

    bool needUnwrapFirst = false;
    if (!firstItemType->IsSameType(*secondItemType)) {
        needUnwrapFirst = true;
        bool firstOptional;
        firstItemType = UnpackOptional(firstItemType, firstOptional);
        MKQL_ENSURE(firstItemType->IsSameType(*secondItemType), "Uncompatible arguemnt types");
    }

    auto firstCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto secondCompute = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = { firstCompute, secondCompute };
    TVector<TType*> argsTypes = { firstType, secondType };

    auto kernel = MakeBlockCoalesceKernel(argsTypes, secondType, needUnwrapFirst);
    return new TBlockFuncNode(ctx.Mutables, "Coalesce", std::move(argsNodes), argsTypes, *kernel, kernel);
}

}
}
