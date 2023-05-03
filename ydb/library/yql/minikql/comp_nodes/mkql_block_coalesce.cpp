#include "mkql_block_coalesce.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
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

class TCoalesceBlockWrapper : public TMutableComputationNode<TCoalesceBlockWrapper> {
public:
    TCoalesceBlockWrapper(TComputationMutables& mutables, IComputationNode* first, IComputationNode* second, TType* firstType, TType* secondType, bool unwrapFirst)
        : TMutableComputationNode(mutables)
        , First(first)
        , Second(second)
        , FirstType(firstType)
        , SecondType(secondType)
        , UnwrapFirst(unwrapFirst)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto first = First->GetValue(ctx);
        auto second = Second->GetValue(ctx);

        const auto& firstDatum = TArrowBlock::From(first).GetDatum();
        const auto& secondDatum = TArrowBlock::From(second).GetDatum();

        if (firstDatum.is_scalar() && secondDatum.is_scalar()) {
            if (!firstDatum.scalar()->is_valid) {
                return second.Release();
            } else if (!UnwrapFirst) {
                return first.Release();
            }
            auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), FirstType);
            auto builder = NYql::NUdf::MakeScalarBuilder(TTypeInfoHelper(), SecondType);
            auto firstItem = reader->GetScalarItem(*firstDatum.scalar());
            return ctx.HolderFactory.CreateArrowBlock(builder->Build(firstItem.GetOptionalValue()));
        }

        size_t len = firstDatum.is_scalar() ? (size_t)secondDatum.length() : (size_t)firstDatum.length();
        if (firstDatum.null_count() == firstDatum.length()) {
            return secondDatum.is_scalar() ? CopyAsArray(ctx, secondDatum, SecondType, false, len) : second.Release();
        } else if (firstDatum.null_count() == 0 || secondDatum.null_count() == secondDatum.length()) {
            if (firstDatum.is_scalar()) {
                return CopyAsArray(ctx, firstDatum, FirstType, UnwrapFirst, len);
            } else if (!UnwrapFirst) {
                return first.Release();
            }
            bool isNestedOptional = static_cast<TOptionalType*>(FirstType)->GetItemType()->IsOptional();
            return ctx.HolderFactory.CreateArrowBlock(NYql::NUdf::Unwrap(*firstDatum.array(), isNestedOptional));
        }

        Y_VERIFY(firstDatum.is_array());
        return secondDatum.is_scalar() ?
            Coalesce(ctx, *firstDatum.array(), secondDatum) :
            Coalesce(ctx, *firstDatum.array(), *secondDatum.array());
    }

private:
    NUdf::TUnboxedValuePod CopyAsArray(TComputationContext& ctx, const arrow::Datum& scalar, TType* type, bool unwrap, size_t len) const {
        Y_VERIFY(scalar.is_scalar());
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), type);
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), type, ctx.ArrowMemoryPool, len, &ctx.Builder->GetPgBuilder());
        auto item = reader->GetScalarItem(*scalar.scalar());
        if (unwrap) {
            item = item.GetOptionalValue();
        }
        builder->Add(item, len);
        return ctx.HolderFactory.CreateArrowBlock(builder->Build(true));
    }

    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const arrow::ArrayData& arr, const arrow::Datum& scalar) const {
        Y_VERIFY(scalar.is_scalar());
        Y_VERIFY(scalar.scalar()->is_valid);

        auto firstReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), FirstType);
        auto secondReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), SecondType);

        auto secondItem = secondReader->GetScalarItem(*scalar.scalar());

        const size_t len = arr.length;
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondType, ctx.ArrowMemoryPool, len, &ctx.Builder->GetPgBuilder());

        for (size_t i = 0; i < len; ++i) {
            auto result = firstReader->GetItem(arr, i);
            if (!result) {
                result = secondItem;
            } else if (UnwrapFirst) {
                result = result.GetOptionalValue();
            }
            builder->Add(result);
        }
        return ctx.HolderFactory.CreateArrowBlock(builder->Build(true));
    }

    NUdf::TUnboxedValuePod Coalesce(TComputationContext& ctx, const arrow::ArrayData& arr1, const arrow::ArrayData& arr2) const {
        Y_VERIFY(arr1.length == arr2.length);
        const size_t len = (size_t)arr1.length;
        auto firstReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), FirstType);
        auto secondReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), SecondType);
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondType, ctx.ArrowMemoryPool, len, &ctx.Builder->GetPgBuilder());

        for (size_t i = 0; i < len; ++i) {
            auto result = firstReader->GetItem(arr1, i);
            if (!result) {
                result = secondReader->GetItem(arr2, i);
            } else if (UnwrapFirst) {
                result = result.GetOptionalValue();
            }
            builder->Add(result);
        }
        return ctx.HolderFactory.CreateArrowBlock(builder->Build(true));
    }

    void RegisterDependencies() const final {
        DependsOn(First);
        DependsOn(Second);
    }

    IComputationNode* const First;
    IComputationNode* const Second;
    TType* const FirstType;
    TType* const SecondType;
    const bool UnwrapFirst;
};

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
    return new TCoalesceBlockWrapper(ctx.Mutables, firstCompute, secondCompute, firstType->GetItemType(), secondType->GetItemType(), needUnwrapFirst);
}

}
}
