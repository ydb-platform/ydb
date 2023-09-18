#include "mkql_block_if.h"

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockIfScalarWrapper : public TMutableComputationNode<TBlockIfScalarWrapper> {
public:
    class TArrowNode : public IArrowKernelComputationNode {
    public:
        TArrowNode(const TBlockIfScalarWrapper* parent)
            : Parent_(parent)
            , ArgsValuesDescr_(ToValueDescr(parent->ArgsTypes))
            , Kernel_(ConvertToInputTypes(parent->ArgsTypes), ConvertToOutputType(parent->ResultType), [parent](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                *res = parent->CalculateImpl(MakeDatumProvider(batch.values[0]), MakeDatumProvider(batch.values[1]), MakeDatumProvider(batch.values[2]), *ctx->memory_pool());
                return arrow::Status::OK();
            })
        {
            Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
            Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
        }

        TStringBuf GetKernelName() const final {
            return "If";
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
                return Parent_->Pred;
            case 1:
                return Parent_->Then;
            case 2:
                return Parent_->Else;
            default:
                throw yexception() << "Bad argument index";
            }
        }

    private:
        const TBlockIfScalarWrapper* Parent_;
        const std::vector<arrow::ValueDescr> ArgsValuesDescr_;
        arrow::compute::ScalarKernel Kernel_;
    };
    friend class TArrowNode;

    TBlockIfScalarWrapper(TComputationMutables& mutables, IComputationNode* pred, IComputationNode* thenNode, IComputationNode* elseNode, TType* resultType,
                          bool thenIsScalar, bool elseIsScalar, const TVector<TType*>& argsTypes)
        : TMutableComputationNode(mutables)
        , Pred(pred)
        , Then(thenNode)
        , Else(elseNode)
        , ResultType(resultType)
        , ThenIsScalar(thenIsScalar)
        , ElseIsScalar(elseIsScalar)
        , ArgsTypes(argsTypes)
    {
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        Y_UNUSED(ctx);
        return std::make_unique<TArrowNode>(this);
    }

    arrow::Datum CalculateImpl(const TDatumProvider& predProv, const TDatumProvider& thenProv, const TDatumProvider& elseProv,
        arrow::MemoryPool& memoryPool) const {
        auto predValue = predProv();

        const bool predScalarValue = GetPrimitiveScalarValue<bool>(*predValue.scalar());
        auto result = predScalarValue ? thenProv() : elseProv();

        if (ThenIsScalar == ElseIsScalar || (predScalarValue ? !ThenIsScalar : !ElseIsScalar)) {
            // can return result as-is
            return result;
        }

        auto otherDatum = predScalarValue ? elseProv() : thenProv();
        MKQL_ENSURE(otherDatum.is_arraylike(), "Expecting array");

        std::shared_ptr<arrow::Scalar> resultScalar = result.scalar();

        TVector<std::shared_ptr<arrow::ArrayData>> resultArrays;
        auto itemType = AS_TYPE(TBlockType, ResultType)->GetItemType();
        ForEachArrayData(otherDatum, [&](const std::shared_ptr<arrow::ArrayData>& otherData) {
            auto chunk = MakeArrayFromScalar(*resultScalar, otherData->length, itemType, memoryPool);
            ForEachArrayData(chunk, [&](const auto& array) {
                resultArrays.push_back(array);
            });
        });
        return MakeArray(resultArrays);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateArrowBlock(CalculateImpl(MakeDatumProvider(Pred, ctx), MakeDatumProvider(Then, ctx), MakeDatumProvider(Else, ctx), ctx.ArrowMemoryPool));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Pred);
        DependsOn(Then);
        DependsOn(Else);
    }

    IComputationNode* const Pred;
    IComputationNode* const Then;
    IComputationNode* const Else;
    TType* const ResultType;
    const bool ThenIsScalar;
    const bool ElseIsScalar;
    const TVector<TType*> ArgsTypes;
};

template<bool ThenIsScalar, bool ElseIsScalar>
class TIfBlockExec {
public:
    explicit TIfBlockExec(TType* type)
        : ThenReader(MakeBlockReader(TTypeInfoHelper(), type)), ElseReader(MakeBlockReader(TTypeInfoHelper(), type)), Type(type)
    {
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum predDatum = batch.values[0];
        arrow::Datum thenDatum = batch.values[1];
        arrow::Datum elseDatum = batch.values[2];

        TBlockItem thenItem;
        const arrow::ArrayData* thenArray = nullptr;
        if constexpr(ThenIsScalar) {
            thenItem = ThenReader->GetScalarItem(*thenDatum.scalar());
        } else {
            MKQL_ENSURE(thenDatum.is_array(), "Expecting array");
            thenArray = thenDatum.array().get();
        }

        TBlockItem elseItem;
        const arrow::ArrayData* elseArray = nullptr;
        if constexpr(ElseIsScalar) {
            elseItem = ElseReader->GetScalarItem(*elseDatum.scalar());
        } else {
            MKQL_ENSURE(elseDatum.is_array(), "Expecting array");
            elseArray = elseDatum.array().get();
        }

        MKQL_ENSURE(predDatum.is_array(), "Expecting array");
        const std::shared_ptr<arrow::ArrayData>& pred = predDatum.array();

        const size_t len = pred->length;
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), Type, *ctx->memory_pool(), len, nullptr);
        const ui8* predValues = pred->GetValues<uint8_t>(1);
        for (size_t i = 0; i < len; ++i) {
            if constexpr (!ThenIsScalar) {
                thenItem = ThenReader->GetItem(*thenArray, i);
            }
            if constexpr (!ElseIsScalar) {
                elseItem = ElseReader->GetItem(*elseArray, i);
            }

            ui64 mask = -ui64(predValues[i]);
            ui64 low = (thenItem.Low() & mask) | (elseItem.Low() & ~mask);
            ui64 high = (thenItem.High() & mask) | (elseItem.High() & ~mask);
            builder->Add(TBlockItem{low, high});
        }
        *res = builder->Build(true);
        return arrow::Status::OK();
    }

private:
    const std::unique_ptr<IBlockReader> ThenReader;
    const std::unique_ptr<IBlockReader> ElseReader;
    TType* const Type;
};


template<bool ThenIsScalar, bool ElseIsScalar>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockIfKernel(const TVector<TType*>& argTypes, TType* resultType) {
    using TExec = TIfBlockExec<ThenIsScalar, ElseIsScalar>;

    auto exec = std::make_shared<TExec>(AS_TYPE(TBlockType, resultType)->GetItemType());
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockIf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    auto pred = callable.GetInput(0);
    auto thenNode = callable.GetInput(1);
    auto elseNode = callable.GetInput(2);

    auto predType = AS_TYPE(TBlockType, pred.GetStaticType());
    MKQL_ENSURE(AS_TYPE(TDataType, predType->GetItemType())->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Expected bool as first argument");

    auto thenType = AS_TYPE(TBlockType, thenNode.GetStaticType());
    auto elseType = AS_TYPE(TBlockType, elseNode.GetStaticType());
    MKQL_ENSURE(thenType->GetItemType()->IsSameType(*elseType->GetItemType()), "Different return types in branches.");

    auto predCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto thenCompute = LocateNode(ctx.NodeLocator, callable, 1);
    auto elseCompute = LocateNode(ctx.NodeLocator, callable, 2);

    bool predIsScalar = predType->GetShape() == TBlockType::EShape::Scalar;
    bool thenIsScalar = thenType->GetShape() == TBlockType::EShape::Scalar;
    bool elseIsScalar = elseType->GetShape() == TBlockType::EShape::Scalar;
    TVector<TType*> argsTypes = { predType, thenType, elseType };


    if (predIsScalar) {
        return new TBlockIfScalarWrapper(ctx.Mutables, predCompute, thenCompute, elseCompute, thenType,
                                         thenIsScalar, elseIsScalar, argsTypes);
    }

    TComputationNodePtrVector argsNodes = { predCompute, thenCompute, elseCompute };

    std::shared_ptr<arrow::compute::ScalarKernel> kernel;
    if (thenIsScalar && elseIsScalar) {
        kernel = MakeBlockIfKernel<true, true>(argsTypes, thenType);
    } else if (thenIsScalar && !elseIsScalar) {
        kernel = MakeBlockIfKernel<true, false>(argsTypes, thenType);
    } else if (!thenIsScalar && elseIsScalar) {
        kernel = MakeBlockIfKernel<false, true>(argsTypes, thenType);
    } else {
        kernel = MakeBlockIfKernel<false, false>(argsTypes, thenType);
    }

    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
}

}
}
