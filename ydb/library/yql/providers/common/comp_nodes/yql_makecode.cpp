#include "yql_makecode.h"
#include "yql_type_resource.h"
#include "yql_position.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr {
namespace NMiniKQL {

template <NYql::TExprNode::EType Type>
struct TMakeCodeArgs;

template <>
struct TMakeCodeArgs<NYql::TExprNode::World> {
    static constexpr size_t MinValue = 0;
    static constexpr size_t MaxValue = 0;
};

template <>
struct TMakeCodeArgs<NYql::TExprNode::Atom> {
    static constexpr size_t MinValue = 1;
    static constexpr size_t MaxValue = 1;
};

template <>
struct TMakeCodeArgs<NYql::TExprNode::List> {
    static constexpr size_t MinValue = 0;
    static constexpr size_t MaxValue = Max<size_t>();
};

template <>
struct TMakeCodeArgs<NYql::TExprNode::Callable> {
    static constexpr size_t MinValue = 1;
    static constexpr size_t MaxValue = Max<size_t>();
};

template <>
struct TMakeCodeArgs<NYql::TExprNode::Lambda> {
    static constexpr size_t MinValue = 2;
    static constexpr size_t MaxValue = Max<size_t>();
};

template <NYql::TExprNode::EType Type>
class TMakeCodeWrapper : public TMutableComputationNode<TMakeCodeWrapper<Type>> {
    typedef TMutableComputationNode<TMakeCodeWrapper<Type>> TBaseComputation;
public:
    TMakeCodeWrapper(TComputationMutables& mutables, TVector<IComputationNode*>&& args, ui32 exprCtxMutableIndex, NYql::TPosition pos)
        : TBaseComputation(mutables)
        , Args_(std::move(args))
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
        , Pos_(pos)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        NYql::TExprNode::TPtr retNode;
        switch (Type) {
        case NYql::TExprNode::Atom: {
            auto strValue = Args_[0]->GetValue(ctx);
            retNode = exprCtxPtr->NewAtom(Pos_, strValue.AsStringRef());
            break;
        }

        case NYql::TExprNode::List: {
            NYql::TExprNode::TListType items;
            for (ui32 i = 0; i < Args_.size(); ++i) {
                auto argValue = Args_[i]->GetValue(ctx);
                auto iter = argValue.GetListIterator();
                NUdf::TUnboxedValue codeValue;
                while (iter.Next(codeValue)) {
                    auto code = GetYqlCode(codeValue);
                    items.push_back(code);
                }
            }

            retNode = exprCtxPtr->NewList(Pos_, std::move(items));
            break;
        }

        case NYql::TExprNode::Callable: {
            NYql::TExprNode::TListType items;
            auto strValue = Args_[0]->GetValue(ctx);
            for (ui32 i = 1; i < Args_.size(); ++i) {
                auto argValue = Args_[i]->GetValue(ctx);
                auto iter = argValue.GetListIterator();
                NUdf::TUnboxedValue codeValue;
                while (iter.Next(codeValue)) {
                    auto code = GetYqlCode(codeValue);
                    items.push_back(code);
                }
            }

            retNode = exprCtxPtr->NewCallable(Pos_, strValue.AsStringRef(), std::move(items));
            break;
        }

        case NYql::TExprNode::Lambda: {
            auto countValue = Args_[0]->GetValue(ctx);
            const bool fixedArgs = !countValue;
            const ui32 argsCount = fixedArgs ? Args_.size() - 2 : countValue.template Get<ui32>();
            NYql::TExprNode::TListType argNodes;
            for (ui32 i = 0; i < argsCount; ++i) {
                argNodes.push_back(exprCtxPtr->NewArgument(Pos_, "arg" + ToString(i)));
            }
            auto argsNode = exprCtxPtr->NewArguments(Pos_, std::move(argNodes));

            // fill external nodes with arguments
            if (fixedArgs) {
                for (ui32 i = 0; i < argsCount; ++i) {
                    auto arg = argsNode->ChildPtr(i);
                    dynamic_cast<IComputationExternalNode*>(Args_[i + 1])->SetValue(ctx,
                        NUdf::TUnboxedValuePod(new TYqlCodeResource(exprCtxPtr, arg)));
                }
            } else {
                NUdf::TUnboxedValue* inplace = nullptr;
                NUdf::TUnboxedValue array = ctx.HolderFactory.CreateDirectArrayHolder(argsCount, inplace);
                for (ui32 i = 0; i < argsCount; ++i) {
                    auto arg = argsNode->ChildPtr(i);
                    inplace[i] = NUdf::TUnboxedValuePod(new TYqlCodeResource(exprCtxPtr, arg));
                }

                dynamic_cast<IComputationExternalNode*>(Args_[1])->SetValue(ctx, std::move(array));
            }

            // get body of lambda
            auto bodyValue = Args_.back()->GetValue(ctx);
            retNode = exprCtxPtr->NewLambda(Pos_, std::move(argsNode), GetYqlCode(bodyValue));
            break;
        }

        case NYql::TExprNode::World: {
            retNode = exprCtxPtr->NewWorld(Pos_);
            break;
        }

        default:
            MKQL_ENSURE(false, "Unsupported type:" << Type);
        }

        return NUdf::TUnboxedValuePod(new TYqlCodeResource(exprCtxPtr, retNode));
    }

    void RegisterDependencies() const override {
        for (auto arg : Args_) {
            this->DependsOn(arg);
        }
    }

private:
    TVector<IComputationNode*> Args_;
    ui32 ExprCtxMutableIndex_;
    NYql::TPosition Pos_;
};

template <NYql::TExprNode::EType Type>
IComputationNode* WrapMakeCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    constexpr size_t expectedMinArgsCount = 3 + TMakeCodeArgs<Type>::MinValue;
    constexpr size_t expectedMaxArgsCount = TMakeCodeArgs<Type>::MaxValue != Max<size_t>() ?
        3 + TMakeCodeArgs<Type>::MaxValue : Max<size_t>();
    MKQL_ENSURE(callable.GetInputsCount() >= expectedMinArgsCount
        && callable.GetInputsCount() <= expectedMaxArgsCount, "Expected from " << expectedMinArgsCount << " to "
        << expectedMaxArgsCount << " arg(s), but got: " << callable.GetInputsCount());
    auto pos = ExtractPosition(callable);
    TVector<IComputationNode*> args;
    args.reserve(callable.GetInputsCount() - 3);
    for (size_t i = 0; i < callable.GetInputsCount() - 3; ++i) {
        args.push_back(LocateNode(ctx.NodeLocator, callable, i + 3));
    }

    return new TMakeCodeWrapper<Type>(ctx.Mutables, std::move(args), exprCtxMutableIndex, pos);
}

template IComputationNode* WrapMakeCode<NYql::TExprNode::World>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeCode<NYql::TExprNode::Atom>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeCode<NYql::TExprNode::List>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeCode<NYql::TExprNode::Callable>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeCode<NYql::TExprNode::Lambda>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

}
}
