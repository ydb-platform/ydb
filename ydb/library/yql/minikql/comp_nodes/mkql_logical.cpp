#include "mkql_logical.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include "mkql_check_args.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsLeftOptional, bool IsRightOptional>
class TAndWrapper : public TBinaryCodegeneratorNode<TAndWrapper<IsLeftOptional, IsRightOptional>> {
    typedef TBinaryCodegeneratorNode<TAndWrapper<IsLeftOptional, IsRightOptional>> TBaseComputation;
public:
    TAndWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(left, right, EValueRepresentation::Embedded)
    {
        Y_UNUSED(mutables);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& left = this->Left->GetValue(ctx);
        if (!IsLeftOptional || left) {
            if (!left.template Get<bool>()) {
                return NUdf::TUnboxedValuePod(false);
            }
        }

        const auto& right = this->Right->GetValue(ctx);
        if (!IsRightOptional || right) {
            if (!right.template Get<bool>()) {
                return NUdf::TUnboxedValuePod(false);
            }
        }

        // both either true (just true) or nothing
        if (IsLeftOptional && !left || IsRightOptional && !right) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(true);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto left = GetNodeValue(this->Left, ctx, block);

        const auto uvFalse = GetFalse(context);

        const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, left, uvFalse, "skip", block);

        const auto both = BasicBlock::Create(context, "both", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2, "result", done);

        result->addIncoming(uvFalse, block);
        BranchInst::Create(done, both, skip, block);

        block = both;
        const auto right = GetNodeValue(this->Right, ctx, block);

        if (IsLeftOptional) {
            const auto andr = BinaryOperator::CreateAnd(left, right, "and", block);
            const auto over = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, uvFalse, "over", block);
            const auto full = SelectInst::Create(over, uvFalse, andr, "full", block);
            result->addIncoming(full, block);
        } else {
            result->addIncoming(right, block);
        }

        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template <bool IsLeftOptional, bool IsRightOptional>
class TOrWrapper : public TBinaryCodegeneratorNode<TOrWrapper<IsLeftOptional, IsRightOptional>> {
    typedef TBinaryCodegeneratorNode<TOrWrapper<IsLeftOptional, IsRightOptional>> TBaseComputation;
public:
    TOrWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(left, right, EValueRepresentation::Embedded)
    {
        Y_UNUSED(mutables);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& left = this->Left->GetValue(ctx);
        if (!IsLeftOptional || left) {
            if (left.template Get<bool>()) {
                return NUdf::TUnboxedValuePod(true);
            }
        }

        const auto& right = this->Right->GetValue(ctx);
        if (!IsRightOptional || right) {
            if (right.template Get<bool>()) {
                return NUdf::TUnboxedValuePod(true);
            }
        }

        // both either false (just false) or nothing
        if (IsLeftOptional && !left || IsRightOptional && !right) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(false);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto left = GetNodeValue(this->Left, ctx, block);

        const auto uvTrue = GetTrue(context);

        const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, left, uvTrue, "skip", block);

        const auto both = BasicBlock::Create(context, "both", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2, "result", done);

        result->addIncoming(uvTrue, block);
        BranchInst::Create(done, both, skip, block);

        block = both;
        const auto right = GetNodeValue(this->Right, ctx, block);

        if (IsLeftOptional) {
            const auto andr = BinaryOperator::CreateAnd(left, right, "and", block);
            const auto over = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, uvTrue, "over", block);
            const auto full = SelectInst::Create(over, uvTrue, andr, "full", block);
            result->addIncoming(full, block);
        } else {
            result->addIncoming(right, block);
        }

        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template <bool IsLeftOptional, bool IsRightOptional>
class TXorWrapper : public TBinaryCodegeneratorNode<TXorWrapper<IsLeftOptional, IsRightOptional>> {
    typedef TBinaryCodegeneratorNode<TXorWrapper<IsLeftOptional, IsRightOptional>> TBaseComputation;
public:
    TXorWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(left, right, EValueRepresentation::Embedded)
    {
        Y_UNUSED(mutables);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& left = this->Left->GetValue(ctx);
        if (IsLeftOptional && !left) {
            return NUdf::TUnboxedValuePod();
        }

        const auto& right = this->Right->GetValue(ctx);
        if (IsRightOptional && !right) {
            return NUdf::TUnboxedValuePod();
        }

        const bool res = left.template Get<bool>() != right.template Get<bool>();
        return NUdf::TUnboxedValuePod(res);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        if (IsLeftOptional || IsRightOptional) {
            const auto zero = ConstantInt::get(valueType, 0);

            const auto both = BasicBlock::Create(context, "both", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto result = PHINode::Create(valueType, 2, "result", done);

            if (IsLeftOptional) {
                const auto left = GetNodeValue(this->Left, ctx, block);

                const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, left, zero, "skip", block);
                result->addIncoming(zero, block);
                BranchInst::Create(done, both, skip, block);

                block = both;
                const auto right = GetNodeValue(this->Right, ctx, block);

                if (IsRightOptional) {
                    const auto xorr = BinaryOperator::CreateXor(left, right, "xor", block);
                    const auto full = BinaryOperator::CreateOr(xorr, GetFalse(context), "full", block);
                    const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, zero, "null", block);
                    const auto last = SelectInst::Create(null, zero, full, "last", block);
                    result->addIncoming(last, block);
                } else {
                    const auto xorr = BinaryOperator::CreateXor(left, right, "xor", block);
                    const auto full = BinaryOperator::CreateOr(xorr, GetFalse(context), "full", block);
                    result->addIncoming(full, block);
                }
            } else if (IsRightOptional) {
                const auto right = GetNodeValue(this->Right, ctx, block);

                const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, zero, "skip", block);
                result->addIncoming(zero, block);
                BranchInst::Create(done, both, skip, block);

                block = both;
                const auto left = GetNodeValue(this->Left, ctx, block);

                const auto xorr = BinaryOperator::CreateXor(left, right, "xor", block);
                const auto full = BinaryOperator::CreateOr(xorr, GetFalse(context), "full", block);
                result->addIncoming(full, block);
            }

            BranchInst::Create(done, block);
            block = done;

            return result;
        } else {
            const auto left = GetNodeValue(this->Left, ctx, block);
            const auto right = GetNodeValue(this->Right, ctx, block);

            const auto xorr = BinaryOperator::CreateXor(left, right, "xor", block);
            const auto full = BinaryOperator::CreateOr(xorr, GetFalse(context), "full", block);
            return full;
        }
    }
#endif
};

template <bool IsOptional>
class TNotWrapper : public TDecoratorCodegeneratorNode<TNotWrapper<IsOptional>> {
    typedef TDecoratorCodegeneratorNode<TNotWrapper<IsOptional>> TBaseComputation;
public:
    TNotWrapper(IComputationNode* arg)
        : TBaseComputation(arg)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& arg) const {
        if (IsOptional && !arg) {
            return NUdf::TUnboxedValuePod();
        }

        const bool res = !arg.template Get<bool>();
        return NUdf::TUnboxedValuePod(res);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* arg, BasicBlock*& block) const {
        Y_UNUSED(ctx);
        const auto xorr = BinaryOperator::CreateXor(arg, ConstantInt::get(arg->getType(), 1), "xor", block);
        const auto result = IsOptional ? SelectInst::Create(IsExists(arg, block), xorr, arg, "sel", block) : static_cast<Value*>(xorr);
        return result;
    }
#endif
};

template <template <bool, bool> class TWrapper>
IComputationNode* WrapLogicalFunction(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto nodeLocator = ctx.NodeLocator;
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto leftType = callable.GetInput(0).GetStaticType();
    const auto rightType = callable.GetInput(1).GetStaticType();
    CheckBinaryFunctionArgs(leftType, rightType, true, true);

    const bool isLeftOptional = leftType->IsOptional();
    const bool isRightOptional = rightType->IsOptional();
    const auto left = LocateNode(nodeLocator, callable, 0);
    const auto right = LocateNode(nodeLocator, callable, 1);
    if (isLeftOptional) {
        if (isRightOptional) {
            return new TWrapper<true, true>(ctx.Mutables, left, right);
        } else {
            return new TWrapper<true, false>(ctx.Mutables, left, right);
        }
    }
    else {
        if (isRightOptional) {
            return new TWrapper<false, true>(ctx.Mutables, left, right);
        } else {
            return new TWrapper<false, false>(ctx.Mutables, left, right);
        }
    }
}

}

IComputationNode* WrapAnd(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapLogicalFunction<TAndWrapper>(callable, ctx);
}

IComputationNode* WrapOr(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapLogicalFunction<TOrWrapper>(callable, ctx);
}

IComputationNode* WrapXor(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapLogicalFunction<TXorWrapper>(callable, ctx);
}

IComputationNode* WrapNot(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    bool isOptional;
    const auto& dataType = UnpackOptionalData(callable.GetInput(0), isOptional);

    const auto schemeType = dataType->GetSchemeType();
    MKQL_ENSURE(schemeType == NUdf::TDataType<bool>::Id, "Expected bool");
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);

    if (isOptional) {
        return new TNotWrapper<true>(node);
    }
    else {
        return new TNotWrapper<false>(node);
    }
}


}
}
