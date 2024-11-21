#include "mkql_builtins_codegen.h" // Y_IGNORE

#ifndef MKQL_DISABLE_CODEGEN
#include "mkql_builtins_codegen_llvm.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

using namespace llvm;

Value* GenerateUnaryWithoutCheck(Value* arg, const TCodegenContext& ctx, BasicBlock*& block, TUnaryGenFunc generator) {
    return generator(arg, ctx, block);
}

Value* GenerateUnaryWithCheck(Value* arg, const TCodegenContext& ctx, BasicBlock*& block, TUnaryGenFunc generator) {
    auto& context = ctx.Codegen.GetContext();
    const auto valType = Type::getInt128Ty(context);

    const auto zero = ConstantInt::get(valType, 0ULL);
    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, arg, zero, "check", block);

    const auto done = BasicBlock::Create(context, "done", ctx.Func);
    const auto good = BasicBlock::Create(context, "good", ctx.Func);
    const auto result = PHINode::Create(valType, 2, "result", done);
    result->addIncoming(zero, block);

    BranchInst::Create(done, good, check, block);

    block = good;
    const auto data = generator(arg, ctx, block);
    result->addIncoming(data, block);
    BranchInst::Create(done, block);

    block = done;
    return result;
}

template<bool CheckLeft, bool CheckRight>
Value* GenerateBinary(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator) {
    auto& context = ctx.Codegen.GetContext();

    const auto valType = Type::getInt128Ty(context);
    const auto zero = ConstantInt::get(valType, 0ULL);

    if (CheckLeft && CheckRight) {
        const auto tls = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, left, zero, "tls", block);;
        const auto trs = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, zero, "trs", block);;
        const auto check = BinaryOperator::CreateOr(tls, trs, "or", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto result = PHINode::Create(valType, 2, "result", done);
        result->addIncoming(zero, block);

        BranchInst::Create(done, good, check, block);

        block = good;
        const auto data = generator(left, right, ctx, block);
        result->addIncoming(data, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    } else if (CheckLeft || CheckRight) {
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, CheckLeft ? left : right, zero, "check", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto result = PHINode::Create(valType, 2, "result", done);
        result->addIncoming(zero, block);

        BranchInst::Create(done, good, check, block);

        block = good;
        const auto data = generator(left, right, ctx, block);
        result->addIncoming(data, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    } else {
        return generator(left, right, ctx, block);
    }
}

Value* GenerateAggregate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator) {
    auto& context = ctx.Codegen.GetContext();

    const auto valType = Type::getInt128Ty(context);
    const auto zero = ConstantInt::get(valType, 0ULL);

    const auto tls = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, left, zero, "tls", block);;
    const auto trs = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, right, zero, "trs", block);;
    const auto check = BinaryOperator::CreateOr(tls, trs, "or", block);

    const auto null = BasicBlock::Create(context, "null", ctx.Func);
    const auto good = BasicBlock::Create(context, "good", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);
    const auto result = PHINode::Create(valType, 2, "result", done);

    BranchInst::Create(null, good, check, block);

    block = null;
    const auto both = BinaryOperator::CreateOr(left, right, "both", block);
    result->addIncoming(both, block);
    BranchInst::Create(done, block);

    block = good;
    const auto data = generator(left, right, ctx, block);
    result->addIncoming(data, block);
    BranchInst::Create(done, block);

    block = done;
    return result;
}

Value* GenerateCompareAggregate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator, CmpInst::Predicate predicate) {
    auto& context = ctx.Codegen.GetContext();

    const auto valType = Type::getInt128Ty(context);
    const auto zero = ConstantInt::get(valType, 0ULL);

    const auto tls = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, left, zero, "tls", block);
    const auto trs = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, right, zero, "trs", block);
    const auto check = BinaryOperator::CreateAnd(tls, trs, "and", block);

    const auto null = BasicBlock::Create(context, "null", ctx.Func);
    const auto good = BasicBlock::Create(context, "good", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);
    const auto result = PHINode::Create(valType, 2, "result", done);

    BranchInst::Create(good, null, check, block);

    block = good;
    const auto data = generator(left, right, ctx, block);
    result->addIncoming(data, block);
    BranchInst::Create(done, block);

    block = null;
    const auto test = CmpInst::Create(Instruction::ICmp, predicate, tls, trs, "test", block);
    const auto wide = MakeBoolean(test, context, block);
    result->addIncoming(wide, block);
    BranchInst::Create(done, block);

    block = done;
    return result;
}

template<bool CheckFirst>
Value* GenerateTernary(Value* first, Value* second, Value* third, const TCodegenContext& ctx, BasicBlock*& block, TTernaryGenFunc generator) {
    auto& context = ctx.Codegen.GetContext();

    const auto valType = Type::getInt128Ty(context);

    if (CheckFirst) {
        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, first, zero, "check", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto result = PHINode::Create(valType, 2, "result", done);
        result->addIncoming(zero, block);

        BranchInst::Create(done, good, check, block);

        block = good;

        const auto data = generator(first, second, third, ctx, block);
        result->addIncoming(data, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    } else {
        return generator(first, second, third, ctx, block);
    }
}

template Value* GenerateBinary<false, false>(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);
template Value* GenerateBinary<true, false>(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);
template Value* GenerateBinary<false, true>(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);
template Value* GenerateBinary<true, true>(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);

template Value* GenerateTernary<true>(Value* first, Value* second, Value* third, const TCodegenContext& ctx, BasicBlock*& block, TTernaryGenFunc generator);
template Value* GenerateTernary<false>(Value* first, Value* second, Value* third, const TCodegenContext& ctx, BasicBlock*& block, TTernaryGenFunc generator);

template<>
std::string GetFuncNameForType<bool>(const char*) {
    return std::string(); // Stub for MSVC linker
}

template<>
std::string GetFuncNameForType<i8>(const char* name) {
    return std::string(name) += ".i8";
}

template<>
std::string GetFuncNameForType<ui8>(const char* name) {
    return std::string(name) += ".i8";
}

template<>
std::string GetFuncNameForType<i16>(const char* name) {
    return std::string(name) += ".i16";
}

template<>
std::string GetFuncNameForType<ui16>(const char* name) {
    return std::string(name) += ".i16";
}

template<>
std::string GetFuncNameForType<i32>(const char* name) {
    return std::string(name) += ".i32";
}

template<>
std::string GetFuncNameForType<ui32>(const char* name) {
    return std::string(name) += ".i32";
}

template<>
std::string GetFuncNameForType<i64>(const char* name) {
    return std::string(name) += ".i64";
}

template<>
std::string GetFuncNameForType<ui64>(const char* name) {
    return std::string(name) += ".i64";
}

template<>
std::string GetFuncNameForType<float>(const char* name) {
    return std::string(name) += ".f32";
}

template<>
std::string GetFuncNameForType<double>(const char* name) {
    return std::string(name) += ".f64";
}

}
}
#endif
