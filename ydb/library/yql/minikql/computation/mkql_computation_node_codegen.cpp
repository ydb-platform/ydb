#include "mkql_computation_node_codegen.h" // Y_IGNORE
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/codegen/codegen.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <util/string/cast.h>
#include <util/folder/path.h>

#ifndef MKQL_DISABLE_CODEGEN

extern "C" void DeleteBoxed(NKikimr::NUdf::IBoxedValue *const boxed) {
    delete boxed;
}

extern "C" void DeleteString(void* strData) {
    auto& str = *(NKikimr::NUdf::TStringValue*)(&strData);
    UdfFreeWithSize(strData, 16 + str.Capacity());
}

namespace NKikimr {
namespace NMiniKQL {

constexpr bool EnableStaticRefcount = true;

using namespace llvm;

Type* GetCompContextType(LLVMContext &context) {
    const auto ptrValueType = PointerType::getUnqual(Type::getInt128Ty(context));
    const auto structPtrType = PointerType::getUnqual(StructType::get(context));
    const auto stringRefType = StructType::get(context, {
        Type::getInt8PtrTy(context),
        Type::getInt32Ty(context),
        Type::getInt32Ty(context)
    });
    const auto sourcePosType = StructType::get(context, {
        Type::getInt32Ty(context),
        Type::getInt32Ty(context),
        stringRefType
    });
    return StructType::get(context, {
        structPtrType,              // factory
        structPtrType,              // stats
        ptrValueType,               // mutables
        structPtrType,              // builder
        Type::getFloatTy(context),  // adjustor
        Type::getInt32Ty(context),  // rsscounter
        PointerType::getUnqual(sourcePosType)
    });
}

Value* TCodegenContext::GetFactory() const {
    if (!Factory) {
        auto& context = Codegen.GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        if (Func->getEntryBlock().empty()) {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "factory_ptr", &Func->getEntryBlock());
            const_cast<Value*&>(Factory) = new LoadInst(ptrType, ptr, "factory", &Func->getEntryBlock());
        } else {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "factory_ptr", &Func->getEntryBlock().front());
            const_cast<Value*&>(Factory) = new LoadInst(ptrType, ptr, "factory", &Func->getEntryBlock().back());
        }
    }
    return Factory;
}

Value* TCodegenContext::GetStat() const {
    if (!Stat) {
        auto& context = Codegen.GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        if (Func->getEntryBlock().empty()) {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 1)}, "stat_ptr", &Func->getEntryBlock());
            const_cast<Value*&>(Stat) = new LoadInst(ptrType, ptr, "stat", &Func->getEntryBlock());
        } else {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 1)}, "stat_ptr", &Func->getEntryBlock().front());
            const_cast<Value*&>(Stat) = new LoadInst(ptrType, ptr, "stat", &Func->getEntryBlock().back());
        }
    }
    return Stat;
}

Value* TCodegenContext::GetMutables() const {
    if (!Mutables) {
        auto& context = Codegen.GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto ptrType = PointerType::getUnqual(Type::getInt128Ty(context));
        if (Func->getEntryBlock().empty()) {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 2)}, "mutables_ptr", &Func->getEntryBlock());
            const_cast<Value*&>(Mutables) = new LoadInst(ptrType, ptr, "mutables", &Func->getEntryBlock());
        } else {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 2)}, "mutables_ptr", &Func->getEntryBlock().front());
            const_cast<Value*&>(Mutables) = new LoadInst(ptrType, ptr, "mutables", &Func->getEntryBlock().back());
        }
    }
    return Mutables;
}

Value* TCodegenContext::GetBuilder() const {
    if (!Builder) {
        auto& context = Codegen.GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        if (Func->getEntryBlock().empty()) {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 3)}, "builder_ptr", &Func->getEntryBlock());
            const_cast<Value*&>(Builder) = new LoadInst(ptrType, ptr, "builder", &Func->getEntryBlock());
        } else {
            const auto ptr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 3)}, "builder_ptr", &Func->getEntryBlock().front());
            const_cast<Value*&>(Builder) = new LoadInst(ptrType, ptr, "builder", &Func->getEntryBlock().back());
        }
    }
    return Builder;
}

Function* GenerateCompareFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, IComputationExternalNode* left,
                                IComputationExternalNode* right, IComputationNode* compare) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto codegenLeft = dynamic_cast<ICodegeneratorExternalNode*>(left);
    const auto codegenRight = dynamic_cast<ICodegeneratorExternalNode*>(right);
    MKQL_ENSURE(codegenLeft, "Left must be codegenerator node.");
    MKQL_ENSURE(codegenRight, "Right must be codegenerator node.");

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto ptrType = PointerType::getUnqual(valueType);
    const auto returnType = Type::getInt1Ty(context);
    const auto contextType = GetCompContextType(context);

    const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(returnType, {PointerType::getUnqual(contextType), valueType, valueType}, false):
        FunctionType::get(returnType, {PointerType::getUnqual(contextType), ptrType, ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    ctx.Ctx = &*args;
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto lv = &*++args;
    const auto rv = &*++args;

    codegenLeft->SetValueBuilder([lv](const TCodegenContext&) { return lv; });
    codegenRight->SetValueBuilder([rv](const TCodegenContext&) { return rv; });

    codegenLeft->CreateInvalidate(ctx, block);
    codegenRight->CreateInvalidate(ctx, block);

    const auto res = GetNodeValue(compare, ctx, block);
    const auto cast = CastInst::Create(Instruction::Trunc, res, returnType, "bool", block);
    ReturnInst::Create(context, cast, block);

    codegenLeft->SetValueBuilder({});
    codegenRight->SetValueBuilder({});

    return ctx.Func;
}

Value* GetterFor(NUdf::EDataSlot slot, Value* value, LLVMContext &context, BasicBlock* block) {
    switch (slot) {
        case NUdf::EDataSlot::Bool: return GetterFor<bool>(value, context, block);
        case NUdf::EDataSlot::Decimal: return GetterForInt128(value, block);
        case NUdf::EDataSlot::Float: return GetterFor<float>(value, context, block);
        case NUdf::EDataSlot::Double: return GetterFor<double>(value, context, block);
        default: break;
    }

    const auto trunc = CastInst::Create(Instruction::Trunc, value, IntegerType::get(context, NUdf::GetDataTypeInfo(slot).FixedSize << 3U), "trunc", block);
    return trunc;
}

namespace {

Value* GetMarkFromUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock* block) {
    auto& context = ctx.Codegen.GetContext();

    const auto type8 = Type::getInt8Ty(context);
    if (value->getType()->isPointerTy()) {
        const auto type = StructType::get(context, {PointerType::getUnqual(StructType::get(context)), ArrayType::get(type8, 8U)});
        const auto cast = CastInst::Create(Instruction::BitCast, value, PointerType::getUnqual(type), "cast", block);
        const auto type32 = Type::getInt32Ty(context);
        const auto metaptr = GetElementPtrInst::CreateInBounds(type, cast, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1), ConstantInt::get(type32, 7)}, "metaptr", block);
        const auto meta = new LoadInst(type8, metaptr, "meta", block);
        const auto mark = BinaryOperator::CreateAnd(meta, ConstantInt::get(meta->getType(), 3), "mark", block);
        return mark;
    } else {
        const auto lshr = BinaryOperator::CreateLShr(value, ConstantInt::get(value->getType(), 120), "lshr",  block);
        const auto meta = CastInst::Create(Instruction::Trunc, lshr, type8, "meta", block);
        const auto mark = BinaryOperator::CreateAnd(ConstantInt::get(meta->getType(), 3), meta, "mark", block);
        return mark;
    }

}

template<bool BoxedOrString>
Value* GetPointerFromUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock* block) {
    auto& context = ctx.Codegen.GetContext();

    const auto type32 = Type::getInt32Ty(context);
    const auto type64 = Type::getInt64Ty(context);
    const auto type = PointerType::getUnqual(BoxedOrString ?
        StructType::get(context, {PointerType::getUnqual(StructType::get(context)), type32, Type::getInt16Ty(context)}):
        StructType::get(context, {type32, type32, type32, type32})
    );
    if (value->getType()->isPointerTy()) {
        const auto strType = StructType::get(context, {type, type64});
        const auto cast = CastInst::Create(Instruction::BitCast, value, PointerType::getUnqual(strType), "cast", block);
        const auto ptr = GetElementPtrInst::CreateInBounds(strType, cast, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 0)}, "ptr", block);
        const auto pointer = new LoadInst(type, ptr, "pointer", block);
        return pointer;
    } else {
        const auto half = CastInst::Create(Instruction::Trunc, value, type64, "half", block);
        const auto pointer = CastInst::Create(Instruction::IntToPtr, half, type, "pointer", block);
        return pointer;
    }
}

ui32 MyCompareStrings(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) {
    return NUdf::CompareStrings(lhs, rhs);
}

bool MyEquteStrings(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) {
    return NUdf::EquateStrings(lhs, rhs);
}

NUdf::THashType MyHashString(NUdf::TUnboxedValuePod val) {
    return NUdf::GetStringHash(val);
}

template <bool IsOptional>
Value* GenEqualsFunction(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block);

template <>
Value* GenEqualsFunction<false>(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto& info = NUdf::GetDataTypeInfo(slot);

    if ((info.Features & NUdf::EDataTypeFeatures::CommonType) && (info.Features & NUdf::EDataTypeFeatures::StringType || NUdf::EDataSlot::Uuid == slot || NUdf::EDataSlot::DyNumber == slot)) {
        return CallBinaryUnboxedValueFunction(&MyEquteStrings, Type::getInt1Ty(context), lv, rv, ctx.Codegen, block);
    }

    const auto lhs = GetterFor(slot, lv, context, block);
    const auto rhs = GetterFor(slot, rv, context, block);

    if (info.Features & (NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TimeIntervalType | NUdf::EDataTypeFeatures::DecimalType) || NUdf::EDataSlot::Bool == slot) {
        const auto equal = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "equal", block);
        return equal;
    }

    if (info.Features & NUdf::EDataTypeFeatures::FloatType) {
        const auto ueq = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UEQ, lhs, rhs, "equals", block);
        const auto lord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(lhs->getType(), 0.0), lhs, "lord", block);
        const auto runo = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(rhs->getType(), 0.0), rhs, "runo", block);
        const auto once = BinaryOperator::CreateXor(lord, runo, "xor", block);
        return BinaryOperator::CreateAnd(ueq, once, "and", block);
    }

    if (info.Features & NUdf::EDataTypeFeatures::TzDateType) {
        const auto ltz = GetterForTimezone(context, lv, block);
        const auto rtz = GetterForTimezone(context, rv, block);

        const auto one = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "one", block);
        const auto two = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, ltz, rtz, "two", block);

        return BinaryOperator::CreateAnd(one, two, "and", block);
    }

    return nullptr;
}

template <>
Value* GenEqualsFunction<true>(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto tiny = BasicBlock::Create(context, "tiny", ctx.Func);
    const auto test = BasicBlock::Create(context, "test", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto res = PHINode::Create(Type::getInt1Ty(context), 2U, "result", done);

    const auto le = IsEmpty(lv, block);
    const auto re = IsEmpty(rv, block);

    const auto any = BinaryOperator::CreateOr(le, re, "or", block);

    BranchInst::Create(tiny, test, any, block);

    block = tiny;

    const auto both = BinaryOperator::CreateAnd(le, re, "and", block);
    res->addIncoming(both, block);
    BranchInst::Create(done, block);

    block = test;

    const auto comp = GenEqualsFunction<false>(slot, lv, rv, ctx, block);
    res->addIncoming(comp, block);
    BranchInst::Create(done, block);

    block = done;
    return res;
}

Value* GenEqualsFunction(NUdf::EDataSlot slot, bool isOptional, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    return isOptional ? GenEqualsFunction<true>(slot, lv, rv, ctx, block) : GenEqualsFunction<false>(slot, lv, rv, ctx, block);
}

template <bool IsOptional>
Value* GenCompareFunction(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block);

template <>
Value* GenCompareFunction<false>(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto& info = NUdf::GetDataTypeInfo(slot);

    if ((info.Features & NUdf::EDataTypeFeatures::CommonType) && (info.Features & NUdf::EDataTypeFeatures::StringType || NUdf::EDataSlot::Uuid == slot || NUdf::EDataSlot::DyNumber == slot)) {
        return CallBinaryUnboxedValueFunction(&MyCompareStrings, Type::getInt32Ty(context), lv, rv, ctx.Codegen, block);
    }

    const bool extra = info.Features & (NUdf::EDataTypeFeatures::FloatType | NUdf::EDataTypeFeatures::TzDateType);
    const auto resultType = Type::getInt32Ty(context);

    const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
    const auto test = BasicBlock::Create(context, "test", ctx.Func);

    const auto res = PHINode::Create(resultType, extra ? 3U : 2U, "result", exit);

    const auto lhs = GetterFor(slot, lv, context, block);
    const auto rhs = GetterFor(slot, rv, context, block);

    if (info.Features & NUdf::EDataTypeFeatures::FloatType) {
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto uno = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, lhs, rhs, "unorded", block);

        BranchInst::Create(more, next, uno, block);
        block = more;

        const auto luno = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(lhs->getType(), 0.0), lhs, "luno", block);
        const auto runo = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(rhs->getType(), 0.0), rhs, "runo", block);
        const auto once = BinaryOperator::CreateXor(luno, runo, "xor", block);

        const auto left = SelectInst::Create(luno, ConstantInt::get(resultType, 1), ConstantInt::get(resultType, -1), "left", block);
        const auto both = SelectInst::Create(once, left, ConstantInt::get(resultType, 0), "both", block);

        res->addIncoming(both, block);
        BranchInst::Create(exit, block);

        block = next;
    }

    const auto equals = info.Features & NUdf::EDataTypeFeatures::FloatType ?
        CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OEQ, lhs, rhs, "equals", block):
        CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "equals", block);

    if (info.Features & NUdf::EDataTypeFeatures::TzDateType) {
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        BranchInst::Create(more, test, equals, block);

        block = more;

        const auto ltz = GetterForTimezone(context, lv, block);
        const auto rtz = GetterForTimezone(context, rv, block);
        const auto tzeq = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, ltz, rtz, "tzeq", block);
        res->addIncoming(ConstantInt::get(resultType, 0), block);
        BranchInst::Create(exit, next, tzeq, block);

        block = next;
        const auto tzlt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, ltz, rtz, "tzlt", block);
        const auto tzout = SelectInst::Create(tzlt, ConstantInt::get(resultType, -1), ConstantInt::get(resultType, 1), "tzout", block);
        res->addIncoming(tzout, block);
        BranchInst::Create(exit, block);
    } else {
        res->addIncoming(ConstantInt::get(resultType, 0), block);
        BranchInst::Create(exit, test, equals, block);
    }

    block = test;

    const auto less = info.Features & NUdf::EDataTypeFeatures::FloatType ?
        CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OLT, lhs, rhs, "less", block): // float
        info.Features & (NUdf::EDataTypeFeatures::SignedIntegralType | NUdf::EDataTypeFeatures::TimeIntervalType | NUdf::EDataTypeFeatures::DecimalType) ?
        CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, lhs, rhs, "less", block): // signed
        info.Features & (NUdf::EDataTypeFeatures::UnsignedIntegralType | NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType) ?
        CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, lhs, rhs, "less", block): // unsigned
        rhs; // bool

    const auto out = SelectInst::Create(less, ConstantInt::get(resultType, -1), ConstantInt::get(resultType, 1), "out", block);
    res->addIncoming(out, block);
    BranchInst::Create(exit, block);

    block = exit;
    return res;
}

template <>
Value* GenCompareFunction<true>(NUdf::EDataSlot slot, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto tiny = BasicBlock::Create(context, "tiny", ctx.Func);
    const auto side = BasicBlock::Create(context, "side", ctx.Func);
    const auto test = BasicBlock::Create(context, "test", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto resultType = Type::getInt32Ty(context);
    const auto res = PHINode::Create(resultType, 3U, "result", done);

    const auto le = IsEmpty(lv, block);
    const auto re = IsEmpty(rv, block);

    const auto any = BinaryOperator::CreateOr(le, re, "or", block);

    BranchInst::Create(tiny, test, any, block);

    block = tiny;

    const auto both = BinaryOperator::CreateAnd(le, re, "and", block);
    res->addIncoming(ConstantInt::get(resultType, 0), block);
    BranchInst::Create(done, side, both, block);

    block = side;

    const auto out = SelectInst::Create(le, ConstantInt::get(resultType, -1), ConstantInt::get(resultType, 1), "out", block);
    res->addIncoming(out, block);
    BranchInst::Create(done, block);

    block = test;

    const auto comp = GenCompareFunction<false>(slot, lv, rv, ctx, block);
    res->addIncoming(comp, block);
    BranchInst::Create(done, block);

    block = done;
    return res;
}

Value* GenCompareFunction(NUdf::EDataSlot slot, bool isOptional, Value* lv, Value* rv, TCodegenContext& ctx, BasicBlock*& block) {
    return isOptional ? GenCompareFunction<true>(slot, lv, rv, ctx, block) : GenCompareFunction<false>(slot, lv, rv, ctx, block);
}

Value* GenCombineHashes(Value* first, Value* second, BasicBlock* block) {
//    key += ~(key << 32);
    const auto x01 = BinaryOperator::CreateShl(first, ConstantInt::get(first->getType(), 32), "x01", block);
    const auto x02 = BinaryOperator::CreateXor(x01, ConstantInt::get(x01->getType(), ~0), "x02", block);
    const auto x03 = BinaryOperator::CreateAdd(x02, first, "x03", block);
//    key ^= (key >> 22);
    const auto x04 = BinaryOperator::CreateLShr(x03, ConstantInt::get(x03->getType(), 22), "x04", block);
    const auto x05 = BinaryOperator::CreateXor(x04, x03, "x05", block);
//    key += ~(key << 13);
    const auto x06 = BinaryOperator::CreateShl(x05, ConstantInt::get(x05->getType(), 13), "x06", block);
    const auto x07 = BinaryOperator::CreateXor(x06, ConstantInt::get(x06->getType(), ~0), "x07", block);
    const auto x08 = BinaryOperator::CreateAdd(x05, x07, "x08", block);
//    key ^= (key >> 8);
    const auto x09 = BinaryOperator::CreateLShr(x08, ConstantInt::get(x08->getType(), 8), "x09", block);
    const auto x10 = BinaryOperator::CreateXor(x08, x09, "x10", block);
//    key += (key << 3);
    const auto x11 = BinaryOperator::CreateShl(x10, ConstantInt::get(x10->getType(), 3), "x11", block);
    const auto x12 = BinaryOperator::CreateAdd(x10, x11, "x12", block);
//    key ^= (key >> 15);
    const auto x13 = BinaryOperator::CreateLShr(x12, ConstantInt::get(x12->getType(), 15), "x13", block);
    const auto x14 = BinaryOperator::CreateXor(x13, x12, "x14", block);
//    key += ~(key << 27);
    const auto x15 = BinaryOperator::CreateShl(x14, ConstantInt::get(x14->getType(), 27), "x15", block);
    const auto x16 = BinaryOperator::CreateXor(x15, ConstantInt::get(x15->getType(), ~0), "x16", block);
    const auto x17 = BinaryOperator::CreateAdd(x14, x16, "x17", block);
//    key ^= (key >> 31);
    const auto x18 = BinaryOperator::CreateLShr(x17, ConstantInt::get(x17->getType(), 31), "x18", block);
    const auto x19 = BinaryOperator::CreateXor(x17, x18, "x19", block);

    return BinaryOperator::CreateXor(x19, second, "both", block);
}

template <bool IsOptional>
Value* GenHashFunction(NUdf::EDataSlot slot, Value* value, TCodegenContext& ctx, BasicBlock*& block);

template <>
Value* GenHashFunction<false>(NUdf::EDataSlot slot, Value* value, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto& info = NUdf::GetDataTypeInfo(slot);

    if ((info.Features & NUdf::EDataTypeFeatures::CommonType) && (info.Features & NUdf::EDataTypeFeatures::StringType || NUdf::EDataSlot::Uuid == slot || NUdf::EDataSlot::DyNumber == slot)) {
        return CallUnaryUnboxedValueFunction(&MyHashString, Type::getInt64Ty(context), value, ctx.Codegen, block);
    }

    const auto val = GetterFor(slot, value, context, block);
    const auto hashType = Type::getInt64Ty(context);

    if (info.Features & (NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TimeIntervalType) || NUdf::EDataSlot::Bool == slot) {
        if (val->getType() == hashType) {
            return val;
        }
        const auto ext = CastInst::Create(Instruction::ZExt, val, hashType, "ext", block);
        return ext;
    }

    if (info.Features & NUdf::EDataTypeFeatures::FloatType) {
        const auto nan = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, val, val, "nan", block);
        const auto zero = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OEQ, val, ConstantFP::get(val->getType(), 0), "zero", block);
        if (NUdf::EDataSlot::Float == slot) {
            const auto cast = CastInst::Create(Instruction::BitCast, val, Type::getInt32Ty(context), "cast", block);
            const auto ext = CastInst::Create(Instruction::ZExt, cast, hashType, "ext", block);
            const auto first = SelectInst::Create(nan, ConstantInt::get(hashType, ~0), ext, "first", block);
            const auto second = SelectInst::Create(zero, ConstantInt::get(hashType, 0), first, "second", block);
            return second;
        } else {
            const auto cast = CastInst::Create(Instruction::BitCast, val, hashType, "cast", block);
            const auto first = SelectInst::Create(nan, ConstantInt::get(hashType, ~0), cast, "first", block);
            const auto second = SelectInst::Create(zero, ConstantInt::get(hashType, 0), first, "second", block);
            return second;
        }
    }

    if (info.Features & NUdf::EDataTypeFeatures::TzDateType) {
        const auto tz = GetterForTimezone(context, value, block);
        const auto ext = val->getType() == hashType ? val : CastInst::Create(Instruction::ZExt, val, hashType, "ext", block);
        const auto etz = CastInst::Create(Instruction::ZExt, tz, hashType, "etz", block);

        return GenCombineHashes(ext, etz, block);
    }

    if (info.Features & NUdf::EDataTypeFeatures::DecimalType) {
        const auto low = CastInst::Create(Instruction::Trunc, val, hashType, "low", block);
        const auto lshr = BinaryOperator::CreateLShr(val, ConstantInt::get(val->getType(), 64), "lshr", block);
        const auto high = CastInst::Create(Instruction::Trunc, lshr, hashType, "high", block);
        return GenCombineHashes(low, high, block);
    }

    return nullptr;
}

template <>
Value* GenHashFunction<true>(NUdf::EDataSlot slot, Value* value, TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto tiny = BasicBlock::Create(context, "tiny", ctx.Func);
    const auto test = BasicBlock::Create(context, "test", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto res = PHINode::Create(Type::getInt64Ty(context), 2U, "result", done);

    BranchInst::Create(tiny, test, IsEmpty(value, block), block);

    block = tiny;

    res->addIncoming(ConstantInt::get(Type::getInt64Ty(context), ~0ULL), block);
    BranchInst::Create(done, block);

    block = test;

    const auto comp = GenHashFunction<false>(slot, value, ctx, block);
    res->addIncoming(comp, block);
    BranchInst::Create(done, block);

    block = done;
    return res;
}

Value* GenHashFunction(NUdf::EDataSlot slot, bool isOptional, Value* value, TCodegenContext& ctx, BasicBlock*& block) {
    return isOptional ? GenHashFunction<true>(slot, value, ctx, block) : GenHashFunction<false>(slot, value, ctx, block);
}

Value* LoadIfPointer(Value* value, BasicBlock* block) {
    return value->getType()->isPointerTy() ? new LoadInst(value->getType()->getPointerElementType(), value, "load_value", block) : value;
}

}

Function* GenerateEqualsFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, bool isTuple, const TKeyTypes& types) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto ptrType = PointerType::getUnqual(valueType);
    const auto returnType = Type::getInt1Ty(context);

    const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(returnType, {valueType, valueType}, false):
        FunctionType::get(returnType, {ptrType, ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    const auto lv = LoadIfPointer(&*args, block);
    const auto rv = LoadIfPointer(&*++args, block);

    if (isTuple) {
        if (types.empty()) {
            ReturnInst::Create(context, ConstantInt::getTrue(context), block);
            return ctx.Func;
        }

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

        const auto elementsType = ArrayType::get(valueType, types.size());
        const auto elementsPtrType = PointerType::getUnqual(elementsType);
        const auto elementsPtrOne = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsPtrType, lv, ctx.Codegen, block);
        const auto elementsPtrTwo = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsPtrType, rv, ctx.Codegen, block);

        const auto goodOne = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elementsPtrOne, ConstantPointerNull::get(elementsPtrType), "good_one", block);
        const auto goodTwo = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elementsPtrTwo, ConstantPointerNull::get(elementsPtrType), "good_two", block);
        const auto good = BinaryOperator::CreateAnd(goodOne, goodTwo, "good", block);

        BranchInst::Create(fast, slow, good, block);

        const auto last = types.size() - 1U;

        {
            block = fast;

            const auto elementsOne = new LoadInst(elementsType, elementsPtrOne, "elements_one", block);
            const auto elementsTwo = new LoadInst(elementsType, elementsPtrTwo, "elements_two", block);

            for (ui32 i = 0U; i < last; ++i) {
                const auto nextOne = ExtractValueInst::Create(elementsOne, i, (TString("next_one_") += ToString(i)).c_str(), block);
                const auto nextTwo = ExtractValueInst::Create(elementsTwo, i, (TString("next_two_") += ToString(i)).c_str(), block);

                const auto step = BasicBlock::Create(context, (TString("step") += ToString(i)).c_str(), ctx.Func);

                const auto test = GenEqualsFunction(types[i].first, types[i].second, nextOne, nextTwo, ctx, block);

                BranchInst::Create(step, stop, test, block);

                block = step;
            }

            const auto backOne = ExtractValueInst::Create(elementsOne, last, "back_one", block);
            const auto backTwo = ExtractValueInst::Create(elementsTwo, last, "back_two", block);

            const auto result = GenEqualsFunction(types.back().first, types.back().second, backOne, backTwo, ctx, block);
            ReturnInst::Create(context, result, block);
        }

        {
            block = slow;

            const auto elementOne = new AllocaInst(valueType, 0U, "element_one", block);
            const auto elementTwo = new AllocaInst(valueType, 0U, "element_two", block);

            const auto indexType = Type::getInt32Ty(context);

            for (ui32 i = 0U; i < last; ++i) {
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(elementOne, lv, ctx.Codegen, block, ConstantInt::get(indexType, i));
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(elementTwo, rv, ctx.Codegen, block, ConstantInt::get(indexType, i));

                const auto nextOne = new LoadInst(valueType, elementOne, (TString("next_one_") += ToString(i)).c_str(), block);
                const auto nextTwo = new LoadInst(valueType, elementTwo, (TString("next_two_") += ToString(i)).c_str(), block);

                if (NUdf::GetDataTypeInfo(types[i].first).Features & NUdf::EDataTypeFeatures::StringType) {
                    ValueRelease(EValueRepresentation::String, nextOne, ctx, block);
                    ValueRelease(EValueRepresentation::String, nextTwo, ctx, block);
                }

                const auto step = BasicBlock::Create(context, (TString("step") += ToString(i)).c_str(), ctx.Func);
                const auto test = GenEqualsFunction(types[i].first, types[i].second, nextOne, nextTwo, ctx, block);

                BranchInst::Create(step, stop, test, block);

                block = step;
            }

            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(elementOne, lv, ctx.Codegen, block, ConstantInt::get(indexType, last));
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(elementTwo, rv, ctx.Codegen, block, ConstantInt::get(indexType, last));

            const auto backOne = new LoadInst(valueType, elementOne, "back_one", block);
            const auto backTwo = new LoadInst(valueType, elementTwo, "back_two", block);

            if (NUdf::GetDataTypeInfo(types.back().first).Features & NUdf::EDataTypeFeatures::StringType) {
                ValueRelease(EValueRepresentation::String, backOne, ctx, block);
                ValueRelease(EValueRepresentation::String, backTwo, ctx, block);
            }

            const auto result = GenEqualsFunction(types.back().first, types.back().second, backOne, backTwo, ctx, block);
            ReturnInst::Create(context, result, block);
        }

        block = stop;
        ReturnInst::Create(context, ConstantInt::getFalse(context), block);

    } else {
        const auto result = GenEqualsFunction(types.front().first, types.front().second, lv, rv, ctx, block);
        ReturnInst::Create(context, result, block);
    }

    return ctx.Func;
}

Function* GenerateHashFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, bool isTuple, const TKeyTypes& types) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto ptrType = PointerType::getUnqual(valueType);
    const auto returnType = Type::getInt64Ty(context);

    const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(returnType, {valueType}, false):
        FunctionType::get(returnType, {ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    const auto arg = LoadIfPointer(&*ctx.Func->arg_begin(), block);

    if (isTuple) {
        if (types.empty()) {
            ReturnInst::Create(context, ConstantInt::get(returnType, 0), block);
            return ctx.Func;
        }

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);

        const auto elementsType = ArrayType::get(valueType, types.size());
        const auto elementsPtrType = PointerType::getUnqual(elementsType);
        const auto elementsPtr = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsPtrType, arg, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elementsPtr, ConstantPointerNull::get(elementsPtrType), "null", block);

        BranchInst::Create(slow, fast, null, block);

        {
            block = fast;

            const auto elements = new LoadInst(elementsType, elementsPtr, "elements", block);

            auto result = static_cast<Value*>(ConstantInt::get(returnType, 0));

            for (auto i = 0U; i < types.size(); ++i) {
                const auto next = ExtractValueInst::Create(elements, i, (TString("next_") += ToString(i)).c_str(), block);

                const auto plus = GenHashFunction(types[i].first, types[i].second, next, ctx, block);

                result = GenCombineHashes(result, plus, block);
            }

            ReturnInst::Create(context, result, block);
        }

        {
            block = slow;

            const auto element = new AllocaInst(valueType, 0U, "element", block);

            auto result = static_cast<Value*>(ConstantInt::get(returnType, 0));

            for (auto i = 0U; i < types.size(); ++i) {
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(element, arg, ctx.Codegen, block, ConstantInt::get(Type::getInt32Ty(context), i));

                const auto next = new LoadInst(valueType, element, (TString("next_") += ToString(i)).c_str(), block);
                if (NUdf::GetDataTypeInfo(types[i].first).Features & NUdf::EDataTypeFeatures::StringType) {
                    ValueRelease(EValueRepresentation::String, next, ctx, block);
                }

                const auto plus = GenHashFunction(types[i].first, types[i].second, next, ctx, block);

                result = GenCombineHashes(result, plus, block);
            }

            ReturnInst::Create(context, result, block);
        }

    } else {
        const auto result = GenHashFunction(types.front().first, types.front().second, arg, ctx, block);
        ReturnInst::Create(context, result, block);
    }

    return ctx.Func;
}

Function* GenerateEqualsFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, const TKeyTypes& types) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto elementsType = ArrayType::get(valueType, types.size());
    const auto ptrType = PointerType::getUnqual(elementsType);
    const auto returnType = Type::getInt1Ty(context);

    const auto funcType = FunctionType::get(returnType, {ptrType, ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    const auto lv = &*args;
    const auto rv = &*++args;

    if (types.empty()) {
        ReturnInst::Create(context, ConstantInt::getTrue(context), block);
        return ctx.Func;
    }

    const auto elementsOne = new LoadInst(elementsType, lv, "elements_one", block);
    const auto elementsTwo = new LoadInst(elementsType, rv, "elements_two", block);

    const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
    ReturnInst::Create(context, ConstantInt::getFalse(context), stop);

    const auto last = types.size() - 1U;
    for (ui32 i = 0U; i < last; ++i) {
        const auto nextOne = ExtractValueInst::Create(elementsOne, i, (TString("next_one_") += ToString(i)).c_str(), block);
        const auto nextTwo = ExtractValueInst::Create(elementsTwo, i, (TString("next_two_") += ToString(i)).c_str(), block);

        const auto step = BasicBlock::Create(context, (TString("step_") += ToString(i)).c_str(), ctx.Func);

        const auto test = GenEqualsFunction(types[i].first, types[i].second, nextOne, nextTwo, ctx, block);

        BranchInst::Create(step, stop, test, block);

        block = step;
    }

    const auto backOne = ExtractValueInst::Create(elementsOne, last, "back_one", block);
    const auto backTwo = ExtractValueInst::Create(elementsTwo, last, "back_two", block);

    const auto result = GenEqualsFunction(types.back().first, types.back().second, backOne, backTwo, ctx, block);
    ReturnInst::Create(context, result, block);

    return ctx.Func;
}

Function* GenerateHashFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, const TKeyTypes& types) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto elementsType = ArrayType::get(valueType, types.size());
    const auto ptrType = PointerType::getUnqual(elementsType);
    const auto returnType = Type::getInt64Ty(context);

    const auto funcType = FunctionType::get(returnType, {ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    if (types.empty()) {
        ReturnInst::Create(context, ConstantInt::get(returnType, 0), block);
        return ctx.Func;
    }

    const auto arg = &*ctx.Func->arg_begin();
    const auto elements = new LoadInst(elementsType, arg, "elements", block);

    if (types.size() > 1U) {
        auto result = static_cast<Value*>(ConstantInt::get(returnType, 0));

        for (auto i = 0U; i < types.size(); ++i) {
            const auto item = ExtractValueInst::Create(elements, i, (TString("item_") += ToString(i)).c_str(), block);

            const auto plus = GenHashFunction(types[i].first, types[i].second, item, ctx, block);

            result = GenCombineHashes(result, plus, block);
        }

        ReturnInst::Create(context, result, block);
    } else {
        const auto value = ExtractValueInst::Create(elements, 0, "value", block);
        const auto result = GenHashFunction(types.front().first, types.front().second, value, ctx, block);
        ReturnInst::Create(context, result, block);
    }

    return ctx.Func;
}

Function* GenerateCompareFunction(NYql::NCodegen::ICodegen& codegen, const TString& name, const TKeyTypes& types) {
    auto& module = codegen.GetModule();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    auto& context = codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto elementsType = ArrayType::get(valueType, types.size());
    const auto ptrType = PointerType::getUnqual(elementsType);
    const auto dirsType = ArrayType::get(Type::getInt1Ty(context), types.size());
    const auto ptrDirsType = PointerType::getUnqual(dirsType);
    const auto returnType = Type::getInt32Ty(context);

    const auto funcType = FunctionType::get(returnType, {ptrDirsType, ptrType, ptrType}, false);

    TCodegenContext ctx(codegen);
    ctx.AlwaysInline = true;
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();

    const auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto block = main;

    const auto dp = &*args;
    const auto lv = &*++args;
    const auto rv = &*++args;

    if (types.empty()) {
        ReturnInst::Create(context, ConstantInt::get(returnType, 0), block);
        return ctx.Func;
    }

    const auto directions = new LoadInst(dirsType, dp, "directions", block);
    const auto elementsOne = new LoadInst(elementsType, lv, "elements_one", block);
    const auto elementsTwo = new LoadInst(elementsType, rv, "elements_two", block);
    const auto zero = ConstantInt::get(returnType, 0);

    for (auto i = 0U; i < types.size(); ++i) {
        const auto nextOne = ExtractValueInst::Create(elementsOne, i, (TString("next_one_") += ToString(i)).c_str(), block);
        const auto nextTwo = ExtractValueInst::Create(elementsTwo, i, (TString("next_two_") += ToString(i)).c_str(), block);

        const auto exit = BasicBlock::Create(context, (TString("exit_") += ToString(i)).c_str(), ctx.Func);
        const auto step = BasicBlock::Create(context, (TString("step_") += ToString(i)).c_str(), ctx.Func);

        const auto test = GenCompareFunction(types[i].first, types[i].second, nextOne, nextTwo, ctx, block);
        const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, zero, test,  (TString("skip_") += ToString(i)).c_str(), block);

        BranchInst::Create(step, exit, skip, block);

        block = exit;

        const auto dir = ExtractValueInst::Create(directions, i, (TString("dir_") += ToString(i)).c_str(), block);
        const auto neg = BinaryOperator::CreateNeg(test, (TString("neg_") += ToString(i)).c_str(), block);
        const auto out = SelectInst::Create(dir, test, neg, (TString("neg_") += ToString(i)).c_str(), block);

        ReturnInst::Create(context, out, block);

        block = step;
    }

    ReturnInst::Create(context, zero, block);
    return ctx.Func;
}

void GenInvalidate(const TCodegenContext& ctx, const std::vector<std::pair<ui32, EValueRepresentation>>& invalidationSet, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto values = ctx.GetMutables();

    for (const auto& index : invalidationSet) {
        const auto invPtr = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(indexType, index.first)}, "inv_ptr", block);
        ValueUnRef(index.second, invPtr, ctx, block);
        new StoreInst(GetInvalid(context), invPtr, block);
    }
}

TUnboxedImmutableCodegeneratorNode::TUnboxedImmutableCodegeneratorNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value)
    : TUnboxedImmutableComputationNode(memInfo, std::move(value))
{}

Value* TUnboxedImmutableCodegeneratorNode::CreateGetValue(const TCodegenContext& ctx, BasicBlock*&) const {
    return ConstantInt::get(Type::getInt128Ty(ctx.Codegen.GetContext()), APInt(128, 2, reinterpret_cast<const uint64_t*>(&UnboxedValue)));
}

TUnboxedImmutableRunCodegeneratorNode::TUnboxedImmutableRunCodegeneratorNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value)
    : TUnboxedImmutableComputationNode(memInfo, std::move(value))
{}

TExternalCodegeneratorNode::TExternalCodegeneratorNode(TComputationMutables& mutables, EValueRepresentation kind)
    : TExternalComputationNode(mutables, kind)
{}

TExternalCodegeneratorRootNode::TExternalCodegeneratorRootNode(TComputationMutables& mutables, EValueRepresentation kind)
    : TExternalCodegeneratorNode(mutables, kind)
{}

NUdf::TUnboxedValue TExternalCodegeneratorRootNode::GetValue(TComputationContext& compCtx) const {
    if (compCtx.ExecuteLLVM && GetFunction)
        return GetFunction(&compCtx);
    return TExternalComputationNode::GetValue(compCtx);
}

void TExternalCodegeneratorRootNode::SetValue(TComputationContext& compCtx, NUdf::TUnboxedValue&& newValue) const {
    if (compCtx.ExecuteLLVM && SetFunction)
        return SetFunction(&compCtx, newValue.Release());

    TExternalComputationNode::SetValue(compCtx, std::move(newValue));
}

TString TExternalCodegeneratorRootNode::MakeName(const TString& method) const {
    TStringStream out;
    out << DebugString() << "::" << method << "_(" << static_cast<const void*>(this) << ").";
    return out.Str();
}

void TExternalCodegeneratorRootNode::FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) {
    if (GetValueFunc)
        GetFunction = reinterpret_cast<TGetPtr>(codegen.GetPointerToFunction(GetValueFunc));

    if (SetValueFunc)
        SetFunction = reinterpret_cast<TSetPtr>(codegen.GetPointerToFunction(SetValueFunc));
}

void TExternalCodegeneratorRootNode::GenerateFunctions(NYql::NCodegen::ICodegen& codegen) {
    GetValueFunc = GenerateGetValue(codegen);
    SetValueFunc = GenerateSetValue(codegen);
    codegen.ExportSymbol(GetValueFunc);
    codegen.ExportSymbol(SetValueFunc);
}

Function* TExternalCodegeneratorRootNode::GenerateGetValue(NYql::NCodegen::ICodegen& codegen) {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();
    const auto& name = MakeName("Get");
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto valueType = Type::getInt128Ty(context);
    const auto contextType = GetCompContextType(context);

    const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(valueType, {PointerType::getUnqual(contextType)}, false):
        FunctionType::get(Type::getVoidTy(context), {PointerType::getUnqual(valueType), PointerType::getUnqual(contextType)}, false);

    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();
    if (codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
        auto& firstArg = *args++;
        firstArg.addAttr(Attribute::StructRet);
        firstArg.addAttr(Attribute::NoAlias);
    }

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    ctx.Ctx = &*args;
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto get = CreateGetValue(ctx, main);

    if (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        ReturnInst::Create(context, get, main);
    } else {
        new StoreInst(get, &*--args, main);
        ReturnInst::Create(context, main);
    }

    return ctx.Func;
}

Function* TExternalCodegeneratorRootNode::GenerateSetValue(NYql::NCodegen::ICodegen& codegen) {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();
    const auto& name = MakeName("Set");
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto intType = Type::getInt128Ty(context);
    const auto contextType = GetCompContextType(context);
    const auto valueType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
        (Type*)PointerType::getUnqual(intType) : (Type*)intType;

    const auto funcType = FunctionType::get(Type::getVoidTy(context), {PointerType::getUnqual(contextType), valueType}, false);
    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    ctx.Ctx = &*args;
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto valueArg = &*++args;

    if (codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
        const auto value = new LoadInst(valueArg->getType()->getPointerElementType(), valueArg, "load_value", main);
        CreateSetValue(ctx, main, value);
    } else {
        CreateSetValue(ctx, main, valueArg);
    }
    ReturnInst::Create(context, main);
    return ctx.Func;
}

Value* TExternalCodegeneratorNode::CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
    if (ValueGetterBuilder) {
        llvm::Function * ValueGetter = ValueGetterBuilder(ctx);
        return CallInst::Create(ValueGetter, {ctx.Ctx}, "getter", block);
    }

    if (ValueBuilder) {
        llvm::Value * TemporaryValue = ValueBuilder(ctx);
        return LoadIfPointer(TemporaryValue, block);
    }

    MKQL_ENSURE(!Getter, "Wrong LLVM function generation order.");
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, ValueIndex)}, "value_ptr", block);
    const auto value = new LoadInst(valueType, valuePtr, "value", block);
    return value;
}

Value* TExternalCodegeneratorNode::CreateRefValue(const TCodegenContext& ctx, BasicBlock*& block) const {
    CreateInvalidate(ctx, block);

    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto values = ctx.GetMutables();
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(indexType, ValueIndex)}, "value_ptr", block);
    return valuePtr;
}

void TExternalCodegeneratorNode::CreateSetValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const {
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto values = ctx.GetMutables();
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(indexType, ValueIndex)}, "value_ptr", block);


    if (value->getType()->isPointerTy()) {
        ValueUnRef(RepresentationKind, valuePtr, ctx, block);
        const auto load = new LoadInst(valueType, value, "value", block);
        new StoreInst(load, valuePtr, block);
        new StoreInst(ConstantInt::get(load->getType(), 0), value, block);
    } else {
        if (EValueRepresentation::Embedded == RepresentationKind) {
            new StoreInst(value, valuePtr, block);
        } else {
            const auto load = new LoadInst(valueType, valuePtr, "value", block);
            const auto equal = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, load, "equal", block);

            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
            const auto refs = BasicBlock::Create(context, "refs", ctx.Func);
            BranchInst::Create(skip, refs, equal, block);

            block = refs;
            ValueUnRef(RepresentationKind, valuePtr, ctx, block);
            new StoreInst(value, valuePtr, block);
            ValueAddRef(RepresentationKind, valuePtr, ctx, block);

            BranchInst::Create(skip, block);
            block = skip;
        }

    }
    CreateInvalidate(ctx, block);
}

Value* TExternalCodegeneratorNode::CreateSwapValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const {
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto values = ctx.GetMutables();
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(indexType, ValueIndex)}, "value_ptr", block);
    const auto output = new LoadInst(valueType, valuePtr, "output", block);
    ValueRelease(RepresentationKind, output, ctx, block);

    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(valueType, value, "load", block);
        new StoreInst(load, valuePtr, block);
        new StoreInst(ConstantInt::get(load->getType(), 0), value, block);
    } else {
        ValueAddRef(RepresentationKind, value, ctx, block);
        new StoreInst(value, valuePtr, block);
    }

    CreateInvalidate(ctx, block);
    return output;
}

void TExternalCodegeneratorNode::CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const {
    GenInvalidate(ctx, InvalidationSet, block);
}

void TExternalCodegeneratorNode::SetValueBuilder(TValueBuilder valueBuilder)
{
    ValueBuilder = std::move(valueBuilder);
}

void TExternalCodegeneratorNode::SetValueGetterBuilder(TValueGetterBuilder valueGetterBuilder)
{
    ValueGetterBuilder = std::move(valueGetterBuilder);
}

void TWideFlowProxyCodegeneratorNode::CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const {
    GenInvalidate(ctx, InvalidationSet, block);
}

void TWideFlowProxyCodegeneratorNode::SetGenerator(TGenerator&& generator) {
    Generator = std::move(generator);
}

ICodegeneratorInlineWideNode::TGenerateResult
TWideFlowProxyCodegeneratorNode::GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
    return Generator(ctx, block);
}

Value* GetOptionalValue(LLVMContext& context, Value* value, BasicBlock* block) {
    const auto type = Type::getInt128Ty(context);
    const auto data = ConstantInt::get(type, 0xFFFFFFFFFFFFFFFFULL);
    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, value, data, "check", block);
    const auto decr = BinaryOperator::CreateSub(value, ConstantInt::get(type, 1), "decr", block);
    const auto result = SelectInst::Create(check, value, decr, "result", block);
    return result;
}

Value* MakeOptional(LLVMContext& context, Value* value, BasicBlock* block) {
    const auto type = Type::getInt128Ty(context);
    const auto data = ConstantInt::get(type, 0xFFFFFFFFFFFFFFFFULL);
    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, value, data, "check", block);
    const auto incr = BinaryOperator::CreateAdd(value, ConstantInt::get(type, 1), "incr", block);
    const auto result = SelectInst::Create(check, value, incr, "result", block);
    return result;
}

ConstantInt* GetTrue(LLVMContext &context) {
    const uint64_t init[] = {1ULL, 0x100000000000000ULL};
    return ConstantInt::get(context, APInt(128, 2, init));
}

ConstantInt* GetFalse(LLVMContext &context) {
    const uint64_t init[] = {0ULL, 0x100000000000000ULL};
    return ConstantInt::get(context, APInt(128, 2, init));
}

ConstantInt* GetDecimalPlusInf(LLVMContext &context) {
    const auto& pair = NYql::NDecimal::MakePair(+NYql::NDecimal::Inf());
    const uint64_t init[] = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init));
}

ConstantInt* GetDecimalMinusInf(LLVMContext &context) {
    const auto& pair = NYql::NDecimal::MakePair(-NYql::NDecimal::Inf());
    const uint64_t init[] = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init));
}

ConstantInt* GetDecimalNan(LLVMContext &context) {
    const auto& pair = NYql::NDecimal::MakePair(NYql::NDecimal::Nan());
    const uint64_t init[] = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init));
}


ConstantInt* GetDecimalMinusNan(LLVMContext &context) {
    const auto& pair = NYql::NDecimal::MakePair(-NYql::NDecimal::Nan());
    const uint64_t init[] = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init));
}

static constexpr ui64 InvalidData = std::numeric_limits<ui64>::max();
static constexpr ui64 FinishData = InvalidData - 1ULL;
static constexpr ui64 YieldData = InvalidData;

ConstantInt* GetEmpty(LLVMContext &context) {
    return ConstantInt::get(Type::getInt128Ty(context), 0ULL);
}

ConstantInt* GetInvalid(LLVMContext &context) {
    return ConstantInt::get(Type::getInt128Ty(context), InvalidData);
}

ConstantInt* GetFinish(LLVMContext &context) {
    return ConstantInt::get(Type::getInt128Ty(context), FinishData);
}

ConstantInt* GetYield(LLVMContext &context) {
    return ConstantInt::get(Type::getInt128Ty(context), YieldData);
}

ConstantInt* GetConstant(ui64 value, LLVMContext &context) {
    const uint64_t init[] = {value, 0x100000000000000ULL};
    return ConstantInt::get(context, APInt(128, 2, init));
}

Value* IsExists(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, v, ConstantInt::get(v->getType(), 0ULL), "exists", block);
}

Value* IsEmpty(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, v, ConstantInt::get(v->getType(), 0ULL), "empty", block);
}

Value* IsInvalid(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, v, ConstantInt::get(v->getType(), InvalidData), "invalid", block);
}

Value* IsValid(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, v, ConstantInt::get(v->getType(), InvalidData), "valid", block);
}

Value* IsFinish(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, v, ConstantInt::get(v->getType(), FinishData), "finish", block);
}

Value* IsYield(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, v, ConstantInt::get(v->getType(), YieldData), "yield", block);
}

Value* IsSpecial(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return BinaryOperator::CreateOr(IsFinish(v, block), IsYield(v, block), "special", block);
}

Value* HasValue(Value* value, BasicBlock* block) {
    const auto v = LoadIfPointer(value, block);
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, v, ConstantInt::get(v->getType(), InvalidData), "has", block);
}

Value* MakeBoolean(Value* boolean , LLVMContext &context, BasicBlock* block) {
    return SelectInst::Create(boolean, GetTrue(context), GetFalse(context), "result", block);
}

Value* SetterForInt128(Value* value, BasicBlock* block) {
    const uint64_t mask[] = {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFULL};
    const auto drop = ConstantInt::get(value->getType(), APInt(128, 2, mask));
    const auto data = BinaryOperator::CreateAnd(value, drop, "and", block);
    const uint64_t init[] = {0ULL, 0x100000000000000ULL}; // Embedded
    const auto meta = ConstantInt::get(value->getType(), APInt(128, 2, init));
    const auto full = BinaryOperator::CreateOr(data, meta, "or", block);
    return full;
}

Value* GetterForInt128(Value* value, BasicBlock* block) {
    const uint64_t init[] = {0ULL, 0x80000000000000ULL};
    const auto test = ConstantInt::get(value->getType(), APInt(128, 2, init));
    const auto sign = BinaryOperator::CreateAnd(value, test, "and", block);

    const uint64_t fill[] = {0ULL, 0xFF00000000000000ULL};
    const auto sext = ConstantInt::get(value->getType(), APInt(128, 2, fill));
    const auto minus = BinaryOperator::CreateOr(value, sext, "or", block);

    const uint64_t mask[] = {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFULL};
    const auto trun = ConstantInt::get(value->getType(), APInt(128, 2, mask));
    const auto plus = BinaryOperator::CreateAnd(value, trun, "and", block);

    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, sign, ConstantInt::get(sign->getType(), 0), "check", block);
    const auto result = SelectInst::Create(check, plus, minus, "result", block);
    return result;
}

Value* GetterForTimezone(LLVMContext& context, Value* value, BasicBlock* block) {
    const auto lshr = BinaryOperator::CreateLShr(value, ConstantInt::get(value->getType(), 64ULL), "lshr", block);
    const auto trunc = CastInst::Create(Instruction::Trunc, lshr, Type::getInt16Ty(context), "trunc", block);
    return trunc;
}

template<> Type* GetTypeFor<bool>(LLVMContext &context) { return Type::getInt1Ty(context); }
template<> Type* GetTypeFor<ui8>(LLVMContext &context)  { return Type::getInt8Ty(context); }
template<> Type* GetTypeFor<i8>(LLVMContext &context)  { return Type::getInt8Ty(context); }

template<> Type* GetTypeFor<i16>(LLVMContext &context)  { return Type::getInt16Ty(context); }
template<> Type* GetTypeFor<ui16>(LLVMContext &context) { return Type::getInt16Ty(context); }

template<> Type* GetTypeFor<i32>(LLVMContext &context)  { return Type::getInt32Ty(context); }
template<> Type* GetTypeFor<ui32>(LLVMContext &context) { return Type::getInt32Ty(context); }

template<> Type* GetTypeFor<i64>(LLVMContext &context)  { return Type::getInt64Ty(context); }
template<> Type* GetTypeFor<ui64>(LLVMContext &context) { return Type::getInt64Ty(context); }

template<> Type* GetTypeFor<float>(LLVMContext &context)  { return Type::getFloatTy(context); }
template<> Type* GetTypeFor<double>(LLVMContext &context) { return Type::getDoubleTy(context); }

void AddRefBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();
    const auto load = value->getType()->isPointerTy() ? new LoadInst(value->getType()->getPointerElementType(), value, "load", block) : value;
    const auto half = CastInst::Create(Instruction::Trunc, load, Type::getInt64Ty(context), "half", block);
    const auto counterType = Type::getInt32Ty(context);
    const auto type = StructType::get(context, {PointerType::getUnqual(StructType::get(context)), counterType, Type::getInt16Ty(context)});
    const auto boxptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type), "boxptr", block);
    const auto cntptr = GetElementPtrInst::CreateInBounds(type, boxptr, {ConstantInt::get(Type::getInt32Ty(context), 0), ConstantInt::get(Type::getInt32Ty(context), 1)}, "cntptr", block);
    const auto refs = new LoadInst(counterType, cntptr, "refs", block);
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if constexpr (EnableStaticRefcount) {
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto magic = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, refs, ConstantInt::get(refs->getType(), 0), "magic", block);
        BranchInst::Create(skip, work, magic, block);

        block = work;
        const auto incr = BinaryOperator::CreateAdd(refs, ConstantInt::get(refs->getType(), 1), "incr", block);
        new StoreInst(incr, cntptr, block);
        BranchInst::Create(skip, block);

        block = skip;
    } else {
        const auto incr = BinaryOperator::CreateAdd(refs, ConstantInt::get(refs->getType(), 1), "incr", block);
        new StoreInst(incr, cntptr, block);
    }
#else
    const auto incr = BinaryOperator::CreateAdd(refs, ConstantInt::get(refs->getType(), 1), "incr", block);
    new StoreInst(incr, cntptr, block);
#endif
}

void UnRefBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();
    const auto load = value->getType()->isPointerTy() ? new LoadInst(value->getType()->getPointerElementType(), value, "load", block) : value;
    const auto half = CastInst::Create(Instruction::Trunc, load, Type::getInt64Ty(context), "half", block);
    const auto counterType = Type::getInt32Ty(context);
    const auto type = StructType::get(context, {PointerType::getUnqual(StructType::get(context)), counterType, Type::getInt16Ty(context)});
    const auto boxptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type), "boxptr", block);
    const auto cntptr = GetElementPtrInst::CreateInBounds(type, boxptr, {ConstantInt::get(Type::getInt32Ty(context), 0), ConstantInt::get(Type::getInt32Ty(context), 1)}, "cntptr", block);
    const auto refs = new LoadInst(counterType, cntptr, "refs", block);

    const auto live = BasicBlock::Create(context, "live", ctx.Func);

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if constexpr (EnableStaticRefcount) {
        const auto magic = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, refs, ConstantInt::get(refs->getType(), 0), "magic", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        BranchInst::Create(live, work, magic, block);

        block = work;
    }
#endif

    const auto decr = BinaryOperator::CreateSub(refs, ConstantInt::get(refs->getType(), 1), "decr", block);
    new StoreInst(decr, cntptr, block);
    const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, decr, ConstantInt::get(decr->getType(), 0), "many", block);

    const auto kill = BasicBlock::Create(context, "kill", ctx.Func);

    BranchInst::Create(live, kill, test, block);

    block = kill;

    const auto fnType = FunctionType::get(Type::getVoidTy(context), {boxptr->getType()}, false);
    const auto name = "DeleteBoxed";
    ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DeleteBoxed));
    const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType).getCallee();
    CallInst::Create(fnType, func, {boxptr}, "", block);

    BranchInst::Create(live, block);
    block = live;
}

void CleanupBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();
    const auto load = value->getType()->isPointerTy() ? new LoadInst(value->getType()->getPointerElementType(), value, "load", block) : value;
    const auto half = CastInst::Create(Instruction::Trunc, load, Type::getInt64Ty(context), "half", block);
    const auto counterType = Type::getInt32Ty(context);
    const auto type = StructType::get(context, {PointerType::getUnqual(StructType::get(context)), counterType, Type::getInt16Ty(context)});
    const auto boxptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type), "boxptr", block);
    const auto cntptr = GetElementPtrInst::CreateInBounds(type, boxptr, {ConstantInt::get(Type::getInt32Ty(context), 0), ConstantInt::get(Type::getInt32Ty(context), 1)}, "cntptr", block);
    const auto refs = new LoadInst(counterType, cntptr, "refs", block);
    const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, refs, ConstantInt::get(refs->getType(), 0), "many", block);

    const auto live = BasicBlock::Create(context, "live", ctx.Func);
    const auto kill = BasicBlock::Create(context, "kill", ctx.Func);

    BranchInst::Create(live, kill, test, block);

    block = kill;

    const auto fnType = FunctionType::get(Type::getVoidTy(context), {boxptr->getType()}, false);
    const auto name = "DeleteBoxed";
    ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DeleteBoxed));
    const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType).getCallee();
    CallInst::Create(fnType, func, {boxptr}, "", block);

    BranchInst::Create(live, block);
    block = live;
}


template<bool IncOrDec>
void ChangeRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto type8 = Type::getInt8Ty(context);
    const auto type32 = Type::getInt32Ty(context);

    const auto mark = GetMarkFromUnboxed(value, ctx, block);

    const auto boxb = BasicBlock::Create(context, "boxb", ctx.Func);
    const auto strb = BasicBlock::Create(context, "strb", ctx.Func);
    const auto doit = BasicBlock::Create(context, "doit", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto refsPtrType = PointerType::getUnqual(type32);
    const auto refsptr = PHINode::Create(refsPtrType, 2U, "refsptr", doit);

    const auto choise = SwitchInst::Create(mark, done, 2U, block);
    choise->addCase(ConstantInt::get(type8, 2), strb);
    choise->addCase(ConstantInt::get(type8, 3), boxb);

    {
        block = strb;

        const auto strptr = GetPointerFromUnboxed<false>(value, ctx, block);
        const auto elemptr = GetElementPtrInst::CreateInBounds(strptr->getType()->getPointerElementType(), strptr, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1)}, "elemptr", block);
        refsptr->addIncoming(elemptr, block);
        BranchInst::Create(doit, block);
    }

    {
        block = boxb;

        const auto boxptr = GetPointerFromUnboxed<true>(value, ctx, block);;
        const auto elemptr = GetElementPtrInst::CreateInBounds(boxptr->getType()->getPointerElementType(), boxptr, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1)}, "elemptr", block);
        refsptr->addIncoming(elemptr, block);
        BranchInst::Create(doit, block);
    }

    block = doit;

    const auto refs = new LoadInst(type32, refsptr, "refs", block);
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if constexpr (EnableStaticRefcount) {
        const auto magic = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, refs, ConstantInt::get(refs->getType(), 0), "magic", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        BranchInst::Create(done, work, magic, block);

        block = work;
    }
#endif
    const auto next = IncOrDec ?
        BinaryOperator::CreateAdd(refs, ConstantInt::get(refs->getType(), 1), "incr", block):
        BinaryOperator::CreateSub(refs, ConstantInt::get(refs->getType(), 1), "decr", block);
    new StoreInst(next, refsptr, block);
    BranchInst::Create(done, block);

    block = done;
}

template<bool Decrement>
void CheckRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto type8 = Type::getInt8Ty(context);
    const auto type32 = Type::getInt32Ty(context);

    const auto mark = GetMarkFromUnboxed(value, ctx, block);

    const auto boxb = BasicBlock::Create(context, "boxb", ctx.Func);
    const auto strb = BasicBlock::Create(context, "strb", ctx.Func);
    const auto nope = BasicBlock::Create(context, "nope", ctx.Func);

    const auto choise = SwitchInst::Create(mark, nope, 2U, block);
    choise->addCase(ConstantInt::get(type8, 2), strb);
    choise->addCase(ConstantInt::get(type8, 3), boxb);

    {
        block = strb;

        const auto strptr = GetPointerFromUnboxed<false>(value, ctx, block);
        const auto refptr = GetElementPtrInst::CreateInBounds(strptr->getType()->getPointerElementType(), strptr, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1)}, "refptr", block);
        const auto refs = new LoadInst(type32, refptr, "refs", block);

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
        if constexpr (EnableStaticRefcount) {
            const auto magic = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, refs, ConstantInt::get(refs->getType(), 0), "magic", block);

            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            BranchInst::Create(nope, work, magic, block);

            block = work;
        }
#endif
        Value* test = refs;

        if constexpr (Decrement) {
            const auto decr = BinaryOperator::CreateSub(refs, ConstantInt::get(refs->getType(), 1), "decr", block);
            new StoreInst(decr, refptr, block);
            test = decr;
        }

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, test, ConstantInt::get(test->getType(), 0), "test", block);

        const auto free = BasicBlock::Create(context, "free", ctx.Func);

        BranchInst::Create(nope, free, good, block);

        block = free;

        const auto fnType = FunctionType::get(Type::getVoidTy(context), {strptr->getType()}, false);
        const auto name = "DeleteString";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DeleteString));
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType).getCallee();
        CallInst::Create(fnType, func, {strptr}, "", block);
        BranchInst::Create(nope, block);
    }

    {
        block = boxb;

        const auto boxptr = GetPointerFromUnboxed<true>(value, ctx, block);;
        const auto refptr = GetElementPtrInst::CreateInBounds(boxptr->getType()->getPointerElementType(), boxptr, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1)}, "cntptr", block);
        const auto refs = new LoadInst(type32, refptr, "refs", block);

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
        if constexpr (EnableStaticRefcount) {
            const auto magic = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, refs, ConstantInt::get(refs->getType(), 0), "magic", block);

            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            BranchInst::Create(nope, work, magic, block);

            block = work;
        }
#endif

        Value* test = refs;

        if constexpr (Decrement) {
            const auto decr = BinaryOperator::CreateSub(refs, ConstantInt::get(refs->getType(), 1), "decr", block);
            new StoreInst(decr, refptr, block);
            test = decr;
        }

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, test, ConstantInt::get(test->getType(), 0), "test", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);

        BranchInst::Create(nope, kill, good, block);

        block = kill;

        const auto fnType = FunctionType::get(Type::getVoidTy(context), {boxptr->getType()}, false);
        const auto name = "DeleteBoxed";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DeleteBoxed));
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType).getCallee();

        CallInst::Create(fnType, func, {boxptr}, "", block);
        BranchInst::Create(nope, block);
    }

    block = nope;
}
#ifdef MAKE_UNBOXED_VALUE_LLVM_REFCOUNTION_FUNCTIONS
Function* GenRefCountFunction(const char* label, void (*func)(Value*, const TCodegenContext&, BasicBlock*&), Type* type, NYql::NCodegen::ICodegen& codegen) {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();
    const auto name = TString(label) += (type->isPointerTy() ? "Ptr" : "Val");
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto funcType = FunctionType::get(Type::getVoidTy(context), {type}, false);

    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee()).getCallee();

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    auto value = &*ctx.Func->arg_begin();

    func(value, ctx, main);
    ReturnInst::Create(context, main);
    return ctx.Func;
}

void AddRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    CallInst::Create(GenRefCountFunction(__func__, &ChangeRefUnboxed<true>, value->getType(), ctx.Codegen), {value}, "", block);
}

void ReleaseUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    CallInst::Create(GenRefCountFunction(__func__, &ChangeRefUnboxed<false>, value->getType(), ctx.Codegen), {value}, "", block);
}

void UnRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    CallInst::Create(GenRefCountFunction(__func__, &CheckRefUnboxed<true>, value->getType(), ctx.Codegen), {value}, "", block);
}

void CleanupUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    CallInst::Create(GenRefCountFunction(__func__, &CheckRefUnboxed<false>, value->getType(), ctx.Codegen), {value}, "", block);
}
#else
void AddRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    return ChangeRefUnboxed<true>(value, ctx, block);
}

void ReleaseUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    return ChangeRefUnboxed<false>(value, ctx, block);
}

void UnRefUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    return CheckRefUnboxed<true>(value, ctx, block);
}

void CleanupUnboxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block) {
    return CheckRefUnboxed<false>(value, ctx, block);
}
#endif

void SafeUnRefUnboxed(Value* pointer, const TCodegenContext& ctx, BasicBlock*& block) {
    if (const auto itemType = pointer->getType()->getPointerElementType(); itemType->isArrayTy()) {
        const auto indexType = Type::getInt64Ty(ctx.Codegen.GetContext());
        Value* zeros = UndefValue::get(itemType);
        for (ui32 idx = 0U; idx < itemType->getArrayNumElements(); ++idx) {
            const auto item = GetElementPtrInst::CreateInBounds(itemType, pointer, {  ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx) }, (TString("item_") += ToString(idx)).c_str(), block);
            UnRefUnboxed(item, ctx, block);
            zeros = InsertValueInst::Create(zeros, ConstantInt::get(itemType->getArrayElementType(), 0), {idx}, (TString("zero_") += ToString(idx)).c_str(), block);
        }
        new StoreInst(zeros, pointer, block);
    } else {
        UnRefUnboxed(pointer, ctx, block);
        new StoreInst(ConstantInt::get(itemType, 0), pointer, block);
    }
}

void ValueAddRef(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block) {
    switch (kind) {
        case EValueRepresentation::Embedded: return;
        case EValueRepresentation::Boxed:   // TODO
        case EValueRepresentation::String: // TODO
        case EValueRepresentation::Any:    return AddRefUnboxed(pointer, ctx, block);
    }
}

void ValueUnRef(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block) {
    switch (kind) {
        case EValueRepresentation::Embedded: return;
        case EValueRepresentation::Boxed:   // TODO
        case EValueRepresentation::String: // TODO
        case EValueRepresentation::Any:    return UnRefUnboxed(pointer, ctx, block);
    }
}

void ValueCleanup(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block) {
    switch (kind) {
        case EValueRepresentation::Embedded: return;
        case EValueRepresentation::Boxed:   // TODO
        case EValueRepresentation::String: // TODO
        case EValueRepresentation::Any:    return CleanupUnboxed(pointer, ctx, block);
    }
}

void ValueRelease(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block) {
    switch (kind) {
        case EValueRepresentation::Embedded: return;
        case EValueRepresentation::Boxed:   // TODO
        case EValueRepresentation::String: // TODO
        case EValueRepresentation::Any:    return ReleaseUnboxed(pointer, ctx, block);
    }
}

std::pair<Value*, Value*> GetVariantParts(Value* variant, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto type = Type::getInt32Ty(context);
    const auto lshr = BinaryOperator::CreateLShr(variant, ConstantInt::get(variant->getType(), 122), "lshr",  block);
    const auto trunc = CastInst::Create(Instruction::Trunc, lshr, type, "trunc", block);

    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, trunc, ConstantInt::get(type , 0), "check", block);

    const auto boxed = BasicBlock::Create(context, "boxed", ctx.Func);
    const auto embed = BasicBlock::Create(context, "embed", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto index = PHINode::Create(type, 2U, "index", done);
    const auto item = PHINode::Create(variant->getType(), 2U, "index", done);

    BranchInst::Create(embed, boxed, check, block);

    {
        block = embed;

        const uint64_t init[] = {0xFFFFFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFFULL};
        const auto mask = ConstantInt::get(variant->getType(), APInt(128, 2, init));
        const auto clean = BinaryOperator::CreateAnd(variant, mask, "clean",  block);

        const auto dec = BinaryOperator::CreateSub(trunc, ConstantInt::get(type, 1), "dec",  block);
        index->addIncoming(dec, block);
        item->addIncoming(clean, block);
        BranchInst::Create(done, block);
    }

    {
        block = boxed;

        const auto place = new AllocaInst(item->getType(), 0U, "place", &ctx.Func->getEntryBlock().back());
        const auto idx = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetVariantIndex>(type, variant, ctx.Codegen, block);
        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetVariantItem>(place, variant, ctx.Codegen, block);
        const auto clean = new LoadInst(item->getType(), place, "clean",  block);
        ValueRelease(EValueRepresentation::Any, clean, ctx, block);
        index->addIncoming(idx, block);
        item->addIncoming(clean, block);
        BranchInst::Create(done, block);
    }

    block = done;
    return std::make_pair(index, item);
}

Value* MakeVariant(Value* item, Value* variant, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    const auto boxed = BasicBlock::Create(context, "boxed", ctx.Func);
    const auto embed = BasicBlock::Create(context, "embed", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    const auto result = PHINode::Create(item->getType(), 1U, "index", done);

    const auto offset = ConstantInt::get(item->getType(), 122);
    const auto lshr = BinaryOperator::CreateLShr(item, offset, "lshr", block);

    const auto checkItem = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lshr, ConstantInt::get(lshr->getType(), 0), "check_item", block);
    const auto checkIndex = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, variant, ConstantInt::get(variant->getType(), (1U << 6U) - 1U), "check_index", block);
    const auto check = BinaryOperator::CreateAnd(checkItem, checkIndex, "and", block);

    BranchInst::Create(embed, boxed, check, block);

    {
        block = embed;

        const auto index = BinaryOperator::CreateAdd(variant, ConstantInt::get(variant->getType(), 1), "index",  block);
        const auto extend = CastInst::Create(Instruction::ZExt, index, item->getType(), "extend", block);
        const auto shift = BinaryOperator::CreateShl(extend, offset, "shift",  block);
        const auto output = BinaryOperator::CreateOr(item, shift, "output",  block);
        result->addIncoming(output, block);
        BranchInst::Create(done, block);
    }

    {
        block = boxed;

        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CreateBoxedVariantHolder));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto signature = FunctionType::get(item->getType(), {factory->getType(), item->getType(), variant->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            const auto output = CallInst::Create(signature, creator, {factory, item, variant}, "output", block);
            result->addIncoming(output, block);
        } else {
            const auto place = new AllocaInst(item->getType(), 0U, "place", block);
            new StoreInst(item, place, block);
            const auto signature = FunctionType::get(Type::getVoidTy(context), {factory->getType(), place->getType(), place->getType(), variant->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            CallInst::Create(signature, creator, {factory, place, place, variant}, "", block);
            const auto output = new LoadInst(item->getType(), place, "output", block);
            result->addIncoming(output, block);
        }

        BranchInst::Create(done, block);
    }

    block = done;
    return result;
}

Value* GetNodeValue(IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) {
    if (const auto codegen = dynamic_cast<ICodegeneratorInlineNode*>(node))
        return codegen->CreateGetValue(ctx, block);

    auto& context = ctx.Codegen.GetContext();
    const auto ptr = ConstantInt::get(Type::getInt64Ty(context), intptr_t(node));
    const auto ptrType = PointerType::getUnqual(StructType::get(context));
    const auto nodeThis = CastInst::Create(Instruction::IntToPtr, ptr, ptrType, "node_this", block);

    const auto valueType = Type::getInt128Ty(context);
    const auto retPtr = new AllocaInst(valueType, 0U, "return_ptr", &ctx.Func->getEntryBlock().back());
    const auto funType = ctx.Codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(Type::getVoidTy(context), {retPtr->getType(), nodeThis->getType(), ctx.Ctx->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {nodeThis->getType(), retPtr->getType(), ctx.Ctx->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto nodeVTable = CastInst::Create(Instruction::IntToPtr, ptr, PointerType::getUnqual(tableType), "node_vtable", block);

    const auto table = new LoadInst(tableType, nodeVTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodIndex(&IComputationNode::GetValue))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (ctx.Codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {retPtr, nodeThis, ctx.Ctx}, "", block);
    } else {
        CallInst::Create(funType, func, {nodeThis, retPtr, ctx.Ctx}, "", block);
    }

    ValueRelease(node->GetRepresentation(), retPtr, ctx, block);
    const auto result = new LoadInst(valueType, retPtr, "return", false, block);
    return result;
}

void GetNodeValue(Value* value, IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) {
    if (const auto codegen = dynamic_cast<ICodegeneratorInlineNode*>(node)) {
        const auto v = codegen->CreateGetValue(ctx, block);
        new StoreInst(v, value, block);
        ValueAddRef(node->GetRepresentation(), value, ctx, block);
        return;
    }

    auto& context = ctx.Codegen.GetContext();
    const auto ptr = ConstantInt::get(Type::getInt64Ty(context), intptr_t(node));
    const auto ptrType = PointerType::getUnqual(StructType::get(context));
    const auto nodeThis = CastInst::Create(Instruction::IntToPtr, ptr, ptrType, "node_this", block);

    const auto funType = ctx.Codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(Type::getVoidTy(context), {value->getType(), nodeThis->getType(), ctx.Ctx->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {nodeThis->getType(), value->getType(), ctx.Ctx->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto nodeVTable = CastInst::Create(Instruction::IntToPtr, ptr, PointerType::getUnqual(tableType), "node_vtable", block);

    const auto table = new LoadInst(tableType, nodeVTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodIndex(&IComputationNode::GetValue))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (ctx.Codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {value, nodeThis, ctx.Ctx}, "", block);
    } else {
        CallInst::Create(funType, func, {nodeThis, value, ctx.Ctx}, "", block);
    }
}

ICodegeneratorInlineWideNode::TGenerateResult GetNodeValues(IComputationWideFlowNode* node, const TCodegenContext& ctx, BasicBlock*& block) {
    if (const auto codegen = dynamic_cast<ICodegeneratorInlineWideNode*>(node))
        return codegen->GenGetValues(ctx, block);
    throw TNoCodegen();
}

Value* GenNewArray(const TCodegenContext& ctx, Value* size, Value* items, BasicBlock* block) {
    auto& context = ctx.Codegen.GetContext();
    const auto fact = ctx.GetFactory();
    const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CreateDirectArrayHolder));
    const auto valueType = Type::getInt128Ty(context);
    if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
        const auto funType = FunctionType::get(valueType, {fact->getType(), size->getType(), items->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
        return CallInst::Create(funType, funcPtr, {fact, size, items}, "array", block);
    } else {
        const auto resultPtr = new AllocaInst(valueType, 0U, "return", block);
        const auto funType = FunctionType::get(Type::getVoidTy(context), {fact->getType(), resultPtr->getType(), size->getType(), items->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
        CallInst::Create(funType, funcPtr, {fact, resultPtr, size, items}, "", block);
        return new LoadInst(valueType, resultPtr, "array", block);
    }
}

Value* GetMemoryUsed(ui64 limit, const TCodegenContext& ctx, BasicBlock* block) {
    if (!limit) {
        return nullptr;
    }

    auto& context = ctx.Codegen.GetContext();
    const auto fact = ctx.GetFactory();
    const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::GetMemoryUsed));
    const auto funType = FunctionType::get(Type::getInt64Ty(context), {fact->getType()}, false);
    const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "get_used", block);
    return CallInst::Create(funType, funcPtr, {fact}, "mem_used", block);
}

template <bool TrackRss>
Value* CheckAdjustedMemLimit(ui64 limit, Value* init, const TCodegenContext& ctx, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();

    if (!limit || !init) {
        return ConstantInt::getFalse(context);
    }

    const auto indexType = Type::getInt32Ty(context);

    if constexpr (TrackRss) {
        const auto rssPtr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 5)}, "rss_ptr", block);
        const auto rss = new LoadInst(Type::getInt32Ty(context), rssPtr, "rsscounter", block);
        const auto inc = BinaryOperator::CreateAdd(rss, ConstantInt::get(rss->getType(), 1), "inc", block);
        new StoreInst(inc, rssPtr, block);
        const auto mod = BinaryOperator::CreateURem(rss, ConstantInt::get(rss->getType(), STEP_FOR_RSS_CHECK), "mod", block);
        const auto now = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, mod, ConstantInt::get(mod->getType() , 0), "now", block);

        const auto call = BasicBlock::Create(context, "call", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

        BranchInst::Create(call, skip, now, block);

        block = call;
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TComputationContext::UpdateUsageAdjustor));
        const auto funType = FunctionType::get(Type::getVoidTy(context), {ctx.Ctx->getType(), Type::getInt64Ty(context)}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "update", block);
        CallInst::Create(funType, funcPtr, {ctx.Ctx, ConstantInt::get(init->getType(), limit)}, "", block);

        BranchInst::Create(skip, block);

        block = skip;
    }

    const auto adjPtr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 4)}, "adj_ptr", block);
    const auto adjustor = new LoadInst(Type::getFloatTy(context), adjPtr, "adjustor", block);

    const auto curr = GetMemoryUsed(limit, ctx, block);
    const auto cast = CastInst::Create(Instruction::UIToFP, curr, adjustor->getType(), "cast", block);
    const auto used = BinaryOperator::CreateFMul(cast, adjustor, "used", block);
    const auto add = BinaryOperator::CreateAdd(init, ConstantInt::get(init->getType(), limit), "add", block);
    const auto upper = CastInst::Create(Instruction::UIToFP, add, adjustor->getType(), "upper", block);
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OGE, used, upper, "enough", block);
}

template Value* CheckAdjustedMemLimit<false>(ui64 limit, Value* init, const TCodegenContext& ctx, BasicBlock*& block);
template Value* CheckAdjustedMemLimit<true>(ui64 limit, Value* init, const TCodegenContext& ctx, BasicBlock*& block);

Value* WrapArgumentForWindows(Value* arg, const TCodegenContext& ctx, BasicBlock* block) {
    if (ctx.Codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        return arg;
    }

    const auto newArg = new AllocaInst(arg->getType(), 0, "argument", block);
    new StoreInst(arg, newArg, block);
    return newArg;
}

Value* CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Type* returnType, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

void CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Value* output, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output}, "", block);
    }
}

void CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Value* output, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block, Value* argument) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType(), argument->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType(), argument->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed, argument}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output, argument}, "", block);
    }
}

Value* CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Type* returnType, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block, Value* argument) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType(), argument->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed, argument}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

void CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Value* output, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block, Value* arg1, Value* arg2) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType(), arg1->getType(), arg2->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType(), arg1->getType(), arg2->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed, arg1, arg2}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output, arg1, arg2}, "", block);
    }
}

Value* CallBoxedValueVirtualMethodImpl(uintptr_t methodPtr, Type* returnType, Value* value, NYql::NCodegen::ICodegen& codegen, BasicBlock* block, Value* arg1, Value* arg2) {
    auto& context = codegen.GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType(), arg1->getType(), arg2->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(methodPtr))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed, arg1, arg2}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

Value* CallUnaryUnboxedValueFunctionImpl(uintptr_t methodPtr, Type* result, Value* arg, NYql::NCodegen::ICodegen& codegen, BasicBlock* block) {
    auto& context = codegen.GetContext();
    const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), methodPtr);
    if (NYql::NCodegen::ETarget::Windows != codegen.GetEffectiveTarget()) {
        const auto funType = FunctionType::get(result, {arg->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
        const auto call = CallInst::Create(funType, funcPtr, {arg}, "call", block);
        return call;
    } else {
        const auto ptrArg = new AllocaInst(arg->getType(), 0U, "arg", block);
        new StoreInst(arg, ptrArg, block);

        if (Type::getInt128Ty(context) == result) {
            const auto ptrResult = new AllocaInst(result, 0U, "result", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrArg}, "", block);
            const auto res = new LoadInst(result, ptrResult, "res", block);
            return res;
        } else {
            const auto funType = FunctionType::get(result, {ptrArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            const auto call = CallInst::Create(funType, funcPtr, {ptrArg}, "call", block);
            return call;
        }
    }
}

Value* CallBinaryUnboxedValueFunctionImpl(uintptr_t methodPtr, Type* result, Value* left, Value* right, NYql::NCodegen::ICodegen& codegen, BasicBlock* block) {
    auto& context = codegen.GetContext();
    const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), methodPtr);
    if (NYql::NCodegen::ETarget::Windows != codegen.GetEffectiveTarget()) {
        const auto funType = FunctionType::get(result, {left->getType(), right->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
        const auto call = CallInst::Create(funType, funcPtr, {left, right}, "call", block);
        return call;
    } else {
        const auto ptrLeft = new AllocaInst(left->getType(), 0U, "left", block);
        const auto ptrRight = new AllocaInst(right->getType(), 0U, "right", block);
        new StoreInst(left, ptrLeft, block);
        new StoreInst(right, ptrRight, block);

        if (Type::getInt128Ty(context) == result) {
            const auto ptrResult = new AllocaInst(result, 0U, "result", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrLeft->getType(), ptrRight->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrLeft, ptrRight}, "", block);
            const auto res = new LoadInst(result, ptrResult, "res", block);
            return res;
        } else {
            const auto funType = FunctionType::get(result, {ptrLeft->getType(), ptrRight->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            const auto call = CallInst::Create(funType, funcPtr, {ptrLeft, ptrRight}, "call", block);
            return call;
        }
    }
}

Y_NO_INLINE Value* TDecoratorCodegeneratorNodeBase::CreateGetValueImpl(IComputationNode* node,
    const TCodegenContext& ctx, BasicBlock*& block) const {
    const auto arg = GetNodeValue(node, ctx, block);
    const auto value = DoGenerateGetValue(ctx, arg, block);
    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), value, "load", block);
        ValueRelease(node->GetRepresentation(), load, ctx, block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE Value* TStatelessFlowCodegeneratorNodeBase::CreateGetValueImpl(const IComputationNode* node,
    const TCodegenContext& ctx, BasicBlock*& block) const {
    const auto value = DoGenerateGetValue(ctx, block);
    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), value, "load", block);
        ValueRelease(node->GetRepresentation(), load, ctx, block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE ICodegeneratorInlineWideNode::TGenerateResult TStatelessWideFlowCodegeneratorNodeBase::GenGetValuesImpl(
    const TCodegenContext& ctx, BasicBlock*& block) const {
    return DoGenGetValues(ctx, block);
}

Y_NO_INLINE Value* TFlowSourceCodegeneratorNodeBase::CreateGetValueImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), node->GetIndex())}, "state_ptr", block);

    const auto value = DoGenerateGetValue(ctx, statePtr, block);
    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(valueType, value, "load", block);
        ValueRelease(node->GetRepresentation(), load, ctx, block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE ICodegeneratorInlineWideNode::TGenerateResult TWideFlowSourceCodegeneratorNodeBase::GenGetValuesImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), node->GetIndex())}, "state_ptr", block);
    return DoGenGetValues(ctx, statePtr, block);
}

Y_NO_INLINE Value* TStatefulFlowCodegeneratorNodeBase::CreateGetValueImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), node->GetIndex())}, "state_ptr", block);

    const auto value = DoGenerateGetValue(ctx, statePtr, block);
    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(valueType, value, "load", block);
        ValueRelease(node->GetRepresentation(), load, ctx, block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE ICodegeneratorInlineWideNode::TGenerateResult TStatefulWideFlowCodegeneratorNodeBase::GenGetValuesImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), node->GetIndex())}, "state_ptr", block);
    return DoGenGetValues(ctx, statePtr, block);
}

Y_NO_INLINE ICodegeneratorInlineWideNode::TGenerateResult TPairStateWideFlowCodegeneratorNodeBase::GenGetValuesImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    auto idx = node->GetIndex();
    const auto valueType = Type::getInt128Ty(context);
    const auto firstPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), idx)}, "first_ptr", block);
    const auto secondPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), ++idx)}, "second_ptr", block);
    return DoGenGetValues(ctx, firstPtr, secondPtr, block);
}

Y_NO_INLINE Value* TPairStateFlowCodegeneratorNodeBase::CreateGetValueImpl(
    const IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    auto idx = node->GetIndex();
    const auto valueType = Type::getInt128Ty(context);
    const auto firstPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), idx)}, "first_ptr", block);
    const auto secondPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), ++idx)}, "second_ptr", block);

    const auto value = DoGenerateGetValue(ctx, firstPtr, secondPtr, block);
    if (value->getType()->isPointerTy()) {
        const auto load = new LoadInst(valueType, value, "load", block);
        ValueRelease(node->GetRepresentation(), load, ctx, block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE Value* TBinaryCodegeneratorNodeBase::CreateGetValueImpl(const IComputationNode* node, 
    const TCodegenContext& ctx, BasicBlock*& block) const {
    const auto value = DoGenerateGetValue(ctx, block);
    if (value->getType()->isPointerTy()) {
        ValueRelease(node->GetRepresentation(), value, ctx, block);
        const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), value, "load", block);
        return load;
    } else {
        return value;
    }
}

Y_NO_INLINE Value* TMutableCodegeneratorNodeBase::CreateGetValueImpl(
    bool stateless, EValueRepresentation representation, ui32 valueIndex,
    const TString& name, const TCodegenContext& ctx, BasicBlock*& block) const {
    if (stateless) {
        const auto newValue = DoGenerateGetValue(ctx, block);
        if (newValue->getType()->isPointerTy()) {
            ValueRelease(representation, newValue, ctx, block);
            const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), newValue, "load", block);
            return load;
        } else {
            return newValue;
        }
    }

    return ctx.AlwaysInline ? MakeGetValueBody(representation, valueIndex, ctx, block) :
        CallInst::Create(GenerateInternalGetValue(name, representation, valueIndex, ctx.Codegen), {ctx.Ctx}, "getter", block);
}

Function* TMutableCodegeneratorNodeBase::GenerateInternalGetValue(const TString& name,
    EValueRepresentation representation, ui32 valueIndex, NYql::NCodegen::ICodegen& codegen) const {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto funcType = FunctionType::get(Type::getInt128Ty(context), {PointerType::getUnqual(GetCompContextType(context))}, false);

    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    ctx.Ctx = &*ctx.Func->arg_begin();
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto get = MakeGetValueBody(representation, valueIndex, ctx, main);

    ReturnInst::Create(context, get, main);
    return ctx.Func;
}

Value* TMutableCodegeneratorNodeBase::MakeGetValueBody(EValueRepresentation representation, ui32 valueIndex, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, valueIndex)}, "value_ptr", block);
    const auto value = new LoadInst(valueType, valuePtr, "value", block);

    const auto invv = ConstantInt::get(value->getType(), 0xFFFFFFFFFFFFFFFFULL);

    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, invv, "check", block);

    const auto comp = BasicBlock::Create(context, "comp", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    BranchInst::Create(comp, done, check, block);

    block = comp;

    const auto newValue = DoGenerateGetValue(ctx, block);

    if (newValue->getType()->isPointerTy()) {
        const auto load = new LoadInst(valueType, newValue, "value", block);
        new StoreInst(load, valuePtr, block);
        new StoreInst(ConstantInt::get(load->getType(), 0), newValue, block);
    } else {
        new StoreInst(newValue, valuePtr, block);
        ValueAddRef(representation, valuePtr, ctx, block);
    }

    BranchInst::Create(done, block);
    block = done;

    const auto result = new LoadInst(valueType, valuePtr, "result", false, block);
    return result;
}

Y_NO_INLINE Value* TMutableCodegeneratorPtrNodeBase::CreateGetValueImpl(
    bool stateless, EValueRepresentation representation, ui32 valueIndex,
    const TString& name, const TCodegenContext& ctx, BasicBlock*& block) const {
    if (stateless) {
        const auto type = Type::getInt128Ty(ctx.Codegen.GetContext());
        const auto pointer = ctx.Func->getEntryBlock().empty() ?
            new AllocaInst(type, 0U, "output", &ctx.Func->getEntryBlock()):
            new AllocaInst(type, 0U, "output", &ctx.Func->getEntryBlock().back());

        DoGenerateGetValue(ctx, pointer, block);
        ValueRelease(representation, pointer, ctx, block);
        const auto load = new LoadInst(type, pointer, "load", block);
        return load;
    }

    return ctx.AlwaysInline ? MakeGetValueBody(valueIndex, ctx, block) :
        CallInst::Create(GenerateInternalGetValue(name, valueIndex, ctx.Codegen), {ctx.Ctx}, "getter", block);
}

Value* TMutableCodegeneratorPtrNodeBase::MakeGetValueBody(ui32 valueIndex, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto indexType = Type::getInt32Ty(context);
    const auto valueType = Type::getInt128Ty(context);
    const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, valueIndex)}, "value_ptr", block);
    const auto value = new LoadInst(valueType, valuePtr, "value", block);

    const auto invv = ConstantInt::get(value->getType(), 0xFFFFFFFFFFFFFFFFULL);

    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, invv, "check", block);

    const auto comp = BasicBlock::Create(context, "comp", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);

    BranchInst::Create(comp, done, check, block);

    block = comp;

    DoGenerateGetValue(ctx, valuePtr, block);

    BranchInst::Create(done, block);
    block = done;

    const auto result = new LoadInst(valueType, valuePtr, "result", false, block);
    return result;
}

Function* TMutableCodegeneratorPtrNodeBase::GenerateInternalGetValue(const TString& name, ui32 valueIndex, NYql::NCodegen::ICodegen& codegen) const {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();
    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto contextType = GetCompContextType(context);

    const auto funcType = FunctionType::get(Type::getInt128Ty(context), {PointerType::getUnqual(contextType)}, false);

    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    ctx.Ctx = &*ctx.Func->arg_begin();
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto get = MakeGetValueBody(valueIndex, ctx, main);

    ReturnInst::Create(context, get, main);
    return ctx.Func;
}

Y_NO_INLINE Value* TMutableCodegeneratorFallbackNodeBase::DoGenerateGetValueImpl(
    uintptr_t methodPtr, uintptr_t thisPtr, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto type = Type::getInt128Ty(context);
    const auto ptrType = PointerType::getUnqual(StructType::get(context));
    const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), methodPtr);
    const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), thisPtr), ptrType, "self", block);
    if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
        const auto funType = FunctionType::get(type, {self->getType(), ctx.Ctx->getType()}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
        const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx}, "value", block);
        return value;
    } else {
        const auto resultPtr = new AllocaInst(type, 0U, "return", block);
        const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType()}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
        CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx}, "", block);
        const auto value = new LoadInst(type, resultPtr, "value", block);
        return value;
    }
}

Y_NO_INLINE Function* TCodegeneratorRootNodeBase::GenerateGetValueImpl(
    const TString& name, const ICodegeneratorInlineNode* gen, NYql::NCodegen::ICodegen& codegen) {
    auto& module = codegen.GetModule();
    auto& context = codegen.GetContext();

    if (const auto f = module.getFunction(name.c_str()))
        return f;

    const auto valueType = Type::getInt128Ty(context);
    const auto contextType = GetCompContextType(context);

    const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(valueType, {PointerType::getUnqual(contextType)}, false):
        FunctionType::get(Type::getVoidTy(context) , {PointerType::getUnqual(valueType), PointerType::getUnqual(contextType)}, false);

    TCodegenContext ctx(codegen);
    ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

    DISubprogramAnnotator annotator(ctx, ctx.Func);
    

    auto args = ctx.Func->arg_begin();
    if (codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
        auto& firstArg = *args++;
        firstArg.addAttr(Attribute::StructRet);
        firstArg.addAttr(Attribute::NoAlias);
    }

    auto main = BasicBlock::Create(context, "main", ctx.Func);
    ctx.Ctx = &*args;
    ctx.Ctx->addAttr(Attribute::NonNull);

    const auto get = gen->CreateGetValue(ctx, main);

    if (codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        ReturnInst::Create(context, get, main);
    } else {
        new StoreInst(get, &*--args, main);
        ReturnInst::Create(context, main);
    }

    return ctx.Func;
}

#if __clang__ && (__clang_major__ < 16)
TSrcLocation TSrcLocation::current() {
    return {};
}

const char* TSrcLocation::file_name() const {
    return __FILE__;
}

size_t TSrcLocation::line() const {
    return __LINE__;
}

size_t TSrcLocation::column() const {
    return 0;
}
#endif

DISubprogramAnnotator::DISubprogramAnnotator(TCodegenContext& ctx, Function* subprogramFunc, const TSrcLocation& location)
    : Ctx(ctx)
    , DebugBuilder(std::make_unique<DIBuilder>(ctx.Codegen.GetModule()))
    , Subprogram(MakeDISubprogram(subprogramFunc->getName(), location))
    , Func(subprogramFunc)
{
    subprogramFunc->setSubprogram(Subprogram);
    Ctx.Annotator = this;
}

DISubprogramAnnotator::~DISubprogramAnnotator() {
    Ctx.Annotator = nullptr;
    { // necessary stub annotation of "CallInst"s
        DIScopeAnnotator stubAnnotate(*this);
        for (BasicBlock& block : *Func) {
            for (Instruction& inst : block) {
                if (CallInst* callInst = dyn_cast_or_null<CallInst>(&inst)) {
                    const auto& debugLoc = callInst->getDebugLoc();
                    if (!debugLoc) {
                        stubAnnotate(callInst);
                    }
                }
            }
        }
    }
    DebugBuilder->finalizeSubprogram(Subprogram);
}

DIFile* DISubprogramAnnotator::MakeDIFile(const TSrcLocation& location) {
    TFsPath path = TString(location.file_name());
    return DebugBuilder->createFile(path.GetName().c_str(), path.Parent().GetPath().c_str());
}

DISubprogram* DISubprogramAnnotator::MakeDISubprogram(const StringRef& name, const TSrcLocation& location) {
    const auto file = MakeDIFile(location);
    const auto unit = DebugBuilder->createCompileUnit(llvm::dwarf::DW_LANG_C_plus_plus, file, "MKQL", false, "", 0);
    const auto subroutineType = DebugBuilder->createSubroutineType(DebugBuilder->getOrCreateTypeArray({}));
    return DebugBuilder->createFunction(
        unit,
        name,
        llvm::StringRef(),
        file, 0,
        subroutineType, 0, llvm::DINode::FlagPrototyped, llvm::DISubprogram::SPFlagDefinition
    );
}

DIScopeAnnotator::DIScopeAnnotator(DISubprogramAnnotator& subprogramAnnotator, const TSrcLocation& location)
    : SubprogramAnnotator(subprogramAnnotator)
    , Scope(SubprogramAnnotator.DebugBuilder->createLexicalBlock(SubprogramAnnotator.Subprogram, SubprogramAnnotator.MakeDIFile(location), location.line(), location.column()))
{}

Instruction* DIScopeAnnotator::operator()(Instruction* inst, const TSrcLocation& location) const {
    inst->setDebugLoc(DILocation::get(SubprogramAnnotator.Ctx.Codegen.GetContext(), location.line(), location.column(), Scope));
    return inst;
}

}
}
#endif
