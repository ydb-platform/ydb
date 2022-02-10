#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>

namespace NKikimr {
namespace NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using namespace llvm;

using TUnaryGenFunc = Value* (*)(Value*, const TCodegenContext&, BasicBlock*&);
using TBinaryGenFunc = Value* (*)(Value*, Value*, const TCodegenContext&, BasicBlock*&);
using TTernaryGenFunc = Value* (*)(Value*, Value*, Value*, const TCodegenContext&, BasicBlock*&);

Value* GenerateUnaryWithoutCheck(Value* arg, const TCodegenContext& ctx, BasicBlock*& block, TUnaryGenFunc generator);
Value* GenerateUnaryWithCheck(Value* arg, const TCodegenContext& ctx, BasicBlock*& block, TUnaryGenFunc generator);

template<bool CheckLeft, bool CheckRight>
Value* GenerateBinary(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);

Value* GenerateAggregate(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator);

Value* GenerateCompareAggregate(Value* lhs, Value* rhs, const TCodegenContext& ctx, BasicBlock*& block, TBinaryGenFunc generator, CmpInst::Predicate simple);

template<bool CheckFirst>
Value* GenerateTernary(Value* first, Value* second, Value* third, const TCodegenContext& ctx, BasicBlock*& block, TTernaryGenFunc generator);

template<typename T>
std::string GetFuncNameForType(const char* name);

#endif

}
}

