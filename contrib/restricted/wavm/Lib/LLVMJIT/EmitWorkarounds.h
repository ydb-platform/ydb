#pragma once

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/IR/Constant.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

inline llvm::Value* getTriviallyNonConstantZero(llvm::IRBuilder<>& irBuilder, llvm::Type* type)
{
	llvm::Value* zeroAlloca = irBuilder.CreateAlloca(type, nullptr, "nonConstantZero");
	irBuilder.CreateStore(llvm::Constant::getNullValue(type), zeroAlloca);
	return irBuilder.CreateLoad(type, zeroAlloca);
}

inline llvm::Value* createFCmpWithWorkaround(llvm::IRBuilder<>& irBuilder,
											 llvm::CmpInst::Predicate predicate,
											 llvm::Value* left,
											 llvm::Value* right)
{
	// Work around a bug in the LLVM IRBuilder constant folder that will fold FCmpUNO(X, X) where
	// X is a constant expression to false, even though it would be true if X evaluates to a NaN. To
	// work around the bug, add a constant zero to the operand in a way that can easily be elided in
	// optimization, and use it as one of the operands: FCmpUNO(X, X+0).
	if(left == right && LLVM_VERSION_MAJOR < 9)
	{
		right
			= irBuilder.CreateFAdd(right, getTriviallyNonConstantZero(irBuilder, right->getType()));
	}
	return irBuilder.CreateFCmp(predicate, left, right);
}

inline llvm::Value* createICmpWithWorkaround(llvm::IRBuilder<>& irBuilder,
											 llvm::CmpInst::Predicate predicate,
											 llvm::Value* left,
											 llvm::Value* right)
{
	// Work around a bug in the LLVM IRBuilder constant folder:
	//   ICmp(T x, bitcast <floating point type> y to T)
	// will be folded to:
	//   ICmp(bitcast T x to <floating point type>, <floating point type> y)
	// This is malformed, and will cause undefined behavior later in the compilation pipeline if it
	// occurs. To work around the bug, or a trivially non-constant zero with the right operand.
	if(LLVM_VERSION_MAJOR < 9)
	{
		right = irBuilder.CreateOr(right, getTriviallyNonConstantZero(irBuilder, right->getType()));
	}
	return irBuilder.CreateICmp(predicate, left, right);
}
