#include <stdint.h>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/OperatorPrinter.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Logging/Logging.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/ADT/Twine.h>
#include <llvm/IR/Argument.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DIBuilder.h>
#include <llvm/IR/DebugInfoMetadata.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/raw_ostream.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

namespace llvm {
	class Metadata;
}

#define EMIT_ENTER_EXIT_HOOKS 0

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;
using namespace WAVM::Runtime;

// Creates a PHI node for the argument of branches to a basic block.
PHIVector EmitFunctionContext::createPHIs(llvm::BasicBlock* basicBlock, IR::TypeTuple type)
{
	auto originalBlock = irBuilder.GetInsertBlock();
	irBuilder.SetInsertPoint(basicBlock);

	PHIVector result;
	for(Uptr elementIndex = 0; elementIndex < type.size(); ++elementIndex)
	{ result.push_back(irBuilder.CreatePHI(asLLVMType(llvmContext, type[elementIndex]), 2)); }

	if(originalBlock) { irBuilder.SetInsertPoint(originalBlock); }

	return result;
}

// Bitcasts a LLVM value to a canonical type for the corresponding WebAssembly type. This is
// currently just used to map all the various vector types to a canonical type for the vector width.
llvm::Value* EmitFunctionContext::coerceToCanonicalType(llvm::Value* value)
{
	if(value->getType()->isVectorTy())
	{
		switch(value->getType()->getScalarSizeInBits()
			   * static_cast<FixedVectorType*>(value->getType())->getNumElements())
		{
		case 128: return irBuilder.CreateBitCast(value, llvmContext.i64x2Type);
		default: WAVM_UNREACHABLE();
		};
	}
	else
	{
		return value;
	}
}

// Debug logging.
void EmitFunctionContext::traceOperator(const std::string& operatorDescription)
{
	std::string controlStackString;
	for(Uptr stackIndex = 0; stackIndex < controlStack.size(); ++stackIndex)
	{
		if(!controlStack[stackIndex].isReachable) { controlStackString += "("; }
		switch(controlStack[stackIndex].type)
		{
		case ControlContext::Type::function: controlStackString += "F"; break;
		case ControlContext::Type::block: controlStackString += "B"; break;
		case ControlContext::Type::ifThen: controlStackString += "I"; break;
		case ControlContext::Type::ifElse: controlStackString += "E"; break;
		case ControlContext::Type::loop: controlStackString += "L"; break;
		case ControlContext::Type::try_: controlStackString += "T"; break;
		case ControlContext::Type::catch_: controlStackString += "C"; break;
		default: WAVM_UNREACHABLE();
		};
		if(!controlStack[stackIndex].isReachable) { controlStackString += ")"; }
	}

	std::string stackString;
	const Uptr stackBase = controlStack.size() == 0 ? 0 : controlStack.back().outerStackSize;
	for(Uptr stackIndex = 0; stackIndex < stack.size(); ++stackIndex)
	{
		if(stackIndex == stackBase) { stackString += "| "; }
		{
			llvm::raw_string_ostream stackTypeStream(stackString);
			stack[stackIndex]->getType()->print(stackTypeStream, true);
		}
		stackString += " ";
	}
	if(stack.size() == stackBase) { stackString += "|"; }

	Log::printf(Log::traceCompilation,
				"%-50s %-50s %-50s\n",
				controlStackString.c_str(),
				operatorDescription.c_str(),
				stackString.c_str());
}

// Traps a divide-by-zero
void EmitFunctionContext::trapDivideByZero(llvm::Value* divisor)
{
	emitConditionalTrapIntrinsic(
		irBuilder.CreateICmpEQ(divisor, llvm::Constant::getNullValue(divisor->getType())),
		"divideByZeroOrIntegerOverflowTrap",
		FunctionType({}, {}, IR::CallingConvention::intrinsic),
		{});
}

// Traps on (x / 0) or (INT_MIN / -1).
void EmitFunctionContext::trapDivideByZeroOrIntegerOverflow(ValueType type,
															llvm::Value* left,
															llvm::Value* right)
{
	emitConditionalTrapIntrinsic(
		irBuilder.CreateOr(
			irBuilder.CreateAnd(
				irBuilder.CreateICmpEQ(left,
									   type == ValueType::i32
										   ? emitLiteral(llvmContext, (U32)INT32_MIN)
										   : emitLiteral(llvmContext, (U64)INT64_MIN)),
				irBuilder.CreateICmpEQ(right,
									   type == ValueType::i32 ? emitLiteral(llvmContext, (U32)-1)
															  : emitLiteral(llvmContext, (U64)-1))),
			irBuilder.CreateICmpEQ(right, llvmContext.typedZeroConstants[(Uptr)type])),
		"divideByZeroOrIntegerOverflowTrap",
		FunctionType({}, {}, IR::CallingConvention::intrinsic),
		{});
}

// A helper function to emit a conditional call to a non-returning intrinsic function.
void EmitFunctionContext::emitConditionalTrapIntrinsic(
	llvm::Value* booleanCondition,
	const char* intrinsicName,
	FunctionType intrinsicType,
	const std::initializer_list<llvm::Value*>& args)
{
	auto trueBlock
		= llvm::BasicBlock::Create(llvmContext, llvm::Twine(intrinsicName) + "Trap", function);
	auto endBlock
		= llvm::BasicBlock::Create(llvmContext, llvm::Twine(intrinsicName) + "Skip", function);

	irBuilder.CreateCondBr(
		booleanCondition, trueBlock, endBlock, moduleContext.likelyFalseBranchWeights);

	irBuilder.SetInsertPoint(trueBlock);
	emitRuntimeIntrinsic(intrinsicName, intrinsicType, args);
	irBuilder.CreateUnreachable();

	irBuilder.SetInsertPoint(endBlock);
}

//
// Control structure operators
//

void EmitFunctionContext::pushControlStack(ControlContext::Type type,
										   TypeTuple resultTypes,
										   llvm::BasicBlock* endBlock,
										   const PHIVector& endPHIs,
										   llvm::BasicBlock* elseBlock,
										   const ValueVector& elseArgs)
{
	// The unreachable operator filtering should filter out any opcodes that call pushControlStack.
	if(controlStack.size()) { WAVM_ERROR_UNLESS(controlStack.back().isReachable); }

	controlStack.push_back({type,
							endBlock,
							endPHIs,
							elseBlock,
							elseArgs,
							resultTypes,
							stack.size(),
							branchTargetStack.size(),
							true});

	if (type != ControlContext::Type::try_) {
		catchStack.push_back(std::nullopt);
	}
}

void EmitFunctionContext::pushBranchTarget(TypeTuple branchArgumentType,
										   llvm::BasicBlock* branchTargetBlock,
										   const PHIVector& branchTargetPHIs)
{
	branchTargetStack.push_back({branchArgumentType, branchTargetBlock, branchTargetPHIs});
}

void EmitFunctionContext::branchToEndOfControlContext()
{
	ControlContext& currentContext = controlStack.back();

	if(currentContext.isReachable)
	{
		// If the control context expects a result, take it from the operand stack and add it to the
		// control context's end PHI.
		for(Iptr resultIndex = Iptr(currentContext.resultTypes.size()) - 1; resultIndex >= 0;
			--resultIndex)
		{
			llvm::Value* result = pop();
			currentContext.endPHIs[resultIndex]->addIncoming(coerceToCanonicalType(result),
															 irBuilder.GetInsertBlock());
		}

		// Branch to the control context's end.
		irBuilder.CreateBr(currentContext.endBlock);
	}
	WAVM_ASSERT(stack.size() == currentContext.outerStackSize);
}

void EmitFunctionContext::enterUnreachable()
{
	// Unwind the operand stack to the outer control context.
	WAVM_ASSERT(controlStack.back().outerStackSize <= stack.size());
	stack.resize(controlStack.back().outerStackSize);

	// Mark the current control context as unreachable: this will cause the outer loop to stop
	// dispatching operators to us until an else/end for the current control context is reached.
	controlStack.back().isReachable = false;
}

// A do-nothing visitor used to decode past unreachable operators (but supporting logging, and
// passing the end operator through).
struct UnreachableOpVisitor
{
	typedef void Result;

	UnreachableOpVisitor(EmitFunctionContext& inContext)
	: context(inContext), unreachableControlDepth(0)
	{
	}
#define VISIT_OP(opcode, name, nameString, Imm, ...)                                               \
	void name(Imm imm) {}
	WAVM_ENUM_NONCONTROL_OPERATORS(VISIT_OP)
	VISIT_OP(_, unknown, "unknown", Opcode)
#undef VISIT_OP

	// Keep track of control structure nesting level in unreachable code, so we know when we reach
	// the end of the unreachable code.
	void block(ControlStructureImm) { ++unreachableControlDepth; }
	void loop(ControlStructureImm) { ++unreachableControlDepth; }
	void if_(ControlStructureImm) { ++unreachableControlDepth; }

	// If an else or end opcode would signal an end to the unreachable code, then pass it through to
	// the IR emitter.
	void else_(NoImm imm)
	{
		if(!unreachableControlDepth) { context.else_(imm); }
	}
	void end(NoImm imm)
	{
		if(!unreachableControlDepth) { context.end(imm); }
		else
		{
			--unreachableControlDepth;
		}
	}

	void try_(ControlStructureImm imm) { ++unreachableControlDepth; }
	void catch_(ExceptionTypeImm imm)
	{
		if(!unreachableControlDepth) { context.catch_(imm); }
	}
	void catch_all(NoImm imm)
	{
		if(!unreachableControlDepth) { context.catch_all(imm); }
	}
	void delegate(DelegateImm imm)
	{
		if(!unreachableControlDepth) { context.delegate(imm); }
	}

private:
	EmitFunctionContext& context;
	Uptr unreachableControlDepth;
};

void EmitFunctionContext::emit()
{
	WAVM_ASSERT(functionType.callingConvention() == CallingConvention::wasm);

	// Create debug info for the function.
	llvm::SmallVector<llvm::Metadata*, 10> diFunctionParameterTypes;
	for(auto parameterType : functionType.params())
	{ diFunctionParameterTypes.push_back(moduleContext.diValueTypes[(Uptr)parameterType]); }
	auto diParamArray = moduleContext.diBuilder.getOrCreateTypeArray(diFunctionParameterTypes);
	auto diFunctionType = moduleContext.diBuilder.createSubroutineType(diParamArray);
	diFunction = moduleContext.diBuilder.createFunction(
		moduleContext.diModuleScope,
		function->getName(),
		function->getName(),
		moduleContext.diModuleScope,
		0,
		diFunctionType,
#if LLVM_VERSION_MAJOR >= 8
		0,
		llvm::DINode::FlagZero,
		llvm::DISubprogram::SPFlagDefinition | llvm::DISubprogram::SPFlagOptimized);
#else
		false,
		true,
		0);
#endif
	function->setSubprogram(diFunction);

	// Create an initial basic block for the function.
	auto entryBasicBlock = llvm::BasicBlock::Create(llvmContext, "entry", function);

	// Create the return basic block, and push the root control context for the function.
	auto returnBlock = llvm::BasicBlock::Create(llvmContext, "return", function);
	auto returnPHIs = createPHIs(returnBlock, functionType.results());
	irBuilder.SetInsertPoint(entryBasicBlock);

	pushControlStack(
		ControlContext::Type::function, functionType.results(), returnBlock, returnPHIs);
	pushBranchTarget(functionType.results(), returnBlock, returnPHIs);

	// Create and initialize allocas for the memory and table base parameters.
	auto llvmArgIt = function->arg_begin();
	initContextVariables(&*llvmArgIt++, moduleContext.iptrType);

	// Create and initialize allocas for all the locals and parameters.
	for(Uptr localIndex = 0;
		localIndex < functionType.params().size() + functionDef.nonParameterLocalTypes.size();
		++localIndex)
	{
		auto localType
			= localIndex < functionType.params().size()
				  ? functionType.params()[localIndex]
				  : functionDef.nonParameterLocalTypes[localIndex - functionType.params().size()];
		auto localPointer = irBuilder.CreateAlloca(asLLVMType(llvmContext, localType), nullptr, "");
		localPointers.push_back(localPointer);
		localPointerElementTypes.push_back(asLLVMType(llvmContext, localType));

		if(localIndex < functionType.params().size())
		{
			// Copy the parameter value into the local that stores it.
			irBuilder.CreateStore(&*llvmArgIt, localPointer);
			++llvmArgIt;
		}
		else
		{
			// Initialize non-parameter locals to zero.
			irBuilder.CreateStore(llvmContext.typedZeroConstants[(Uptr)localType], localPointer);
		}
	}

	if(EMIT_ENTER_EXIT_HOOKS)
	{
		emitRuntimeIntrinsic(
			"debugEnterFunction",
			FunctionType({}, {ValueType::funcref}, IR::CallingConvention::intrinsic),
			{llvm::ConstantExpr::getSub(
				llvm::ConstantExpr::getPtrToInt(function, moduleContext.iptrType),
				emitLiteralIptr(offsetof(Runtime::Function, code), moduleContext.iptrType))});
	}

	emitRuntimeIntrinsic(
		"checkCallStackDepth", FunctionType({}, {}, IR::CallingConvention::intrinsic), {});

	// Decode the WebAssembly opcodes and emit LLVM IR for them.
	OperatorDecoderStream decoder(functionDef.code);
	UnreachableOpVisitor unreachableOpVisitor(*this);
	OperatorPrinter operatorPrinter(irModule, functionDef);
	Uptr opIndex = 0;
	const bool enableTracing = Log::isCategoryEnabled(Log::traceCompilation);
	while(decoder && controlStack.size())
	{
		if(enableTracing) { traceOperator(decoder.decodeOpWithoutConsume(operatorPrinter)); }

		irBuilder.SetCurrentDebugLocation(
			llvm::DILocation::get(llvmContext, (unsigned int)opIndex++, 0, diFunction));

		if(controlStack.back().isReachable) { decoder.decodeOp(*this); }
		else
		{
			decoder.decodeOp(unreachableOpVisitor);
		}
	};
	WAVM_ASSERT(irBuilder.GetInsertBlock() == returnBlock);

	if(EMIT_ENTER_EXIT_HOOKS)
	{
		emitRuntimeIntrinsic(
			"debugExitFunction",
			FunctionType({}, {ValueType::funcref}, IR::CallingConvention::intrinsic),
			{llvm::ConstantExpr::getSub(
				llvm::ConstantExpr::getPtrToInt(function, moduleContext.iptrType),
				emitLiteralIptr(offsetof(Runtime::Function, code), moduleContext.iptrType))});
	}

	// Emit the function return.
	emitReturn(functionType.results(), stack);
}
