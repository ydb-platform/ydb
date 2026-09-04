#pragma once

#include "EmitContext.h"
#include "EmitModuleContext.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Logging/Logging.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/IR/Intrinsics.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

namespace WAVM { namespace LLVMJIT {
	struct EmitFunctionContext : EmitContext
	{
		typedef void Result;

		struct EmitModuleContext& moduleContext;
		const IR::Module& irModule;
		const IR::FunctionDef& functionDef;
		IR::FunctionType functionType;
		llvm::Function* function;

		std::vector<llvm::Value*> localPointers;
		std::vector<llvm::Type*> localPointerElementTypes;

		llvm::DISubprogram* diFunction;

		// Information about an in-scope control structure.
		struct ControlContext
		{
			enum class Type : U8
			{
				function,
				block,
				ifThen,
				ifElse,
				loop,
				try_,
				catch_
			};

			Type type;
			llvm::BasicBlock* endBlock;
			PHIVector endPHIs;
			llvm::BasicBlock* elseBlock;
			ValueVector elseArgs;
			IR::TypeTuple resultTypes;
			Uptr outerStackSize;
			Uptr outerBranchTargetStackSize;
			bool isReachable;
		};

		struct BranchTarget
		{
			IR::TypeTuple params;
			llvm::BasicBlock* block;
			PHIVector phis;
		};

		std::vector<ControlContext> controlStack;
		std::vector<BranchTarget> branchTargetStack;
		std::vector<llvm::Value*> stack;

		EmitFunctionContext(LLVMContext& inLLVMContext,
							EmitModuleContext& inModuleContext,
							const IR::Module& inIRModule,
							const IR::FunctionDef& inFunctionDef,
							llvm::Function* inLLVMFunction)
		: EmitContext(inLLVMContext, inModuleContext.memoryOffsets)
		, moduleContext(inModuleContext)
		, irModule(inIRModule)
		, functionDef(inFunctionDef)
		, functionType(inIRModule.types[inFunctionDef.type.index])
		, function(inLLVMFunction)
		{
		}

		void emit();

		// Operand stack manipulation
		llvm::Value* pop()
		{
			WAVM_ASSERT(
				stack.size() - (controlStack.size() ? controlStack.back().outerStackSize : 0) >= 1);
			llvm::Value* result = stack.back();
			stack.pop_back();
			return result;
		}

		void popMultiple(llvm::Value** outValues, Uptr num)
		{
			WAVM_ASSERT(stack.size()
							- (controlStack.size() ? controlStack.back().outerStackSize : 0)
						>= num);
			std::copy(stack.end() - num, stack.end(), outValues);
			stack.resize(stack.size() - num);
		}

		llvm::Value* getValueFromTop(Uptr offset = 0) const
		{
			return stack[stack.size() - offset - 1];
		}

		void push(llvm::Value* value) { stack.push_back(value); }

		void pushMultiple(llvm::Value** values, Uptr numValues)
		{
			for(Uptr valueIndex = 0; valueIndex < numValues; ++valueIndex)
			{ push(values[valueIndex]); }
		}

		// Creates a PHI node for the argument of branches to a basic block.
		PHIVector createPHIs(llvm::BasicBlock* basicBlock, IR::TypeTuple type);

		// Bitcasts a LLVM value to a canonical type for the corresponding WebAssembly type.
		// This is currently just used to map all the various vector types to a canonical type for
		// the vector width.
		llvm::Value* coerceToCanonicalType(llvm::Value* value);

		// Debug logging.
		void traceOperator(const std::string& operatorDescription);

		// Coerces an I32 value to an I1, and vice-versa.
		llvm::Value* coerceI32ToBool(llvm::Value* i32Value)
		{
			return irBuilder.CreateICmpNE(i32Value,
										  llvmContext.typedZeroConstants[(Uptr)IR::ValueType::i32]);
		}

		llvm::Value* coerceBoolToI32(llvm::Value* boolValue)
		{
			return zext(boolValue, llvmContext.i32Type);
		}

		// Converts a bounded memory address to a LLVM pointer.
		llvm::Value* coerceAddressToPointer(llvm::Value* boundedAddress,
											llvm::Type* memoryType,
											Uptr memoryIndex);

		// Traps a divide-by-zero
		void trapDivideByZero(llvm::Value* divisor);

		// Traps on (x / 0) or (INT_MIN / -1).
		void trapDivideByZeroOrIntegerOverflow(IR::ValueType type,
											   llvm::Value* left,
											   llvm::Value* right);

		llvm::Value* callLLVMIntrinsic(const std::initializer_list<llvm::Type*>& typeArguments,
									   llvm::Intrinsic::ID id,
									   llvm::ArrayRef<llvm::Value*> arguments)
		{
			return irBuilder.CreateCall(moduleContext.getLLVMIntrinsic(typeArguments, id),
										arguments);
		}

		// A helper function to emit a conditional call to a non-returning intrinsic function.
		void emitConditionalTrapIntrinsic(llvm::Value* booleanCondition,
										  const char* intrinsicName,
										  IR::FunctionType intrinsicType,
										  const std::initializer_list<llvm::Value*>& args);

		void pushControlStack(ControlContext::Type type,
							  IR::TypeTuple resultTypes,
							  llvm::BasicBlock* endBlock,
							  const PHIVector& endPHIs,
							  llvm::BasicBlock* elseBlock = nullptr,
							  const ValueVector& elseArgs = {});

		void pushBranchTarget(IR::TypeTuple branchArgumentType,
							  llvm::BasicBlock* branchTargetBlock,
							  const PHIVector& branchTargetPHIs);

		void branchToEndOfControlContext();

		BranchTarget& getBranchTargetByDepth(Uptr depth)
		{
			WAVM_ASSERT(depth < branchTargetStack.size());
			return branchTargetStack[branchTargetStack.size() - depth - 1];
		}

		// This is called after unconditional control flow to indicate that operators following it
		// are unreachable until the control stack is popped.
		void enterUnreachable();

		llvm::Value* identity(llvm::Value* value, llvm::Type* type) { return value; }

		llvm::Value* sext(llvm::Value* value, llvm::Type* type)
		{
			return irBuilder.CreateSExt(value, type);
		}

		llvm::Value* zext(llvm::Value* value, llvm::Type* type)
		{
			return irBuilder.CreateZExt(value, type);
		}

		llvm::Value* trunc(llvm::Value* value, llvm::Type* type)
		{
			return irBuilder.CreateTrunc(value, type);
		}

		llvm::Value* coerceIptrToIndex(IR::IndexType indexType, llvm::Value* iptrValue)
		{
			switch(indexType)
			{
			case IR::IndexType::i32: return irBuilder.CreateTrunc(iptrValue, llvmContext.i32Type);
			case IR::IndexType::i64: return zext(iptrValue, moduleContext.iptrType);
			default: WAVM_UNREACHABLE();
			};
		}

		template<int numElements> llvm::Value* splat(llvm::Value* scalar, llvm::Type*)
		{
			return irBuilder.CreateVectorSplat(numElements, scalar);
		}

		template<int numElements>
		llvm::Value* insertInZeroedVector(llvm::Value* scalar, llvm::VectorType* destType)
		{
			llvm::Constant* zeroVector = llvm::Constant::getNullValue(destType);
			return irBuilder.CreateInsertElement(zeroVector, scalar, U64(0));
		}

		llvm::Value* extendHalfOfIntVector(
			llvm::Value* vector,
			Uptr baseSourceElementIndex,
			llvm::Value* (EmitFunctionContext::*extend)(llvm::Value*, llvm::Type*));

		llvm::Value* insertIntoHalfZeroVector(llvm::Value* halfVector);

		llvm::Value* emitSRem(IR::ValueType type, llvm::Value* left, llvm::Value* right);
		llvm::Value* emitF64Promote(llvm::Value* operand, llvm::Type* destType);

		template<typename Float>
		llvm::Value* emitTruncFloatToInt(IR::ValueType destType,
										 bool isSigned,
										 Float minBounds,
										 Float maxBounds,
										 llvm::Value* operand);

		template<typename Int, typename Float>
		llvm::Value* emitTruncFloatToIntSat(llvm::Type* destType,
											bool isSigned,
											Float minFloatBounds,
											Float maxFloatBounds,
											Int minIntBounds,
											Int maxIntBounds,
											llvm::Value* operand);

		template<typename Int, typename Float, Uptr numElements>
		llvm::Value* emitTruncVectorFloatToIntSat(llvm::Type* destType,
												  bool isSigned,
												  Float minFloatBounds,
												  Float maxFloatBounds,
												  Int minIntBounds,
												  Int maxIntBounds,
												  Int nanResult,
												  llvm::Value* operand);

		llvm::Value* emitBitSelect(llvm::Value* mask,
								   llvm::Value* trueValue,
								   llvm::Value* falseValue);

		llvm::Value* emitVectorSelect(llvm::Value* condition,
									  llvm::Value* trueValue,
									  llvm::Value* falseValue);

		void trapIfMisalignedAtomic(llvm::Value* address, U32 naturalAlignmentLog2);

		struct CatchContext
		{
			// Only used for Windows SEH.
			llvm::CatchSwitchInst* catchSwitchInst;

			// Only used for non-Windows exceptions.
			llvm::LandingPadInst* landingPadInst;

			// Used for all platforms.
			llvm::Value* exceptionPointer;
			llvm::BasicBlock* nextHandlerBlock;
			llvm::Value* exceptionTypeId;
		};

		std::vector<std::optional<CatchContext>> catchStack;

		void endTryWithoutCatch();
		void endTryCatch();
		void exitCatch();

#define VISIT_OPCODE(encoding, name, nameString, Imm, ...) void name(IR::Imm imm);
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
	};
}}
