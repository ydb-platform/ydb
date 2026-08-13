#include "WAVM/IR/Validate.h"
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/OperatorPrinter.h"
#include "WAVM/IR/OperatorSignatures.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashSet.h"
#include "WAVM/Logging/Logging.h"

using namespace WAVM;
using namespace WAVM::IR;

#define VALIDATE_UNLESS(reason, comparison)                                                        \
	if(comparison) { throw ValidationException(reason #comparison); }

#define VALIDATE_INDEX(index, arraySize)                                                           \
	if(index >= arraySize)                                                                         \
	{                                                                                              \
		throw ValidationException(                                                                 \
			std::string("invalid index: " #index " must be less than " #arraySize " (" #index "=") \
			+ std::to_string(index) + (", " #arraySize "=") + std::to_string(arraySize) + ')');    \
	}

#define VALIDATE_FEATURE(context, feature)                                                         \
	if(!module.featureSpec.feature)                                                                \
	{                                                                                              \
		throw ValidationException(std::string(context) + " requires "                              \
								  + getFeatureName(Feature::feature) + " feature");                \
	}

namespace WAVM {
	template<> struct Hash<KindAndIndex>
	{
		Uptr operator()(const KindAndIndex& kindAndIndex, Uptr seed = 0) const
		{
			Uptr hash = seed;
			hash = Hash<Uptr>()(Uptr(kindAndIndex.kind), hash);
			hash = Hash<Uptr>()(kindAndIndex.index, hash);
			return hash;
		}
	};
}

namespace WAVM { namespace IR {
	struct ModuleValidationState
	{
		const IR::Module& module;
		HashSet<KindAndIndex> declaredExternRefs;

		ModuleValidationState(const IR::Module& inModule) : module(inModule) {}
	};
}}

std::shared_ptr<ModuleValidationState> IR::createModuleValidationState(const IR::Module& module)
{
	return std::make_shared<ModuleValidationState>(module);
}

static void validate(const IR::Module& module, IR::ValueType valueType)
{
	switch(valueType)
	{
	case ValueType::i32:
	case ValueType::i64:
	case ValueType::f32:
	case ValueType::f64: WAVM_ASSERT(module.featureSpec.mvp); break;
	case ValueType::v128: VALIDATE_FEATURE("v128 value type", simd) break;
	case ValueType::externref:
	case ValueType::funcref: VALIDATE_FEATURE(asString(valueType), referenceTypes) break;

	case ValueType::none:
	case ValueType::any:
	default:
		throw ValidationException("invalid value type (" + std::to_string((Uptr)valueType) + ")");
	};
}

static void validate(SizeConstraints size, U64 maxMax)
{
	U64 max = size.max == UINT64_MAX ? maxMax : size.max;
	VALIDATE_UNLESS("disjoint size bounds: ", size.min > max);
	VALIDATE_UNLESS("maximum size exceeds limit: ", max > maxMax);
}

static void validate(const IR::Module& module, ReferenceType type)
{
	switch(type)
	{
	case ReferenceType::funcref: break;
	case ReferenceType::externref: VALIDATE_FEATURE(asString(type), referenceTypes); break;

	case ReferenceType::none:
	default:
		throw ValidationException("invalid reference type (" + std::to_string((Uptr)type) + ")");
	}
}

static void validate(const Module& module, TableType type)
{
	validate(module, type.elementType);
	validate(type.size,
			 type.indexType == IndexType::i32 ? IR::maxTable32Elems : IR::maxTable64Elems);
	if(type.isShared)
	{
		VALIDATE_FEATURE("shared table", sharedTables);
		VALIDATE_UNLESS("shared tables must have a maximum size: ", type.size.max == UINT64_MAX);
	}
	if(type.indexType != IndexType::i32) { VALIDATE_FEATURE("64-bit table indices", table64); }
}

static void validate(const Module& module, MemoryType type)
{
	validate(type.size,
			 type.indexType == IndexType::i32 ? IR::maxMemory32Pages : IR::maxMemory64Pages);
	if(type.isShared)
	{
		VALIDATE_FEATURE("shared memory", atomics);
		VALIDATE_UNLESS("shared memories must have a maximum size: ", type.size.max == UINT64_MAX);
	}
	if(type.indexType != IndexType::i32) { VALIDATE_FEATURE("64-bit memory addresses", memory64); }
}

static void validate(const Module& module, GlobalType type) { validate(module, type.valueType); }

static void validate(const Module& module, TypeTuple typeTuple)
{
	for(ValueType valueType : typeTuple) { validate(module, valueType); }
}

template<typename Type> void validateType(Type expectedType, Type actualType, const char* context)
{
	if(!isSubtype(actualType, expectedType))
	{
		throw ValidationException(std::string("type mismatch: expected ") + asString(expectedType)
								  + " but got " + asString(actualType) + " in " + context);
	}
}

static void validateExternKind(const Module& module, ExternKind externKind)
{
	switch(externKind)
	{
	case ExternKind::function:
	case ExternKind::table:
	case ExternKind::memory:
	case ExternKind::global: break;

	case ExternKind::exceptionType:
		VALIDATE_FEATURE("exception type extern", exceptionHandling);
		break;

	case ExternKind::invalid:
	default:
		throw ValidationException("invalid extern kind (" + std::to_string(Uptr(externKind)) + ")");
	};
}

static ValueType validateGlobalIndex(const Module& module,
									 Uptr globalIndex,
									 bool mustBeMutable,
									 bool mustBeImmutable,
									 bool mustBeImport,
									 const char* context)
{
	VALIDATE_INDEX(globalIndex, module.globals.size());
	const GlobalType& globalType = module.globals.getType(globalIndex);
	if(mustBeMutable && !globalType.isMutable)
	{ throw ValidationException("attempting to mutate immutable global"); }
	else if(mustBeImport && globalIndex >= module.globals.imports.size())
	{
		throw ValidationException(
			"global variable initializer expression may only access imported globals");
	}
	else if(mustBeImmutable && globalType.isMutable)
	{
		throw ValidationException(
			"global variable initializer expression may only access immutable globals");
	}
	return globalType.valueType;
}

static FunctionType validateFunctionIndex(const Module& module, Uptr functionIndex)
{
	VALIDATE_INDEX(functionIndex, module.functions.size());
	return module.types[module.functions.getType(functionIndex).index];
}

static void validateFunctionRef(const ModuleValidationState& state, Uptr functionIndex)
{
	validateFunctionIndex(state.module, functionIndex);
}

static void validateFunctionRefIsDeclared(const ModuleValidationState& state, Uptr functionIndex)
{
	if(!state.declaredExternRefs.contains(KindAndIndex{ExternKind::function, functionIndex}))
	{
		throw ValidationException(
			"function " + std::to_string(functionIndex)
			+ " must be an import, exported, or present in an elem segment or global initializer"
			+ " to be used as the operand to ref.func");
	}
}

static FunctionType validateBlockType(const Module& module, const IndexedBlockType& type)
{
	switch(type.format)
	{
	case IndexedBlockType::noParametersOrResult: return FunctionType();
	case IndexedBlockType::oneResult:
		validate(module, type.resultType);
		return FunctionType(TypeTuple(type.resultType));
	case IndexedBlockType::functionType: {
		VALIDATE_INDEX(type.index, module.types.size());
		FunctionType functionType = module.types[type.index];
		if(functionType.params().size() > 0)
		{ VALIDATE_FEATURE("block with params", multipleResultsAndBlockParams); }
		else if(functionType.results().size() > 1)
		{
			VALIDATE_FEATURE("block with multiple results", multipleResultsAndBlockParams);
		}
		else if(functionType.callingConvention() != CallingConvention::wasm)
		{
			throw ValidationException("invalid calling convention for block");
		}
		return functionType;
	}
	default: WAVM_UNREACHABLE();
	}
}

static FunctionType validateFunctionType(const Module& module, const IndexedFunctionType& type)
{
	VALIDATE_INDEX(type.index, module.types.size());
	const FunctionType functionType = module.types[type.index];
	if(functionType.results().size() > IR::maxReturnValues)
	{ throw ValidationException("function has more return values than WAVM can support"); }
	return functionType;
}

static void validateInitializer(const ModuleValidationState& state,
								const InitializerExpression& expression,
								ValueType expectedType,
								const char* context)
{
	const Module& module = state.module;

	switch(expression.type)
	{
	case InitializerExpression::Type::i32_const:
		validateType(expectedType, ValueType::i32, context);
		break;
	case InitializerExpression::Type::i64_const:
		validateType(expectedType, ValueType::i64, context);
		break;
	case InitializerExpression::Type::f32_const:
		validateType(expectedType, ValueType::f32, context);
		break;
	case InitializerExpression::Type::f64_const:
		validateType(expectedType, ValueType::f64, context);
		break;
	case InitializerExpression::Type::v128_const:
		validateType(expectedType, ValueType::v128, context);
		break;
	case InitializerExpression::Type::global_get: {
		const ValueType globalValueType = validateGlobalIndex(
			module, expression.ref, false, true, true, "initializer expression global index");
		validateType(expectedType, globalValueType, context);
		break;
	}
	case InitializerExpression::Type::ref_null:
		validateType(expectedType, asValueType(expression.nullReferenceType), context);
		break;
	case InitializerExpression::Type::ref_func: {
		validateFunctionRef(state, expression.ref);
		validateType(expectedType, ValueType::funcref, context);
		break;
	}

	case InitializerExpression::Type::invalid:
	default: throw ValidationException("invalid initializer expression");
	};
}

struct FunctionValidationContext
{
	const bool enableTracing{Log::isCategoryEnabled(Log::traceValidation)};

	FunctionValidationContext(ModuleValidationState& inModuleValidationState,
							  const FunctionDef& inFunctionDef)
	: module(inModuleValidationState.module)
	, functionDef(inFunctionDef)
	, functionType(inModuleValidationState.module.types[inFunctionDef.type.index])
	, moduleValidationState(inModuleValidationState)
	{
		// Validate the function's local types.
		for(auto localType : functionDef.nonParameterLocalTypes) { validate(module, localType); }

		// Initialize the local types.
		locals.reserve(functionType.params().size() + functionDef.nonParameterLocalTypes.size());
		locals.insert(locals.end(), functionType.params().begin(), functionType.params().end());
		locals.insert(locals.end(),
					  functionDef.nonParameterLocalTypes.begin(),
					  functionDef.nonParameterLocalTypes.end());

		// Log the start of the function and its signature+locals.
		if(enableTracing)
		{
			traceOperator("func");
			for(auto param : functionType.params())
			{ traceOperator(std::string("param ") + asString(param)); }
			for(auto result : functionType.results())
			{ traceOperator(std::string("result ") + asString(result)); }
			for(auto local : functionDef.nonParameterLocalTypes)
			{ traceOperator(std::string("local ") + asString(local)); }
		}

		// Push the function context onto the control stack.
		pushControlStack(
			ControlContext::Type::function, functionType.results(), functionType.results());
	}

	Uptr getControlStackSize() { return controlStack.size(); }

	void validateNonEmptyControlStack(const char* context)
	{
		if(controlStack.size() == 0)
		{
			throw ValidationException(std::string("Expected non-empty control stack in ")
									  + context);
		}
	}

	void traceOperator(const std::string& operatorDescription)
	{
		std::string controlStackString;
		for(Uptr stackIndex = 0; stackIndex < controlStack.size(); ++stackIndex)
		{
			if(!controlStack[stackIndex].isReachable) { controlStackString += "("; }
			switch(controlStack[stackIndex].type)
			{
			case ControlContext::Type::function: controlStackString += "F"; break;
			case ControlContext::Type::block: controlStackString += "B"; break;
			case ControlContext::Type::ifThen: controlStackString += "T"; break;
			case ControlContext::Type::ifElse: controlStackString += "E"; break;
			case ControlContext::Type::loop: controlStackString += "L"; break;
			case ControlContext::Type::try_: controlStackString += "R"; break;
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
			stackString += asString(stack[stackIndex]);
			stackString += " ";
		}
		if(stack.size() == stackBase) { stackString += "|"; }

		Log::printf(Log::traceValidation,
					"%-50s %-50s %-50s\n",
					controlStackString.c_str(),
					operatorDescription.c_str(),
					stackString.c_str());
	}

	// Operation dispatch methods.
	void block(ControlStructureImm imm)
	{
		const FunctionType type = validateBlockType(module, imm.type);
		popAndValidateTypeTuple("block arguments", type.params());
		pushControlStack(ControlContext::Type::block, type.results(), type.results());

		pushOperandTuple(type.params());
	}
	void loop(ControlStructureImm imm)
	{
		const FunctionType type = validateBlockType(module, imm.type);
		popAndValidateTypeTuple("loop arguments", type.params());
		pushControlStack(ControlContext::Type::loop, type.params(), type.results());
		pushOperandTuple(type.params());
	}
	void if_(ControlStructureImm imm)
	{
		const FunctionType type = validateBlockType(module, imm.type);
		popAndValidateOperand("if condition", ValueType::i32);
		popAndValidateTypeTuple("if arguments", type.params());
		pushControlStack(
			ControlContext::Type::ifThen, type.results(), type.results(), type.params());
		pushOperandTuple(type.params());
	}
	void else_(NoImm imm)
	{
		WAVM_ASSERT(controlStack.size());

		if(controlStack.back().type != ControlContext::Type::ifThen)
		{ throw ValidationException("else only allowed in if context"); }

		popAndValidateTypeTuple("if result", controlStack.back().results);
		validateStackEmptyAtEndOfControlStructure();

		controlStack.back().type = ControlContext::Type::ifElse;
		controlStack.back().isReachable = true;

		pushOperandTuple(controlStack.back().elseParams);
	}
	void end(NoImm)
	{
		WAVM_ASSERT(controlStack.size());

		if(controlStack.back().type == ControlContext::Type::try_)
		{ throw ValidationException("end may not occur in try context"); }

		TypeTuple results = controlStack.back().results;
		if(controlStack.back().type == ControlContext::Type::ifThen
		   && results != controlStack.back().elseParams)
		{ throw ValidationException("else-less if must have identity signature"); }

		popAndValidateTypeTuple("end result", controlStack.back().results);
		validateStackEmptyAtEndOfControlStructure();

		controlStack.pop_back();
		if(controlStack.size()) { pushOperandTuple(results); }
	}
	void try_(ControlStructureImm imm)
	{
		const FunctionType type = validateBlockType(module, imm.type);
		VALIDATE_FEATURE("try", exceptionHandling);
		popAndValidateTypeTuple("try arguments", type.params());
		pushControlStack(ControlContext::Type::try_, type.results(), type.results());
		pushOperandTuple(type.params());
	}
	void validateCatch()
	{
		WAVM_ASSERT(controlStack.size());

		popAndValidateTypeTuple("try result", controlStack.back().results);
		validateStackEmptyAtEndOfControlStructure();

		if(controlStack.back().type == ControlContext::Type::try_
		   || controlStack.back().type == ControlContext::Type::catch_)
		{
			controlStack.back().type = ControlContext::Type::catch_;
			controlStack.back().isReachable = true;
		}
		else
		{
			throw ValidationException("catch only allowed in try/catch context");
		}
	}
	void catch_(ExceptionTypeImm imm)
	{
		VALIDATE_FEATURE("catch", exceptionHandling);
		VALIDATE_INDEX(imm.exceptionTypeIndex, module.exceptionTypes.size());
		const ExceptionType& type = module.exceptionTypes.getType(imm.exceptionTypeIndex);
		validateCatch();
		for(auto param : type.params) { pushOperand(param); }
	}
	void catch_all(NoImm)
	{
		VALIDATE_FEATURE("catch_all", exceptionHandling);
		validateCatch();
	}
	void delegate(DelegateImm imm)
	{
		VALIDATE_FEATURE("delegate", exceptionHandling);
		WAVM_ASSERT(controlStack.size());

		if(controlStack.back().type != ControlContext::Type::try_)
		{ throw ValidationException("delegate may occur only in try context"); }

		TypeTuple results = controlStack.back().results;

		popAndValidateTypeTuple("end result", controlStack.back().results);
		validateStackEmptyAtEndOfControlStructure();

		controlStack.pop_back();
		if(controlStack.size()) { pushOperandTuple(results); }
	}

	void return_(NoImm)
	{
		popAndValidateTypeTuple("ret", functionType.results());
		enterUnreachable();
	}

	void br(BranchImm imm)
	{
		popAndValidateTypeTuple("br argument", getBranchTargetByDepth(imm.targetDepth).params);
		enterUnreachable();
	}
	void br_table(BranchTableImm imm)
	{
		popAndValidateOperand("br_table index", ValueType::i32);

		const TypeTuple defaultTargetParams = getBranchTargetByDepth(imm.defaultTargetDepth).params;

		// Validate that each target has the same number of parameters as the default target, and
		// that the parameters for each target match the arguments provided.
		WAVM_ASSERT(imm.branchTableIndex < functionDef.branchTables.size());
		const std::vector<Uptr>& targetDepths = functionDef.branchTables[imm.branchTableIndex];
		for(Uptr targetIndex = 0; targetIndex < targetDepths.size(); ++targetIndex)
		{
			const ControlContext& branchTarget = getBranchTargetByDepth(targetDepths[targetIndex]);
			const TypeTuple targetParams = branchTarget.params;
			if(targetParams.size() != defaultTargetParams.size())
			{
				throw ValidationException(
					"br_table targets must all take the same number of parameters");
			}
			else
			{
				peekAndValidateTypeTuple("br_table case argument", targetParams);
			}
		}

		popAndValidateTypeTuple("br_table argument", defaultTargetParams);

		enterUnreachable();
	}
	void br_if(BranchImm imm)
	{
		const TypeTuple targetParams = getBranchTargetByDepth(imm.targetDepth).params;
		popAndValidateOperand("br_if condition", ValueType::i32);
		popAndValidateTypeTuple("br_if argument", targetParams);
		pushOperandTuple(targetParams);
	}

	void unreachable(NoImm) { enterUnreachable(); }
	void drop(NoImm) { popAndValidateOperand("drop", ValueType::any); }

	void select(SelectImm imm)
	{
		popAndValidateOperand("select condition", ValueType::i32);

		if(imm.type == ValueType::any)
		{
			const ValueType falseType = popAndValidateOperand("select false value", ValueType::any);
			const ValueType trueType = popAndValidateOperand("select true value", ValueType::any);
			VALIDATE_UNLESS("non-typed select operands must be numeric types: ",
							(falseType != ValueType::none && !isNumericType(falseType))
								|| (trueType != ValueType::none && !isNumericType(trueType)))
			if(falseType == ValueType::none) { pushOperand(trueType); }
			else if(trueType == ValueType::none)
			{
				pushOperand(falseType);
			}
			else
			{
				VALIDATE_UNLESS("non-typed select operands must have the same numeric type: ",
								falseType != trueType);
				pushOperand(falseType);
			}
		}
		else
		{
			VALIDATE_FEATURE("typed select instruction (0x1c)", referenceTypes);

			validate(module, imm.type);
			popAndValidateOperand("select false value", imm.type);
			popAndValidateOperand("select true value", imm.type);
			pushOperand(imm.type);
		}
	}

	void local_get(GetOrSetVariableImm<false> imm)
	{
		pushOperand(validateLocalIndex(imm.variableIndex));
	}
	void local_set(GetOrSetVariableImm<false> imm)
	{
		popAndValidateOperand("local.set", validateLocalIndex(imm.variableIndex));
	}
	void local_tee(GetOrSetVariableImm<false> imm)
	{
		const ValueType localType = validateLocalIndex(imm.variableIndex);
		const ValueType operandType = popAndValidateOperand("local.tee", localType);
		pushOperand(operandType);
	}

	void global_get(GetOrSetVariableImm<true> imm)
	{
		pushOperand(
			validateGlobalIndex(module, imm.variableIndex, false, false, false, "global.get"));
	}
	void global_set(GetOrSetVariableImm<true> imm)
	{
		popAndValidateOperand(
			"global.set",
			validateGlobalIndex(module, imm.variableIndex, true, false, false, "global.set"));
	}

	void table_get(TableImm imm)
	{
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());
		const TableType& tableType = module.tables.getType(imm.tableIndex);
		popAndValidateOperand("table.get", asValueType(tableType.indexType));
		pushOperand(asValueType(tableType.elementType));
	}
	void table_set(TableImm imm)
	{
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());
		const TableType& tableType = module.tables.getType(imm.tableIndex);
		popAndValidateOperands(
			"table.get", asValueType(tableType.indexType), asValueType(tableType.elementType));
	}
	void table_grow(TableImm imm)
	{
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());
		const TableType& tableType = module.tables.getType(imm.tableIndex);
		popAndValidateOperands(
			"table.grow", asValueType(tableType.elementType), asValueType(tableType.indexType));
		pushOperand(ValueType::i32);
	}
	void table_fill(TableImm imm)
	{
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());
		const TableType& tableType = module.tables.getType(imm.tableIndex);
		popAndValidateOperands("table.fill",
							   ValueType::i32,
							   asValueType(tableType.elementType),
							   asValueType(tableType.indexType));
	}

	void throw_(ExceptionTypeImm imm)
	{
		VALIDATE_FEATURE("throw", exceptionHandling);
		VALIDATE_INDEX(imm.exceptionTypeIndex, module.exceptionTypes.size());
		const ExceptionType& exceptionType = module.exceptionTypes.getType(imm.exceptionTypeIndex);
		popAndValidateTypeTuple("exception arguments", exceptionType.params);
		enterUnreachable();
	}

	void rethrow(RethrowImm imm)
	{
		VALIDATE_FEATURE("rethrow", exceptionHandling);
		VALIDATE_UNLESS(
			"rethrow must target a catch: ",
			getBranchTargetByDepth(imm.catchDepth).type != ControlContext::Type::catch_);
		enterUnreachable();
	}

	void ref_null(ReferenceTypeImm imm)
	{
		validate(module, imm.referenceType);
		pushOperand(asValueType(imm.referenceType));
	}

	void ref_is_null(NoImm)
	{
		const ValueType operandType = popAndValidateOperand("ref.is_null operand", ValueType::any);
		if(!isReferenceType(operandType) && operandType != ValueType::none)
		{
			throw ValidationException(std::string("expected reference type but got")
									  + asString(operandType) + " in ref.is_null operand");
		}
		pushOperand(ValueType::i32);
	}

	void call(FunctionImm imm)
	{
		FunctionType calleeType = validateFunctionIndex(module, imm.functionIndex);
		popAndValidateTypeTuple("call arguments", calleeType.params());
		pushOperandTuple(calleeType.results());
	}
	void call_indirect(CallIndirectImm imm)
	{
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());
		const TableType& tableType = module.tables.getType(imm.tableIndex);
		VALIDATE_UNLESS("call_indirect requires a table element type of funcref: ",
						tableType.elementType != ReferenceType::funcref);
		FunctionType calleeType = validateFunctionType(module, imm.type);
		popAndValidateOperand("call_indirect function index", asValueType(tableType.indexType));
		popAndValidateTypeTuple("call_indirect arguments", calleeType.params());
		pushOperandTuple(calleeType.results());
	}

	void validateImm(NoImm) {}

	template<typename nativeType> void validateImm(LiteralImm<nativeType> imm) {}

	template<Uptr naturalAlignmentLog2> void validateImm(LoadOrStoreImm<naturalAlignmentLog2> imm)
	{
		VALIDATE_UNLESS("load or store alignment greater than natural alignment: ",
						imm.alignmentLog2 > naturalAlignmentLog2);
		VALIDATE_INDEX(imm.memoryIndex, module.memories.size());
		const MemoryType& memoryType = module.memories.getType(imm.memoryIndex);
		switch(memoryType.indexType)
		{
		case IndexType::i32:
			VALIDATE_UNLESS("load or store offset too large for i32 address",
							imm.offset > UINT32_MAX);
			break;
		case IndexType::i64:
			VALIDATE_UNLESS("load or store offset too large for i64 address",
							imm.offset > UINT64_MAX);
			break;
		default: WAVM_UNREACHABLE();
		};
	}

	template<Uptr naturalAlignmentLog2, Uptr numLanes>
	void validateImm(LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes> imm)
	{
		validateImm(static_cast<LoadOrStoreImm<naturalAlignmentLog2>&>(imm));
		VALIDATE_UNLESS("invalid lane index: ", imm.laneIndex >= numLanes);
	}

	void validateImm(MemoryImm imm) { VALIDATE_INDEX(imm.memoryIndex, module.memories.size()); }
	void validateImm(MemoryCopyImm imm)
	{
		VALIDATE_INDEX(imm.sourceMemoryIndex, module.memories.size());
		VALIDATE_INDEX(imm.destMemoryIndex, module.memories.size());
	}

	void validateImm(TableImm imm) { VALIDATE_INDEX(imm.tableIndex, module.tables.size()); }
	void validateImm(TableCopyImm imm)
	{
		VALIDATE_INDEX(imm.sourceTableIndex, module.tables.size());
		VALIDATE_INDEX(imm.destTableIndex, module.tables.size());
		VALIDATE_UNLESS(
			"source table element type must be a subtype of the destination table element type",
			!isSubtype(asValueType(module.tables.getType(imm.sourceTableIndex).elementType),
					   asValueType(module.tables.getType(imm.destTableIndex).elementType)));
	}

	void validateImm(FunctionRefImm imm)
	{
		validateFunctionRef(moduleValidationState, imm.functionIndex);
		validateFunctionRefIsDeclared(moduleValidationState, imm.functionIndex);
	}

	template<Uptr numLanes> void validateImm(LaneIndexImm<numLanes> imm)
	{
		VALIDATE_UNLESS("invalid lane index: ", imm.laneIndex >= numLanes);
	}

	template<Uptr numLanes> void validateImm(ShuffleImm<numLanes> imm)
	{
		for(Uptr laneIndex = 0; laneIndex < numLanes; ++laneIndex)
		{
			VALIDATE_UNLESS("shuffle invalid lane index: ",
							imm.laneIndices[laneIndex] >= numLanes * 2);
		}
	}

	template<Uptr naturalAlignmentLog2>
	void validateImm(AtomicLoadOrStoreImm<naturalAlignmentLog2> imm)
	{
		VALIDATE_UNLESS("atomic memory operators must have natural alignment: ",
						imm.alignmentLog2 != naturalAlignmentLog2);

		VALIDATE_INDEX(imm.memoryIndex, module.memories.size());
	}

	void validateImm(AtomicFenceImm imm)
	{
		WAVM_ASSERT(imm.order == MemoryOrder::sequentiallyConsistent);
	}

	void validateImm(DataSegmentAndMemImm imm)
	{
		VALIDATE_INDEX(imm.memoryIndex, module.memories.size());
		VALIDATE_INDEX(imm.dataSegmentIndex, module.dataSegments.size());
	}

	void validateImm(DataSegmentImm imm)
	{
		VALIDATE_INDEX(imm.dataSegmentIndex, module.dataSegments.size());
	}

	void validateImm(ElemSegmentAndTableImm imm)
	{
		VALIDATE_INDEX(imm.elemSegmentIndex, module.elemSegments.size());
		VALIDATE_INDEX(imm.tableIndex, module.tables.size());

		// Validate that the elem type contained by the segment is compatible with the target table.
		const TableType tableType = module.tables.getType(imm.tableIndex);
		ElemSegment::Contents* contents = module.elemSegments[imm.elemSegmentIndex].contents.get();
		switch(contents->encoding)
		{
		case IR::ElemSegment::Encoding::expr:
			VALIDATE_UNLESS("table.init elem segment type is not a subtype of table element type",
							!isSubtype(contents->elemType, tableType.elementType));
			break;
		case IR::ElemSegment::Encoding::index:
			VALIDATE_UNLESS(
				"table.init elem segment type is not a subtype of table element type",
				!isSubtype(asReferenceType(contents->externKind), tableType.elementType));
			break;
		default: WAVM_UNREACHABLE();
		};
	}

	void validateImm(ElemSegmentImm imm)
	{
		VALIDATE_INDEX(imm.elemSegmentIndex, module.elemSegments.size());
	}

#define VALIDATE_OP(_1, name, nameString, Imm, Signature, requiredFeature)                         \
	void name(Imm imm)                                                                             \
	{                                                                                              \
		VALIDATE_FEATURE(nameString, requiredFeature);                                             \
		const char* operatorName = nameString;                                                     \
		WAVM_SUPPRESS_UNUSED(operatorName);                                                        \
		validateImm(imm);                                                                          \
		const OpSignature& signature = IR::OpSignatures::Signature;                                \
		popAndValidateOpParams(nameString, signature.params);                                      \
		pushOpResults(signature.results);                                                          \
	}
	WAVM_ENUM_NONCONTROL_NONPARAMETRIC_OPERATORS(VALIDATE_OP)
#undef VALIDATE_OP

#define VALIDATE_POLYMORPHIC_OP(_1, name, nameString, Imm, Signature, requiredFeature)             \
	void name(Imm imm)                                                                             \
	{                                                                                              \
		VALIDATE_FEATURE(nameString, requiredFeature);                                             \
		const char* operatorName = nameString;                                                     \
		WAVM_SUPPRESS_UNUSED(operatorName);                                                        \
		validateImm(imm);                                                                          \
		const OpSignature& signature = IR::OpSignatures::Signature(module, imm);                   \
		popAndValidateOpParams(nameString, signature.params);                                      \
		pushOpResults(signature.results);                                                          \
	}
	WAVM_ENUM_INDEX_POLYMORPHIC_OPERATORS(VALIDATE_POLYMORPHIC_OP)
#undef DUMMY_VALIDATE_OP

private:
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
		Uptr outerStackSize;

		TypeTuple params;
		TypeTuple results;
		bool isReachable;

		TypeTuple elseParams;
	};

	const Module& module;
	const FunctionDef& functionDef;
	FunctionType functionType;
	ModuleValidationState& moduleValidationState;

	std::vector<ValueType> locals;
	std::vector<ControlContext> controlStack;
	std::vector<ValueType> stack;

	void pushControlStack(ControlContext::Type type,
						  TypeTuple params,
						  TypeTuple results,
						  TypeTuple elseParams = TypeTuple())
	{
		controlStack.push_back({type, stack.size(), params, results, true, elseParams});
	}

	void validateStackEmptyAtEndOfControlStructure()
	{
		WAVM_ASSERT(controlStack.size());

		if(stack.size() != controlStack.back().outerStackSize)
		{
			std::string message = "stack was not empty at end of control structure: ";
			for(Uptr stackIndex = controlStack.back().outerStackSize; stackIndex < stack.size();
				++stackIndex)
			{
				if(stackIndex != controlStack.back().outerStackSize) { message += ", "; }
				message += asString(stack[stackIndex]);
			}
			throw ValidationException(std::move(message));
		}
	}

	void enterUnreachable()
	{
		WAVM_ASSERT(controlStack.size());

		stack.resize(controlStack.back().outerStackSize);
		controlStack.back().isReachable = false;
	}

	void validateBranchDepth(Uptr depth) const
	{
		VALIDATE_INDEX(depth, controlStack.size());
		if(depth >= controlStack.size()) { throw ValidationException("invalid branch depth"); }
	}

	const ControlContext& getBranchTargetByDepth(Uptr depth) const
	{
		validateBranchDepth(depth);
		return controlStack[controlStack.size() - depth - 1];
	}

	ValueType validateLocalIndex(Uptr localIndex)
	{
		VALIDATE_INDEX(localIndex, locals.size());
		return locals[localIndex];
	}

	ValueType peekAndValidateOperand(const char* context,
									 Uptr operandDepth,
									 const ValueType expectedType)
	{
		WAVM_ASSERT(controlStack.size());

		ValueType actualType;
		if(stack.size() > controlStack.back().outerStackSize + operandDepth)
		{ actualType = stack[stack.size() - operandDepth - 1]; }
		else if(!controlStack.back().isReachable)
		{
			// If the current instruction is unreachable, then pop a bottom type that is a subtype
			// of all other types.
			actualType = ValueType::none;
		}
		else
		{
			// If the current instruction is reachable, but the operand stack is empty, then throw a
			// validation exception.
			throw ValidationException(std::string("type mismatch: expected ")
									  + asString(expectedType) + " but stack was empty" + " in "
									  + context + " operand");
		}

		if(!isSubtype(actualType, expectedType))
		{
			throw ValidationException(std::string("type mismatch: expected ")
									  + asString(expectedType) + " but got " + asString(actualType)
									  + " in " + context + " operand");
		}

		return actualType;
	}

	void popAndValidateOperandArray(const char* context, const ValueType* expectedTypes, Uptr num)
	{
		for(Uptr operandIndexFromEnd = 0; operandIndexFromEnd < num; ++operandIndexFromEnd)
		{
			const Uptr operandIndex = num - operandIndexFromEnd - 1;
			popAndValidateOperand(context, expectedTypes[operandIndex]);
		}
	}

	template<Uptr num>
	void popAndValidateOperandArray(const char* context, const ValueType (&expectedTypes)[num])
	{
		popAndValidateOperandArray(context, expectedTypes, num);
	}

	template<typename... OperandTypes>
	void popAndValidateOperands(const char* context, OperandTypes... operands)
	{
		ValueType operandTypes[] = {operands...};
		popAndValidateOperandArray(context, operandTypes);
	}

	ValueType popAndValidateOperand(const char* context, const ValueType expectedType)
	{
		ValueType actualType = peekAndValidateOperand(context, 0, expectedType);

		WAVM_ASSERT(controlStack.size());
		if(stack.size() > controlStack.back().outerStackSize) { stack.pop_back(); }

		return actualType;
	}

	void popAndValidateTypeTuple(const char* context, TypeTuple expectedTypes)
	{
		popAndValidateOperandArray(context, expectedTypes.data(), expectedTypes.size());
	}

	void popAndValidateOpParams(const char* context, OpTypeTuple expectedTypes)
	{
		popAndValidateOperandArray(context, expectedTypes.data(), expectedTypes.size());
	}

	void peekAndValidateTypeTuple(const char* context, TypeTuple expectedTypes)
	{
		for(Uptr operandIndex = 0; operandIndex < expectedTypes.size(); ++operandIndex)
		{
			peekAndValidateOperand(
				context, expectedTypes.size() - operandIndex - 1, expectedTypes[operandIndex]);
		}
	}

	void pushOperand(ValueType type) { stack.push_back(type); }
	void pushOperandTuple(TypeTuple typeTuple)
	{
		for(ValueType type : typeTuple) { pushOperand(type); }
	}
	void pushOpResults(OpTypeTuple results)
	{
		for(ValueType type : results) { pushOperand(type); }
	}
};

void IR::validateTypes(ModuleValidationState& state)
{
	const Module& module = state.module;

	for(Uptr typeIndex = 0; typeIndex < module.types.size(); ++typeIndex)
	{
		FunctionType functionType = module.types[typeIndex];

		// Validate the function type parameters and results here, but don't check the limit on
		// number of return values here, since they don't apply to block types that are also stored
		// here. Instead, uses of a function type from the types array must call
		// validateFunctionType to validate its use as a function type.
		validate(module, functionType.params());
		validate(module, functionType.results());

		if(functionType.results().size() > 1)
		{
			VALIDATE_FEATURE("function type with multiple results", multipleResultsAndBlockParams);
		}

		if(functionType.callingConvention() != CallingConvention::wasm)
		{ VALIDATE_FEATURE("non-WASM function type", nonWASMFunctionTypes); }
	}
}

void IR::validateImports(ModuleValidationState& state)
{
	const Module& module = state.module;

	WAVM_ASSERT(module.imports.size()
				== module.functions.imports.size() + module.tables.imports.size()
					   + module.memories.imports.size() + module.globals.imports.size()
					   + module.exceptionTypes.imports.size());

	for(Uptr functionIndex = 0; functionIndex < module.functions.imports.size(); ++functionIndex)
	{
		const Import<IndexedFunctionType>& functionImport = module.functions.imports[functionIndex];
		validateFunctionType(module, functionImport.type);
		state.declaredExternRefs.add(KindAndIndex{ExternKind::function, functionIndex});
	}
	for(auto& tableImport : module.tables.imports) { validate(module, tableImport.type); }
	for(auto& memoryImport : module.memories.imports) { validate(module, memoryImport.type); }
	for(auto& globalImport : module.globals.imports)
	{
		validate(module, globalImport.type);
		if(globalImport.type.isMutable)
		{ VALIDATE_FEATURE("mutable imported global", importExportMutableGlobals); }
	}
	for(auto& exceptionTypeImport : module.exceptionTypes.imports)
	{ validate(module, exceptionTypeImport.type.params); }

	if(module.tables.size() > 1) { VALIDATE_FEATURE("multiple tables", referenceTypes); }
	if(module.memories.size() > 1) { VALIDATE_FEATURE("multiple memories", multipleMemories); }
}

void IR::validateFunctionDeclarations(ModuleValidationState& state)
{
	const Module& module = state.module;

	for(Uptr functionDefIndex = 0; functionDefIndex < module.functions.defs.size();
		++functionDefIndex)
	{
		const FunctionDef& functionDef = module.functions.defs[functionDefIndex];
		const FunctionType functionType = validateFunctionType(module, functionDef.type);

		if(functionType.callingConvention() != CallingConvention::wasm)
		{ throw ValidationException("Function definitions must have WASM calling convention"); }
	}
}

void IR::validateGlobalDefs(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& globalDef : module.globals.defs)
	{
		validate(module, globalDef.type);
		validateInitializer(state,
							globalDef.initializer,
							globalDef.type.valueType,
							"global initializer expression");

		if(globalDef.initializer.type == InitializerExpression::Type::ref_func)
		{
			state.declaredExternRefs.add(
				KindAndIndex{ExternKind::function, globalDef.initializer.ref});
		}
	}
}

void IR::validateExceptionTypeDefs(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& exceptionTypeDef : module.exceptionTypes.defs)
	{ validate(module, exceptionTypeDef.type.params); }
}

void IR::validateTableDefs(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& tableDef : module.tables.defs) { validate(module, tableDef.type); }
	if(module.tables.size() > 1) { VALIDATE_FEATURE("multiple tables", referenceTypes); }
}

void IR::validateMemoryDefs(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& memoryDef : module.memories.defs) { validate(module, memoryDef.type); }
	if(module.memories.size() > 1) { VALIDATE_FEATURE("multiple memories", multipleMemories); }
}

void IR::validateExports(ModuleValidationState& state)
{
	const Module& module = state.module;
	HashSet<std::string> exportNameSet;
	for(auto& exportIt : module.exports)
	{
		validateExternKind(module, exportIt.kind);
		switch(exportIt.kind)
		{
		case ExternKind::function:
			VALIDATE_INDEX(exportIt.index, module.functions.size());
			state.declaredExternRefs.add(KindAndIndex{ExternKind::function, exportIt.index});
			break;
		case ExternKind::table: VALIDATE_INDEX(exportIt.index, module.tables.size()); break;
		case ExternKind::memory: VALIDATE_INDEX(exportIt.index, module.memories.size()); break;
		case ExternKind::global:
			validateGlobalIndex(module,
								exportIt.index,
								false,
								!module.featureSpec.importExportMutableGlobals,
								false,
								"exported global index");
			break;
		case ExternKind::exceptionType:
			VALIDATE_INDEX(exportIt.index, module.exceptionTypes.size());
			break;

		case ExternKind::invalid:
		default: WAVM_UNREACHABLE();
		};

		VALIDATE_UNLESS("duplicate export: ", exportNameSet.contains(exportIt.name));
		exportNameSet.addOrFail(exportIt.name);
	}
}

void IR::validateStartFunction(ModuleValidationState& state)
{
	const Module& module = state.module;
	if(module.startFunctionIndex != UINTPTR_MAX)
	{
		VALIDATE_INDEX(module.startFunctionIndex, module.functions.size());
		FunctionType startFunctionType
			= module.types[module.functions.getType(module.startFunctionIndex).index];
		VALIDATE_UNLESS("start function must not have any parameters or results: ",
						startFunctionType != FunctionType());
	}
}

void IR::validateElemSegments(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& elemSegment : module.elemSegments)
	{
		if(elemSegment.contents->encoding == ElemSegment::Encoding::index)
		{
			validateExternKind(module, elemSegment.contents->externKind);

			if(elemSegment.contents->externKind != ExternKind::function)
			{
				VALIDATE_FEATURE("elem segment reference non-function externs",
								 allowAnyExternKindElemSegments);
			}
		}

		switch(elemSegment.type)
		{
		case ElemSegment::Type::active: {
			VALIDATE_INDEX(elemSegment.tableIndex, module.tables.size());
			const TableType& tableType = module.tables.getType(elemSegment.tableIndex);

			ReferenceType segmentElemType;
			switch(elemSegment.contents->encoding)
			{
			case ElemSegment::Encoding::expr:
				segmentElemType = elemSegment.contents->elemType;
				break;
			case ElemSegment::Encoding::index:
				segmentElemType = elemSegment.contents->externKind == ExternKind::function
									  ? ReferenceType::funcref
									  : ReferenceType::externref;
				break;
			default: WAVM_UNREACHABLE();
			};

			const ReferenceType tableElemType = tableType.elementType;
			if(!isSubtype(segmentElemType, tableType.elementType))
			{
				throw ValidationException(std::string("segment elem type (")
										  + asString(segmentElemType)
										  + ") is not a subtype of the table's elem type ("
										  + asString(tableElemType) + ")");
			}

			validateInitializer(state,
								elemSegment.baseOffset,
								asValueType(tableType.indexType),
								"elem segment base initializer");
			break;
		}
		case ElemSegment::Type::passive: break;
		case ElemSegment::Type::declared: break;

		default: WAVM_UNREACHABLE();
		};

		switch(elemSegment.contents->encoding)
		{
		case ElemSegment::Encoding::expr:
			for(const ElemExpr& elem : elemSegment.contents->elemExprs)
			{
				ReferenceType exprType = ReferenceType::none;
				switch(elem.type)
				{
				case ElemExpr::Type::ref_null: exprType = elem.nullReferenceType; break;
				case ElemExpr::Type::ref_func:
					exprType = ReferenceType::funcref;
					VALIDATE_INDEX(elem.index, module.functions.size());
					state.declaredExternRefs.add(KindAndIndex{ExternKind::function, elem.index});
					break;

				case ElemExpr::Type::invalid:
				default: WAVM_UNREACHABLE();
				};

				if(!isSubtype(exprType, elemSegment.contents->elemType))
				{
					throw ValidationException(std::string("elem expression type (")
											  + asString(exprType)
											  + ") is not a subtype of the segment's elem type ("
											  + asString(elemSegment.contents->elemType) + ")");
				}
			}
			break;
		case ElemSegment::Encoding::index:
			for(Uptr externIndex : elemSegment.contents->elemIndices)
			{
				switch(elemSegment.contents->externKind)
				{
				case ExternKind::function:
					VALIDATE_INDEX(externIndex, module.functions.size());
					break;
				case ExternKind::table: VALIDATE_INDEX(externIndex, module.tables.size()); break;
				case ExternKind::memory: VALIDATE_INDEX(externIndex, module.memories.size()); break;
				case ExternKind::global: VALIDATE_INDEX(externIndex, module.globals.size()); break;
				case ExternKind::exceptionType:
					VALIDATE_INDEX(externIndex, module.exceptionTypes.size());
					break;
				case ExternKind::invalid:
				default: WAVM_UNREACHABLE();
				};

				state.declaredExternRefs.add(
					KindAndIndex{elemSegment.contents->externKind, externIndex});
			}
			break;
		default: WAVM_UNREACHABLE();
		};
	}
}

void IR::validateDataSegments(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(auto& dataSegment : module.dataSegments)
	{
		if(dataSegment.isActive)
		{
			VALIDATE_INDEX(dataSegment.memoryIndex, module.memories.size());
			const MemoryType& memoryType = module.memories.getType(dataSegment.memoryIndex);
			validateInitializer(state,
								dataSegment.baseOffset,
								asValueType(memoryType.indexType),
								"data segment base initializer");
		}
	}
}

void IR::validateCodeSection(ModuleValidationState& state)
{
	const Module& module = state.module;
	for(const auto& functionDef : module.functions.defs)
	{
		CodeValidationStream validationStream(state, functionDef);
		OperatorDecoderStream operatorDecoderStream(functionDef.code);
		while(operatorDecoderStream) { operatorDecoderStream.decodeOp(validationStream); }
	}
}

namespace WAVM { namespace IR {
	struct CodeValidationStreamImpl
	{
		FunctionValidationContext functionContext;
		OperatorPrinter operatorPrinter;

		CodeValidationStreamImpl(ModuleValidationState& moduleValidationState,
								 const FunctionDef& functionDef)
		: functionContext(moduleValidationState, functionDef)
		, operatorPrinter(moduleValidationState.module, functionDef)
		{
		}
	};
}}

IR::CodeValidationStream::CodeValidationStream(ModuleValidationState& moduleValidationState,
											   const FunctionDef& functionDef)
{
	impl = new CodeValidationStreamImpl(moduleValidationState, functionDef);
}

IR::CodeValidationStream::~CodeValidationStream()
{
	delete impl;
	impl = nullptr;
}

void IR::CodeValidationStream::finish()
{
	if(impl->functionContext.getControlStackSize())
	{ throw ValidationException("end of code reached before end of function"); }
}

#define VISIT_OPCODE(_, name, nameString, Imm, ...)                                                \
	void IR::CodeValidationStream::name(Imm imm)                                                   \
	{                                                                                              \
		if(impl->functionContext.enableTracing)                                                    \
		{ impl->functionContext.traceOperator(impl->operatorPrinter.name(imm)); }                  \
		impl->functionContext.validateNonEmptyControlStack(nameString);                            \
		impl->functionContext.name(imm);                                                           \
	}
WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
