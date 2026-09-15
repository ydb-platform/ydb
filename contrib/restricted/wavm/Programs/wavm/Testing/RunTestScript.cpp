#include <inttypes.h>
#include <stdlib.h>
#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "../wavm.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/FloatComponents.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Memory.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Platform/Thread.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Linker.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"
#include "WAVM/ThreadTest/ThreadTest.h"
#include "WAVM/WASTParse/TestScript.h"
#include "WAVM/WASTParse/WASTParse.h"
#include "wavm-test.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;
using namespace WAVM::WAST;

WAVM_DEFINE_INTRINSIC_MODULE(spectest);

// Try to use the default thread stack size to make sure WAVM passes tests with it, but on
// MacOS the default thread stack size of 512KB is not enough for sanitized builds.
#if defined(__APPLE__) && (WAVM_ENABLE_ASAN || WAVM_ENABLE_UBSAN)
constexpr Uptr threadStackNumBytes = 1 * 1024 * 1024;
#else
constexpr Uptr threadStackNumBytes = 0;
#endif

struct Config
{
	bool strictAssertInvalid{false};
	bool strictAssertMalformed{false};
	bool testCloning{false};
	bool traceTests{false};
	bool traceLLVMIR{false};
	bool traceAssembly{false};
	FeatureSpec featureSpec{FeatureLevel::standard};
};

enum class TestScriptStateKind
{
	root,
	childThread
};

struct TestScriptState
{
	const char* scriptFilename;
	const Config& config;
	TestScriptStateKind kind;

	bool hasInstantiatedModule;
	GCPointer<Instance> lastInstance;
	GCPointer<Compartment> compartment;
	GCPointer<Context> context;

	HashMap<std::string, GCPointer<Instance>> moduleInternalNameToInstanceMap;
	HashMap<std::string, GCPointer<Instance>> moduleNameToInstanceMap;

	std::vector<WAST::Error> errors;

	HashMap<std::string, std::unique_ptr<struct TestScriptThread>> threads;

	TestScriptState(const char* inScriptFilename, const Config& inConfig)
	: scriptFilename(inScriptFilename)
	, config(inConfig)
	, kind(TestScriptStateKind::root)
	, hasInstantiatedModule(false)
	, compartment(Runtime::createCompartment())
	, context(Runtime::createContext(compartment))
	{
		addIntrinsicModules();
	}

	TestScriptState(TestScriptState&& movee) noexcept
	: scriptFilename(movee.scriptFilename)
	, config(movee.config)
	, kind(movee.kind)
	, hasInstantiatedModule(movee.hasInstantiatedModule)
	, lastInstance(movee.lastInstance)
	, compartment(std::move(movee.compartment))
	, context(std::move(movee.context))
	, moduleInternalNameToInstanceMap(std::move(movee.moduleInternalNameToInstanceMap))
	, moduleNameToInstanceMap(std::move(movee.moduleNameToInstanceMap))
	{
	}

	TestScriptState forkThread() const
	{
		TestScriptState result(scriptFilename, config, TestScriptStateKind::childThread);
		result.scriptFilename = scriptFilename;
		result.hasInstantiatedModule = false;
		result.compartment = compartment;
		result.context = context;
		result.addIntrinsicModules();
		return result;
	}

	TestScriptState* clone() const
	{
		WAVM_ASSERT(kind == TestScriptStateKind::root);

		Compartment* clonedCompartment = Runtime::cloneCompartment(compartment);
		if(!clonedCompartment) { return nullptr; }
		Context* clonedContext = Runtime::cloneContext(context, clonedCompartment);
		if(!clonedContext) { WAVM_ERROR_UNLESS(tryCollectCompartment(clonedCompartment)); }

		TestScriptState* result
			= new TestScriptState(scriptFilename, config, TestScriptStateKind::root);
		result->compartment = clonedCompartment;
		result->context = clonedContext;

		result->hasInstantiatedModule = hasInstantiatedModule;
		result->lastInstance = Runtime::remapToClonedCompartment(lastInstance, result->compartment);

		for(const auto& pair : moduleInternalNameToInstanceMap)
		{
			result->moduleInternalNameToInstanceMap.addOrFail(
				pair.key, Runtime::remapToClonedCompartment(pair.value, result->compartment));
		}

		for(const auto& pair : moduleNameToInstanceMap)
		{
			result->moduleNameToInstanceMap.addOrFail(
				pair.key, Runtime::remapToClonedCompartment(pair.value, result->compartment));
		}

		result->errors = errors;

		return result;
	}

	~TestScriptState()
	{
		WAVM_ASSERT(!threads.size());

		if(compartment && kind == TestScriptStateKind::root)
		{
			// Ensure that the compartment is garbage-collected after clearing all the references
			// held by the script state.
			lastInstance = nullptr;
			context = nullptr;
			moduleInternalNameToInstanceMap.clear();
			moduleNameToInstanceMap.clear();
			WAVM_ERROR_UNLESS(tryCollectCompartment(std::move(compartment)));
		}
	}

private:
	TestScriptState(const char* inScriptFilename,
					const Config& inConfig,
					TestScriptStateKind inKind)
	: scriptFilename(inScriptFilename), config(inConfig), kind(inKind)
	{
	}

	void addIntrinsicModules()
	{
		moduleNameToInstanceMap.set(
			"spectest",
			Intrinsics::instantiateModule(
				compartment, {WAVM_INTRINSIC_MODULE_REF(spectest)}, "spectest"));
		moduleNameToInstanceMap.set("threadTest", ThreadTest::instantiate(compartment));
	}
};

struct TestScriptThread
{
	TestScriptState state;
	const ThreadCommand* threadCommand;
	Platform::Thread* platformThread;
};

struct TestScriptResolver : Resolver
{
	TestScriptResolver(const TestScriptState& inState) : state(inState) {}
	bool resolve(const std::string& moduleName,
				 const std::string& exportName,
				 ExternType type,
				 Object*& outObject) override
	{
		auto namedModule = state.moduleNameToInstanceMap.get(moduleName);
		if(namedModule && *namedModule)
		{
			outObject = getInstanceExport(*namedModule, exportName);
			return outObject != nullptr && isA(outObject, type);
		}

		return false;
	}

private:
	const TestScriptState& state;
};

WAVM_VALIDATE_AS_PRINTF(3, 4)
static void testErrorf(TestScriptState& state,
					   const TextFileLocus& locus,
					   const char* messageFormat,
					   ...)
{
	va_list messageArgs;
	va_start(messageArgs, messageFormat);

	// Call vsnprintf to determine how many bytes the formatted string will be.
	// vsnprintf consumes the va_list passed to it, so make a copy of it.
	va_list messageArgsProbe;
	va_copy(messageArgsProbe, messageArgs);
	int numFormattedChars = std::vsnprintf(nullptr, 0, messageFormat, messageArgsProbe);
	va_end(messageArgsProbe);

	// Allocate a buffer for the formatted message.
	WAVM_ERROR_UNLESS(numFormattedChars >= 0);
	std::string formattedMessage;
	formattedMessage.resize(numFormattedChars);

	// Print the formatted message
	int numWrittenChars = std::vsnprintf(
		(char*)formattedMessage.data(), numFormattedChars + 1, messageFormat, messageArgs);
	WAVM_ASSERT(numWrittenChars == numFormattedChars);
	va_end(messageArgs);

	// Add the error to the cursor's error list.
	state.errors.push_back({locus, std::move(formattedMessage)});
}

static Instance* getModuleContextByInternalName(TestScriptState& state,
												const TextFileLocus& locus,
												const char* context,
												const std::string& internalName)
{
	// Look up the module this invoke uses.
	if(!state.hasInstantiatedModule)
	{
		testErrorf(state, locus, "no module to use in %s", context);
		return nullptr;
	}
	Instance* instance = state.lastInstance;
	if(internalName.size())
	{
		auto namedModule = state.moduleInternalNameToInstanceMap.get(internalName);
		if(!namedModule)
		{
			testErrorf(state, locus, "unknown %s module name: %s", context, internalName.c_str());
			return nullptr;
		}
		instance = *namedModule;
	}
	return instance;
}

static Runtime::ExceptionType* getExpectedTrapType(WAST::ExpectedTrapType expectedType)
{
	switch(expectedType)
	{
	case WAST::ExpectedTrapType::outOfBoundsMemoryAccess:
		return Runtime::ExceptionTypes::outOfBoundsMemoryAccess;
	case WAST::ExpectedTrapType::outOfBoundsTableAccess:
		return Runtime::ExceptionTypes::outOfBoundsTableAccess;
	case WAST::ExpectedTrapType::outOfBoundsDataSegmentAccess:
		return Runtime::ExceptionTypes::outOfBoundsDataSegmentAccess;
	case WAST::ExpectedTrapType::outOfBoundsElemSegmentAccess:
		return Runtime::ExceptionTypes::outOfBoundsElemSegmentAccess;
	case WAST::ExpectedTrapType::stackOverflow: return Runtime::ExceptionTypes::stackOverflow;
	case WAST::ExpectedTrapType::integerDivideByZeroOrIntegerOverflow:
		return Runtime::ExceptionTypes::integerDivideByZeroOrOverflow;
	case WAST::ExpectedTrapType::invalidFloatOperation:
		return Runtime::ExceptionTypes::invalidFloatOperation;
	case WAST::ExpectedTrapType::invokeSignatureMismatch:
		return Runtime::ExceptionTypes::invokeSignatureMismatch;
	case WAST::ExpectedTrapType::reachedUnreachable:
		return Runtime::ExceptionTypes::reachedUnreachable;
	case WAST::ExpectedTrapType::indirectCallSignatureMismatch:
		return Runtime::ExceptionTypes::indirectCallSignatureMismatch;
	case WAST::ExpectedTrapType::uninitializedTableElement:
		return Runtime::ExceptionTypes::uninitializedTableElement;
	case WAST::ExpectedTrapType::outOfMemory: return Runtime::ExceptionTypes::outOfMemory;
	case WAST::ExpectedTrapType::misalignedAtomicMemoryAccess:
		return Runtime::ExceptionTypes::misalignedAtomicMemoryAccess;
	case WAST::ExpectedTrapType::invalidArgument: return Runtime::ExceptionTypes::invalidArgument;

	case WAST::ExpectedTrapType::outOfBounds:
	default: WAVM_UNREACHABLE();
	};
}

static std::string describeExpectedTrapType(WAST::ExpectedTrapType expectedType)
{
	if(expectedType == WAST::ExpectedTrapType::outOfBounds) { return "wavm.outOfBounds*"; }
	else
	{
		return describeExceptionType(getExpectedTrapType(expectedType));
	}
}

static bool isExpectedExceptionType(WAST::ExpectedTrapType expectedType,
									Runtime::ExceptionType* actualType)
{
	// WAVM has more precise trap types for out-of-bounds accesses than the spec tests.
	if(expectedType == WAST::ExpectedTrapType::outOfBounds)
	{
		return actualType == Runtime::ExceptionTypes::outOfBoundsMemoryAccess
			   || actualType == Runtime::ExceptionTypes::outOfBoundsDataSegmentAccess
			   || actualType == Runtime::ExceptionTypes::outOfBoundsTableAccess
			   || actualType == Runtime::ExceptionTypes::outOfBoundsElemSegmentAccess;
	}
	else if(expectedType == WAST::ExpectedTrapType::outOfBoundsMemoryAccess)
	{
		return actualType == Runtime::ExceptionTypes::outOfBoundsMemoryAccess
			   || actualType == Runtime::ExceptionTypes::outOfBoundsDataSegmentAccess;
	}
	else if(expectedType == WAST::ExpectedTrapType::outOfBoundsTableAccess)
	{
		return actualType == Runtime::ExceptionTypes::outOfBoundsTableAccess
			   || actualType == Runtime::ExceptionTypes::outOfBoundsElemSegmentAccess;
	}
	else
	{
		return getExpectedTrapType(expectedType) == actualType;
	}
}

static void traceLLVMIR(const char* moduleName, const IR::Module& irModule)
{
	std::string llvmIR = LLVMJIT::emitLLVMIR(irModule, LLVMJIT::getHostTargetSpec(), true);

	Log::printf(Log::output, "%s LLVM IR:\n%s\n", moduleName, llvmIR.c_str());
}

static void traceAssembly(const char* moduleName, ModuleConstRefParam compiledModule)
{
	const std::vector<U8> objectBytes = getObjectCode(compiledModule);
	const std::string disassemblyString
		= LLVMJIT::disassembleObject(LLVMJIT::getHostTargetSpec(), objectBytes);

	Log::printf(
		Log::output, "%s object code disassembly:\n%s\n", moduleName, disassemblyString.c_str());
}

static void maybeCollectGarbage(TestScriptState& state)
{
	// collectCompartmentGarbage assumes that no WebAssembly code is running in the compartment, so
	// only run it for the root test script state, and if it has no active child threads.
	if(state.kind == TestScriptStateKind::root && !state.threads.size())
	{ collectCompartmentGarbage(state.compartment); }
}

static bool processAction(TestScriptState& state, Action* action, std::vector<Value>* outResults)
{
	if(outResults) { outResults->clear(); }

	switch(action->type)
	{
	case ActionType::_module: {
		auto moduleAction = (ModuleAction*)action;

		// Clear the previous module.
		state.lastInstance = nullptr;
		maybeCollectGarbage(state);

		// Link and instantiate the module.
		TestScriptResolver resolver(state);
		LinkResult linkResult = linkModule(*moduleAction->module, resolver);
		if(linkResult.success)
		{
			std::string moduleDebugName
				= std::string(state.scriptFilename) + ":" + action->locus.describe();

			if(state.config.traceLLVMIR)
			{ traceLLVMIR(moduleDebugName.c_str(), *moduleAction->module); }

			ModuleRef compiledModule = compileModule(*moduleAction->module);

			if(state.config.traceAssembly)
			{ traceAssembly(moduleDebugName.c_str(), compiledModule); }

			state.hasInstantiatedModule = true;
			state.lastInstance = instantiateModule(state.compartment,
												   compiledModule,
												   std::move(linkResult.resolvedImports),
												   std::move(moduleDebugName));

			// Call the module start function, if it has one.
			Function* startFunction = getStartFunction(state.lastInstance);
			if(startFunction) { invokeFunction(state.context, startFunction); }
		}
		else
		{
			// Create an error for each import that couldn't be linked.
			for(auto& missingImport : linkResult.missingImports)
			{
				testErrorf(state,
						   moduleAction->locus,
						   "missing import module=\"%s\" export=\"%s\" type=\"%s\"",
						   missingImport.moduleName.c_str(),
						   missingImport.exportName.c_str(),
						   asString(missingImport.type).c_str());
			}
		}

		// Register the module under its internal name.
		if(moduleAction->internalModuleName.size())
		{
			state.moduleInternalNameToInstanceMap.set(moduleAction->internalModuleName,
													  state.lastInstance);
		}

		return true;
	}
	case ActionType::invoke: {
		auto invokeAction = (InvokeAction*)action;

		// Look up the module this invoke uses.
		Instance* instance = getModuleContextByInternalName(
			state, invokeAction->locus, "invoke", invokeAction->internalModuleName);

		// A null instance at this point indicates a module that failed to link or instantiate, so
		// don't produce further errors.
		if(!instance) { return false; }

		// Find the named export in the instance.
		auto function = asFunctionNullable(getInstanceExport(instance, invokeAction->exportName));
		if(!function)
		{
			testErrorf(state,
					   invokeAction->locus,
					   "couldn't find exported function with name: %s",
					   invokeAction->exportName.c_str());
			return false;
		}

		// Split the tagged argument values into their types and untagged values.
		std::vector<ValueType> argTypes;
		std::vector<UntaggedValue> untaggedArgs;
		for(const Value& arg : invokeAction->arguments)
		{
			argTypes.push_back(arg.type);
			untaggedArgs.push_back(arg);
		}

		// Infer the expected type of the function from the number and type of the invoke's
		// arguments and the function's actual result types.
		const FunctionType invokeSig(getFunctionType(function).results(), TypeTuple(argTypes));

		// Allocate an array to receive the invoke results.
		std::vector<UntaggedValue> untaggedResults;
		untaggedResults.resize(invokeSig.results().size());

		// Invoke the function.
		invokeFunction(
			state.context, function, invokeSig, untaggedArgs.data(), untaggedResults.data());

		// Convert the untagged result values to tagged values.
		if(outResults)
		{
			outResults->resize(invokeSig.results().size());
			for(Uptr resultIndex = 0; resultIndex < untaggedResults.size(); ++resultIndex)
			{
				const ValueType resultType = invokeSig.results()[resultIndex];
				const UntaggedValue& untaggedResult = untaggedResults[resultIndex];
				(*outResults)[resultIndex] = Value(resultType, untaggedResult);
			}
		}

		return true;
	}
	case ActionType::get: {
		auto getAction = (GetAction*)action;

		// Look up the module this get uses.
		Instance* instance = getModuleContextByInternalName(
			state, getAction->locus, "get", getAction->internalModuleName);

		// A null instance at this point indicates a module that failed to link or instantiate, so
		// just return without further errors.
		if(!instance) { return false; }

		// Find the named export in the instance.
		auto global = asGlobalNullable(getInstanceExport(instance, getAction->exportName));
		if(!global)
		{
			testErrorf(state,
					   getAction->locus,
					   "couldn't find exported global with name: %s",
					   getAction->exportName.c_str());
			return false;
		}

		// Get the value of the specified global.
		if(outResults) { *outResults = {getGlobalValue(state.context, global)}; }

		return true;
	}
	default: WAVM_UNREACHABLE();
	}
}

// Tests whether a float is an "arithmetic" NaN, which have the MSB of the significand set.
template<typename Float> bool isArithmeticNaN(Float value)
{
	FloatComponents<Float> components;
	components.value = value;
	return components.bits.exponent == FloatComponents<Float>::maxExponentBits
		   && components.bits.significand >= FloatComponents<Float>::canonicalSignificand;
}

// Tests whether a float is a "canonical" NaN, which *only* have the MSB of the significand set.
template<typename Float> bool isCanonicalNaN(Float value)
{
	FloatComponents<Float> components;
	components.value = value;
	return components.bits.exponent == FloatComponents<Float>::maxExponentBits
		   && components.bits.significand == FloatComponents<Float>::canonicalSignificand;
}

template<typename Float>
bool isFloatResultInExpectedSet(Float result, const FloatResultSet<Float>& expectedResultSet)
{
	switch(expectedResultSet.type)
	{
	case FloatResultSet<Float>::Type::canonicalNaN: return isCanonicalNaN(result);
	case FloatResultSet<Float>::Type::arithmeticNaN: return isArithmeticNaN(result);
	case FloatResultSet<Float>::Type::literal:
		return !memcmp(&result, &expectedResultSet.literal, sizeof(Float));

	default: WAVM_UNREACHABLE();
	};
}

static bool isResultInExpectedSet(const Value& result, const ResultSet& expectedResultSet)
{
	switch(expectedResultSet.type)
	{
	case ResultSet::Type::i32_const:
		return result.type == ValueType::i32 && result.i32 == expectedResultSet.i32;
	case ResultSet::Type::i64_const:
		return result.type == ValueType::i64 && result.i64 == expectedResultSet.i64;

	case ResultSet::Type::i8x16_const:
	case ResultSet::Type::i16x8_const:
	case ResultSet::Type::i32x4_const:
	case ResultSet::Type::i64x2_const:
		return result.type == ValueType::v128 && result.v128.i64x2[0] == expectedResultSet.i64x2[0]
			   && result.v128.i64x2[1] == expectedResultSet.i64x2[1];

	case ResultSet::Type::f32_const:
		return result.type == ValueType::f32
			   && isFloatResultInExpectedSet<F32>(result.f32, expectedResultSet.f32);
	case ResultSet::Type::f64_const:
		return result.type == ValueType::f64
			   && isFloatResultInExpectedSet<F64>(result.f64, expectedResultSet.f64);
	case ResultSet::Type::f32x4_const:
		return result.type == ValueType::v128
			   && isFloatResultInExpectedSet<F32>(result.v128.f32x4[0], expectedResultSet.f32x4[0])
			   && isFloatResultInExpectedSet<F32>(result.v128.f32x4[1], expectedResultSet.f32x4[1])
			   && isFloatResultInExpectedSet<F32>(result.v128.f32x4[2], expectedResultSet.f32x4[2])
			   && isFloatResultInExpectedSet<F32>(result.v128.f32x4[3], expectedResultSet.f32x4[3]);
	case ResultSet::Type::f64x2_const:
		return result.type == ValueType::v128
			   && isFloatResultInExpectedSet<F64>(result.v128.f64x2[0], expectedResultSet.f64x2[0])
			   && isFloatResultInExpectedSet<F64>(result.v128.f64x2[1], expectedResultSet.f64x2[1]);

	case ResultSet::Type::ref_extern:
		return isReferenceType(result.type) && result.object == expectedResultSet.object;
	case ResultSet::Type::ref_func:
		return isReferenceType(result.type) && result.object
			   && result.object->kind == ObjectKind::function;
	case ResultSet::Type::ref_null:
		return result.type == asValueType(expectedResultSet.nullReferenceType) && !result.object;
	case ResultSet::Type::either:
		for(const std::shared_ptr<ResultSet>& alternative : expectedResultSet.alternatives)
		{
			if(isResultInExpectedSet(result, *alternative)) { return true; }
		}
		return false;

	default: WAVM_UNREACHABLE();
	};
}

template<typename Float> std::string asString(const FloatResultSet<Float> resultSet)
{
	switch(resultSet.type)
	{
	case FloatResultSet<Float>::Type::canonicalNaN: return "nan:canonical";
	case FloatResultSet<Float>::Type::arithmeticNaN: return "nan:arithmetic";
	case FloatResultSet<Float>::Type::literal: return asString(resultSet.literal);

	default: WAVM_UNREACHABLE();
	};
}

static std::string asString(const ResultSet& resultSet)
{
	switch(resultSet.type)
	{
	case ResultSet::Type::i32_const: return "(i32.const " + asString(resultSet.i32) + ')';
	case ResultSet::Type::i64_const: return "(i64.const " + asString(resultSet.i64) + ')';

	case ResultSet::Type::i8x16_const: {
		std::string string = "(v128.const i8x16";
		for(Uptr laneIndex = 0; laneIndex < 16; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.i8x16[laneIndex]);
		}
		string += ')';
		return string;
	}
	case ResultSet::Type::i16x8_const: {
		std::string string = "(v128.const i16x8";
		for(Uptr laneIndex = 0; laneIndex < 8; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.i16x8[laneIndex]);
		}
		string += ')';
		return string;
	}
	case ResultSet::Type::i32x4_const: {
		std::string string = "(v128.const i32x4";
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.i32x4[laneIndex]);
		}
		string += ')';
		return string;
	}
	case ResultSet::Type::i64x2_const: {
		std::string string = "(v128.const i64x2";
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.i64x2[laneIndex]);
		}
		string += ')';
		return string;
	}

	case ResultSet::Type::f32_const: return "(f32.const " + asString(resultSet.f32) + ')';
	case ResultSet::Type::f64_const: return "(f64.const " + asString(resultSet.f64) + ')';
	case ResultSet::Type::f32x4_const: {
		std::string string = "(v128.const f32x4";
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.f32x4[laneIndex]);
		}
		string += ')';
		return string;
	}
	case ResultSet::Type::f64x2_const: {
		std::string string = "(v128.const f64x2";
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{
			string += ' ';
			string += asString(resultSet.f64x2[laneIndex]);
		}
		string += ')';
		return string;
	}

	case ResultSet::Type::ref_extern: {
		// buffer needs 34 characters:
		// (ref.extern <0xHHHHHHHHHHHHHHHH>)\0
		char buffer[34];
		snprintf(buffer,
				 sizeof(buffer),
				 "(ref.extern <0x%.16" WAVM_PRIxPTR ">)",
				 reinterpret_cast<Uptr>(resultSet.object));
		return std::string(buffer);
	}
	case ResultSet::Type::ref_func: return "(ref.func)";
	case ResultSet::Type::ref_null: {
		// buffer needs 18 characters:
		// (ref.null (func|extern))\0
		char buffer[18];
		snprintf(buffer,
				 sizeof(buffer),
				 "(ref.null %s)",
				 resultSet.nullReferenceType == ReferenceType::funcref ? "func" : "extern");
		return std::string(buffer);
	}
	case ResultSet::Type::either: {
		std::string result = "(either";
		for(const std::shared_ptr<ResultSet>& alternative : resultSet.alternatives)
		{
			result += ' ';
			result += asString(*alternative);
		}
		result += ')';
		return result;
	}

	default: WAVM_UNREACHABLE();
	};
}

static ResultSet asResultSet(const Value& value, ResultSet::Type expectedType)
{
	ResultSet resultSet;
	if(value.type == ValueType::v128
	   && (expectedType == ResultSet::Type::i8x16_const
		   || expectedType == ResultSet::Type::i16x8_const
		   || expectedType == ResultSet::Type::i32x4_const
		   || expectedType == ResultSet::Type::i64x2_const))
	{
		resultSet.type = expectedType;
		memcpy(resultSet.i8x16, value.v128.i8x16, sizeof(V128));
	}
	else if(value.type == ValueType::v128 && expectedType == ResultSet::Type::f32x4_const)
	{
		resultSet.type = expectedType;
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{
			resultSet.f32x4[laneIndex].type = FloatResultSet<F32>::Type::literal;
			resultSet.f32x4[laneIndex].literal = value.v128.f32x4[laneIndex];
		}
	}
	else if(value.type == ValueType::v128 && expectedType == ResultSet::Type::f64x2_const)
	{
		resultSet.type = expectedType;
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{
			resultSet.f64x2[laneIndex].type = FloatResultSet<F64>::Type::literal;
			resultSet.f64x2[laneIndex].literal = value.v128.f64x2[laneIndex];
		}
	}
	else
	{
		switch(value.type)
		{
		case ValueType::i32:
			resultSet.type = ResultSet::Type::i32_const;
			resultSet.i32 = value.i32;
			break;
		case ValueType::i64:
			resultSet.type = ResultSet::Type::i64_const;
			resultSet.i64 = value.i64;
			break;
		case ValueType::f32:
			resultSet.type = ResultSet::Type::f32_const;
			resultSet.f32.type = FloatResultSet<F32>::Type::literal;
			resultSet.f32.literal = value.f32;
			break;
		case ValueType::f64:
			resultSet.type = ResultSet::Type::f64_const;
			resultSet.f64.type = FloatResultSet<F64>::Type::literal;
			resultSet.f64.literal = value.f64;
			break;
		case ValueType::v128:
			resultSet.type = ResultSet::Type::i8x16_const;
			memcpy(resultSet.i8x16, value.v128.i8x16, sizeof(V128));
			break;
		case ValueType::externref:
			resultSet.type = ResultSet::Type::ref_extern;
			resultSet.object = value.object;
			break;
		case ValueType::funcref:
			resultSet.type = ResultSet::Type::ref_extern;
			resultSet.object = asObject(value.function);
			break;

		case ValueType::none:
		case ValueType::any:
		default: WAVM_UNREACHABLE();
		};
	}

	return resultSet;
}

static bool areResultsInExpectedSet(const std::vector<Value>& results,
									const std::vector<ResultSet>& expectedResultSets,
									std::string& outMessage)
{
	if(results.size() != expectedResultSets.size())
	{
		outMessage = "expected " + std::to_string(expectedResultSets.size()) + " results, but got "
					 + asString(results);
		return false;
	}

	for(Uptr resultIndex = 0; resultIndex < results.size(); ++resultIndex)
	{
		if(!isResultInExpectedSet(results[resultIndex], expectedResultSets[resultIndex]))
		{
			outMessage = "expected " + asString(expectedResultSets[resultIndex]) + ", but got "
						 + asString(asResultSet(results[resultIndex],
												expectedResultSets[resultIndex].type));
			if(results.size() != 1) { outMessage += " in result " + std::to_string(resultIndex); }
			return false;
		}
	}

	return true;
}

static void processRegister(TestScriptState& state, const RegisterCommand* registerCommand)
{
	// Look up a module by internal name, and bind the result to the public name.
	Instance* instance = getModuleContextByInternalName(
		state, registerCommand->locus, "register", registerCommand->internalModuleName);
	state.moduleNameToInstanceMap.set(registerCommand->moduleName, instance);
}

static void processAssertReturn(TestScriptState& state, const AssertReturnCommand* assertCommand)
{
	// Execute the action and do a bitwise comparison of the result to the expected result.
	std::vector<Value> actionResults;
	std::string errorMessage;
	if(processAction(state, assertCommand->action.get(), &actionResults)
	   && !areResultsInExpectedSet(actionResults, assertCommand->expectedResultSets, errorMessage))
	{ testErrorf(state, assertCommand->locus, "%s", errorMessage.c_str()); }
}

static void processAssertReturnNaN(TestScriptState& state,
								   const AssertReturnNaNCommand* assertCommand)
{
	// Execute the action and check that the result is a NaN of the expected type.
	std::vector<Value> actionResults;
	if(processAction(state, assertCommand->action.get(), &actionResults))
	{
		if(actionResults.size() != 1)
		{
			testErrorf(state,
					   assertCommand->locus,
					   "expected single floating-point result, but got %s",
					   asString(actionResults).c_str());
		}
		else
		{
			Value actionResult = actionResults[0];
			bool requireCanonicalNaN = false;
			bool isError;
			if(assertCommand->type == Command::assert_return_canonical_nan)
			{
				requireCanonicalNaN = true;
				isError = actionResult.type == ValueType::f32
							  ? !isCanonicalNaN(actionResult.f32)
							  : actionResult.type == ValueType::f64
									? !isCanonicalNaN(actionResult.f64)
									: true;
			}
			else if(assertCommand->type == Command::assert_return_arithmetic_nan)
			{
				isError = actionResult.type == ValueType::f32
							  ? !isArithmeticNaN(actionResult.f32)
							  : actionResult.type == ValueType::f64
									? !isArithmeticNaN(actionResult.f64)
									: true;
			}
			else if(assertCommand->type == Command::assert_return_canonical_nan_f32x4)
			{
				requireCanonicalNaN = true;
				isError = !isCanonicalNaN(actionResult.v128.f32x4[0])
						  || !isCanonicalNaN(actionResult.v128.f32x4[1])
						  || !isCanonicalNaN(actionResult.v128.f32x4[2])
						  || !isCanonicalNaN(actionResult.v128.f32x4[3]);
			}
			else if(assertCommand->type == Command::assert_return_arithmetic_nan_f32x4)
			{
				isError = !isArithmeticNaN(actionResult.v128.f32x4[0])
						  || !isArithmeticNaN(actionResult.v128.f32x4[1])
						  || !isArithmeticNaN(actionResult.v128.f32x4[2])
						  || !isArithmeticNaN(actionResult.v128.f32x4[3]);
			}
			else if(assertCommand->type == Command::assert_return_canonical_nan_f64x2)
			{
				requireCanonicalNaN = true;
				isError = !isCanonicalNaN(actionResult.v128.f64x2[0])
						  || !isCanonicalNaN(actionResult.v128.f64x2[1]);
			}
			else if(assertCommand->type == Command::assert_return_arithmetic_nan_f64x2)
			{
				isError = !isArithmeticNaN(actionResult.v128.f64x2[0])
						  || !isArithmeticNaN(actionResult.v128.f64x2[1]);
			}
			else
			{
				WAVM_UNREACHABLE();
			}

			if(isError)
			{
				testErrorf(state,
						   assertCommand->locus,
						   requireCanonicalNaN ? "expected canonical NaN but got %s"
											   : "expected arithmetic NaN but got %s",
						   asString(actionResult).c_str());
			}
		}
	}
}

static void processAssertReturnFunc(TestScriptState& state,
									const AssertReturnFuncCommand* assertCommand)
{
	// Execute the action and check that the result is a function.
	std::vector<Value> actionResults;
	if(processAction(state, assertCommand->action.get(), &actionResults))
	{
		if(actionResults.size() != 1 || !isReferenceType(actionResults[0].type)
		   || !asFunctionNullable(actionResults[0].object))
		{
			testErrorf(state,
					   assertCommand->locus,
					   "expected single reference result but got %s",
					   asString(actionResults).c_str());
		}
	}
}

static void processAssertTrap(TestScriptState& state, const AssertTrapCommand* assertCommand)
{
	Runtime::catchRuntimeExceptions(
		[&] {
			std::vector<Value> actionResults;
			if(processAction(state, assertCommand->action.get(), &actionResults))
			{
				testErrorf(state,
						   assertCommand->locus,
						   "expected trap but got %s",
						   asString(actionResults).c_str());
			}
		},
		[&](Runtime::Exception* exception) {
			if(!isExpectedExceptionType(assertCommand->expectedType, exception->type))
			{
				testErrorf(state,
						   assertCommand->action->locus,
						   "expected %s trap but got %s trap",
						   describeExpectedTrapType(assertCommand->expectedType).c_str(),
						   describeExceptionType(exception->type).c_str());
			}
			destroyException(exception);
		});
}

static void processAssertThrows(TestScriptState& state, const AssertThrowsCommand* assertCommand)
{
	// Look up the module containing the expected exception type.
	Instance* instance
		= getModuleContextByInternalName(state,
										 assertCommand->locus,
										 "assert_throws",
										 assertCommand->exceptionTypeInternalModuleName);

	// A null instance at this point indicates a module that failed to link or instantiate, so
	// don't produce further errors.
	if(!instance) { return; }

	// Find the named export in the instance.
	auto expectedExceptionType = asExceptionTypeNullable(
		getInstanceExport(instance, assertCommand->exceptionTypeExportName));
	if(!expectedExceptionType)
	{
		testErrorf(state,
				   assertCommand->locus,
				   "couldn't find exported exception type with name: %s",
				   assertCommand->exceptionTypeExportName.c_str());
		return;
	}

	Runtime::catchRuntimeExceptions(
		[&] {
			std::vector<Value> actionResults;
			if(processAction(state, assertCommand->action.get(), &actionResults))
			{
				testErrorf(state,
						   assertCommand->locus,
						   "expected trap but got %s",
						   asString(actionResults).c_str());
			}
		},
		[&](Runtime::Exception* exception) {
			if(exception->type != expectedExceptionType)
			{
				testErrorf(state,
						   assertCommand->action->locus,
						   "expected %s exception but got %s exception",
						   describeExceptionType(expectedExceptionType).c_str(),
						   describeExceptionType(exception->type).c_str());
			}
			else
			{
				TypeTuple exceptionParameterTypes
					= getExceptionTypeParameters(expectedExceptionType);

				for(Uptr argumentIndex = 0; argumentIndex < exceptionParameterTypes.size();
					++argumentIndex)
				{
					Value argumentValue(exceptionParameterTypes[argumentIndex],
										exception->arguments[argumentIndex]);
					if(argumentValue != assertCommand->expectedArguments[argumentIndex])
					{
						testErrorf(
							state,
							assertCommand->locus,
							"expected %s for exception argument %" WAVM_PRIuPTR " but got %s",
							asString(assertCommand->expectedArguments[argumentIndex]).c_str(),
							argumentIndex,
							asString(argumentValue).c_str());
					}
				}
			}
			destroyException(exception);
		});
}

static void processAssertInvalid(TestScriptState& state,
								 const AssertInvalidOrMalformedCommand* assertCommand)
{
	switch(assertCommand->wasInvalidOrMalformed)
	{
	case InvalidOrMalformed::wellFormedAndValid:
		testErrorf(state, assertCommand->locus, "module was well formed and valid");
		break;
	case InvalidOrMalformed::malformed:
		if(state.config.strictAssertInvalid)
		{ testErrorf(state, assertCommand->locus, "module was malformed"); }
		break;

	case InvalidOrMalformed::invalid:
	default: break;
	};
}

static void processAssertMalformed(TestScriptState& state,
								   const AssertInvalidOrMalformedCommand* assertCommand)
{
	switch(assertCommand->wasInvalidOrMalformed)
	{
	case InvalidOrMalformed::wellFormedAndValid:
		testErrorf(state, assertCommand->locus, "module was well formed and valid");
		break;

	case InvalidOrMalformed::invalid:
		if(state.config.strictAssertMalformed)
		{ testErrorf(state, assertCommand->locus, "module was invalid"); }
		break;

	case InvalidOrMalformed::malformed:
	default: break;
	};
}

static void processAssertUnlinkable(TestScriptState& state,
									const AssertUnlinkableCommand* assertCommand)
{
	Runtime::catchRuntimeExceptions(
		[&] {
			TestScriptResolver resolver(state);
			LinkResult linkResult = linkModule(*assertCommand->moduleAction->module, resolver);
			if(linkResult.success)
			{
				auto instance
					= instantiateModule(state.compartment,
										compileModule(*assertCommand->moduleAction->module),
										std::move(linkResult.resolvedImports),
										"test module");

				// Call the module start function, if it has one.
				Function* startFunction = getStartFunction(instance);
				if(startFunction) { invokeFunction(state.context, startFunction); }

				testErrorf(state, assertCommand->locus, "module was linkable");
			}
		},
		[&](Runtime::Exception* exception) {
			destroyException(exception);
			// If the instantiation throws an exception, the assert_unlinkable succeeds.
		});
}

static void processBenchmark(TestScriptState& state, const BenchmarkCommand* benchmarkCommand)
{
	InvokeAction* invokeAction = benchmarkCommand->invokeAction.get();

	// Look up the module this invoke uses.
	Instance* invokeInstance = getModuleContextByInternalName(
		state, invokeAction->locus, "invoke", invokeAction->internalModuleName);

	// A null instance at this point indicates a module that failed to link or instantiate, so don't
	// produce further errors.
	if(!invokeInstance) { return; }

	// Find the named export in the instance.
	auto invokeFunction
		= asFunctionNullable(getInstanceExport(invokeInstance, invokeAction->exportName));
	if(!invokeFunction)
	{
		testErrorf(state,
				   invokeAction->locus,
				   "couldn't find exported function with name: %s",
				   invokeAction->exportName.c_str());
		return;
	}

	// Infer the expected type of the function from the number and type of the invoke's arguments
	// and the function's actual result types.
	std::vector<ValueType> argTypes;
	for(const Value& arg : invokeAction->arguments) { argTypes.push_back(arg.type); }
	const FunctionType invokeFunctionSig(getFunctionType(invokeFunction).results(),
										 TypeTuple(argTypes));

	// Check the actual signature of the invoked function.
	if(getFunctionType(invokeFunction) != invokeFunctionSig)
	{
		if(Log::isCategoryEnabled(Log::debug))
		{
			Log::printf(
				Log::debug,
				"Invoke signature mismatch:\n  Invoke signature: %s\n  Invoked function type: %s\n",
				asString(invokeFunctionSig).c_str(),
				asString(getFunctionType(invokeFunction)).c_str());
		}
		throwException(ExceptionTypes::invokeSignatureMismatch);
	}

	// Generate a WASM module that imports the function being invoked.
	const FunctionType benchmarkFunctionSig({}, {ValueType::i32});
	IR::Module benchmarkModule(IR::FeatureLevel::wavm);
	benchmarkModule.types.push_back(invokeFunctionSig);
	benchmarkModule.types.push_back(benchmarkFunctionSig);
	benchmarkModule.functions.imports.push_back({{0}, "", ""});
	benchmarkModule.imports.push_back({ExternKind::function, 0});

	// Generate a function that calls the imported function a variable number of times.
	Serialization::ArrayOutputStream codeByteStream;
	OperatorEncoderStream encoder(codeByteStream);

	encoder.loop({{IndexedBlockType::noParametersOrResult}});

	// Exit the loop once the number of iterations remaining reaches zero.
	encoder.local_get({0});
	encoder.i32_eqz();
	encoder.br_if({1});

	// Decrement the number of iterations remaining.
	encoder.local_get({0});
	encoder.i32_const({1});
	encoder.i32_sub();
	encoder.local_set({0});

	// Translate the invoke arguments to XXX.const instructions.
	for(const Value& arg : invokeAction->arguments)
	{
		switch(arg.type)
		{
		case ValueType::i32: encoder.i32_const({arg.i32}); break;
		case ValueType::i64: encoder.i64_const({arg.i64}); break;
		case ValueType::f32: encoder.f32_const({arg.f32}); break;
		case ValueType::f64: encoder.f64_const({arg.f64}); break;
		case ValueType::v128: encoder.v128_const({arg.v128}); break;

		case ValueType::externref:
		case ValueType::funcref: Errors::unimplemented("Benchmark invoke reference arguments");

		case ValueType::any:
		case ValueType::none:
		default: WAVM_UNREACHABLE();
		};
	}

	// Call the imported invoke function.
	encoder.call({0});

	// Drop all the invoked functions' results.
	for(const ValueType result : invokeFunctionSig.results())
	{
		WAVM_SUPPRESS_UNUSED(result);
		encoder.drop();
	}

	// Branch to the beginning of the loop.
	encoder.br({0});

	// End the loop and the function body.
	encoder.end();
	encoder.end();

	benchmarkModule.functions.defs.push_back({{1}, {}, codeByteStream.getBytes(), {}});
	benchmarkModule.exports.push_back({"benchmark", ExternKind::function, 1});

	// Validate the generated module in builds with assertions enabled.
	if(WAVM_ENABLE_ASSERTS)
	{
		std::shared_ptr<IR::ModuleValidationState> moduleValidationState
			= IR::createModuleValidationState(benchmarkModule);
		IR::validatePreCodeSections(*moduleValidationState);
		IR::validateCodeSection(*moduleValidationState);
		IR::validatePostCodeSections(*moduleValidationState);
	}

	// Compile and instantiate the generated module.
	Instance* benchmarkInstance = instantiateModule(
		state.compartment, compileModule(benchmarkModule), {asObject(invokeFunction)}, "benchmark");
	Function* benchmarkFunction = getTypedInstanceExport(
		benchmarkInstance, "benchmark", FunctionType({}, {ValueType::i32}));

	// Run the benchmark once to warm up caches (not just CPU, also the cached invoke thunk).
	UntaggedValue benchmarkArgs[1];
	benchmarkArgs[0].u32 = 10;
	Runtime::invokeFunction(state.context, benchmarkFunction, benchmarkFunctionSig, benchmarkArgs);

	// Sample the benchmark until 20ms or 1 million samples have been collected.
	static constexpr F64 minSamplingNS = 20000000.0;
	static constexpr Uptr maxSamples = 1000000;
	U32 numIterationsPerSample = 10;
	F64 totalNS = 0.0;
	Uptr numSamples = 0;
	Uptr numIterations = 0;
	while(numSamples < maxSamples && totalNS < minSamplingNS)
	{
		benchmarkArgs[0].u32 = numIterationsPerSample;
		Timing::Timer timer;
		Runtime::invokeFunction(
			state.context, benchmarkFunction, benchmarkFunctionSig, benchmarkArgs);
		timer.stop();
		totalNS += timer.getNanoseconds();
		++numSamples;
		numIterations += numIterationsPerSample;
		numIterationsPerSample = numIterationsPerSample * 3 / 2;
	};
	const F64 nsPerIteration = totalNS / numIterations;

	Log::printf(Log::output,
				"%s benchmark: %.1fns (%" WAVM_PRIuPTR " iterations)\n",
				benchmarkCommand->name.c_str(),
				nsPerIteration,
				numIterations);
}

static void processCommands(TestScriptState& state,
							const std::vector<std::unique_ptr<Command>>& commands);

static I64 testScriptThreadMain(void* sharedStateVoid)
{
	TestScriptThread* testScriptThread = (TestScriptThread*)sharedStateVoid;
	processCommands(testScriptThread->state, testScriptThread->threadCommand->commands);
	return 0;
}

static void processThread(TestScriptState& state, const ThreadCommand* threadCommand)
{
	std::unique_ptr<TestScriptThread> thread(
		new TestScriptThread{state.forkThread(), threadCommand, nullptr});

	for(const std::string& sharedModuleInternalName : threadCommand->sharedModuleInternalNames)
	{
		GCPointer<Instance>* sharedInstance
			= state.moduleInternalNameToInstanceMap.get(sharedModuleInternalName);
		if(!sharedInstance)
		{
			testErrorf(state,
					   threadCommand->locus,
					   "No module named %s to share.",
					   sharedModuleInternalName.c_str());
		}
		else if(!thread->state.moduleInternalNameToInstanceMap.add(sharedModuleInternalName,
																   *sharedInstance))
		{
			testErrorf(state,
					   threadCommand->locus,
					   "Module named %s may only be shared once.",
					   sharedModuleInternalName.c_str());
		}
		thread->state.hasInstantiatedModule = true;
	}

	thread->platformThread
		= Platform::createThread(threadStackNumBytes, testScriptThreadMain, thread.get());
	if(!state.threads.add(threadCommand->threadName, std::move(thread)))
	{
		testErrorf(state,
				   threadCommand->locus,
				   "A thread with the name %s is already running.",
				   threadCommand->threadName.c_str());
	}
}

static void processWait(TestScriptState& state, const WaitCommand* waitCommand)
{
	std::unique_ptr<TestScriptThread>* maybeThread = state.threads.get(waitCommand->threadName);
	if(!maybeThread)
	{
		testErrorf(state,
				   waitCommand->locus,
				   "There is no thread running with the name %s.",
				   waitCommand->threadName.c_str());
	}
	else
	{
		std::unique_ptr<TestScriptThread>& thread = *maybeThread;
		const I64 threadResult = Platform::joinThread(thread->platformThread);
		if(threadResult)
		{
			testErrorf(state,
					   waitCommand->locus,
					   "Thread %s exited with error code %" PRId64 ".",
					   waitCommand->threadName.c_str(),
					   threadResult);
		}

		state.errors.insert(
			state.errors.end(), thread->state.errors.begin(), thread->state.errors.end());

		state.threads.removeOrFail(waitCommand->threadName);
	}

	// The thread might have been blocking garbage collection, so try collecting garbage after
	// waiting for it to end.
	maybeCollectGarbage(state);
}

static void processCommand(TestScriptState& state, const Command* command)
{
	switch(command->type)
	{
	case Command::_register: processRegister(state, (RegisterCommand*)command); break;
	case Command::action:
		processAction(state, ((ActionCommand*)command)->action.get(), nullptr);
		break;
	case Command::assert_return: processAssertReturn(state, (AssertReturnCommand*)command); break;
	case Command::assert_return_canonical_nan:
	case Command::assert_return_arithmetic_nan:
	case Command::assert_return_canonical_nan_f32x4:
	case Command::assert_return_arithmetic_nan_f32x4:
	case Command::assert_return_canonical_nan_f64x2:
	case Command::assert_return_arithmetic_nan_f64x2:
		processAssertReturnNaN(state, (AssertReturnNaNCommand*)command);
		break;
	case Command::assert_return_func:
		processAssertReturnFunc(state, (AssertReturnFuncCommand*)command);
		break;
	case Command::assert_trap: processAssertTrap(state, (AssertTrapCommand*)command); break;
	case Command::assert_throws: processAssertThrows(state, (AssertThrowsCommand*)command); break;
	case Command::assert_invalid:
		processAssertInvalid(state, (AssertInvalidOrMalformedCommand*)command);
		break;
	case Command::assert_malformed:
		processAssertMalformed(state, (AssertInvalidOrMalformedCommand*)command);
		break;
	case Command::assert_unlinkable:
		processAssertUnlinkable(state, (AssertUnlinkableCommand*)command);
		break;
	case Command::benchmark: processBenchmark(state, (BenchmarkCommand*)command); break;
	case Command::thread: processThread(state, (ThreadCommand*)command); break;
	case Command::wait: processWait(state, (WaitCommand*)command); break;

	default: WAVM_UNREACHABLE();
	};
}

static void processCommandWithCloning(TestScriptState& state, const Command* command)
{
	// If testing cloning is disabled, or this is running in a thread, or with child threads, or
	// about to start a child thread, don't clone the compartment. This avoids issues with trying to
	// synchronize cloning the compartment between the root and child states.
	if(!state.config.testCloning || state.kind != TestScriptStateKind::root || state.threads.size()
	   || command->type == Command::Type::thread)
	{
		processCommand(state, command);
		return;
	}

	const Uptr originalNumErrors = state.errors.size();

	// Clone the test compartment and state.
	TestScriptState* maybeClonedState = state.clone();
	if(!maybeClonedState)
	{
		testErrorf(state,
				   command->locus,
				   "Failed to clone compartment due to failure to allocate memory.");
	}

	// Process the command in both the original and the cloned compartments.
	processCommand(state, command);

	if(maybeClonedState)
	{
		TestScriptState& clonedState = *maybeClonedState;

		processCommand(clonedState, command);

		// Check that the command produced the same errors in both the original and cloned
		// compartments.
		if(state.errors.size() != clonedState.errors.size())
		{
			testErrorf(state,
					   command->locus,
					   "Command produced different number of errors in cloned compartment");
		}
		else
		{
			for(Uptr errorIndex = originalNumErrors; errorIndex < state.errors.size(); ++errorIndex)
			{
				if(state.errors[errorIndex] != clonedState.errors[errorIndex])
				{
					testErrorf(state,
							   clonedState.errors[errorIndex].locus,
							   "Error only occurs in cloned state: %s",
							   clonedState.errors[errorIndex].message.c_str());
				}
			}
		}

		// Check that the original and cloned memory are the same after processing the command.
		if(state.lastInstance && clonedState.lastInstance)
		{
			WAVM_ASSERT(state.lastInstance != clonedState.lastInstance);

			Memory* memory = getDefaultMemory(state.lastInstance);
			Memory* clonedMemory = getDefaultMemory(clonedState.lastInstance);
			if(memory && clonedMemory)
			{
				WAVM_ASSERT(memory != clonedMemory);

				const Uptr numMemoryPages = getMemoryNumPages(memory);
				const Uptr numClonedMemoryPages = getMemoryNumPages(clonedMemory);

				if(numMemoryPages != numClonedMemoryPages)
				{
					testErrorf(state,
							   command->locus,
							   "Cloned memory size doesn't match (original = %" WAVM_PRIuPTR
							   " pages, cloned = %" WAVM_PRIuPTR " pages",
							   numMemoryPages,
							   numClonedMemoryPages);
				}
				else
				{
					const Uptr numMemoryBytes = numMemoryPages * IR::numBytesPerPage;
					U8* memoryBytes = memoryArrayPtr<U8>(memory, 0, numMemoryBytes);
					U8* clonedMemoryBytes = memoryArrayPtr<U8>(clonedMemory, 0, numMemoryBytes);
					if(memcmp(memoryBytes, clonedMemoryBytes, numMemoryBytes))
					{
						for(Uptr byteIndex = 0; byteIndex < numMemoryBytes; ++byteIndex)
						{
							const U8 value = memoryBytes[byteIndex];
							const U8 clonedValue = clonedMemoryBytes[byteIndex];
							if(value != clonedValue)
							{
								testErrorf(
									state,
									command->locus,
									"Memory differs from cloned memory at address 0x08%" WAVM_PRIxPTR
									": 0x%02x vs 0x%02x",
									byteIndex,
									value,
									clonedValue);
							}
						}
					}
				}
			}
		}

		delete maybeClonedState;
	}
}

static void processCommands(TestScriptState& state,
							const std::vector<std::unique_ptr<Command>>& commands)
{
	for(auto& command : commands)
	{
		if(state.config.traceTests)
		{
			Log::printf(Log::output,
						"Evaluating test command at %s(%s)\n",
						state.scriptFilename,
						command->locus.describe().c_str());
		}
		catchRuntimeExceptions(
			[&state, &command] { processCommandWithCloning(state, command.get()); },
			[&state, &command](Runtime::Exception* exception) {
				testErrorf(state,
						   command->locus,
						   "unexpected trap: %s",
						   describeExceptionType(exception->type).c_str());
				destroyException(exception);
			});
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(spectest, "print", void, spectest_print) {}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest, "print_i32", void, spectest_print_i32, I32 a)
{
	Log::printf(Log::debug, "%s : i32\n", asString(a).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest, "print_i64", void, spectest_print_i64, I64 a)
{
	Log::printf(Log::debug, "%s : i64\n", asString(a).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest, "print_f32", void, spectest_print_f32, F32 a)
{
	Log::printf(Log::debug, "%s : f32\n", asString(a).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest, "print_f64", void, spectest_print_f64, F64 a)
{
	Log::printf(Log::debug, "%s : f64\n", asString(a).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest,
							   "print_f64_f64",
							   void,
							   spectest_print_f64_f64,
							   F64 a,
							   F64 b)
{
	Log::printf(Log::debug, "%s : f64\n%s : f64\n", asString(a).c_str(), asString(b).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest,
							   "print_i32_f32",
							   void,
							   spectest_print_i32_f32,
							   I32 a,
							   F32 b)
{
	Log::printf(Log::debug, "%s : i32\n%s : f32\n", asString(a).c_str(), asString(b).c_str());
}
WAVM_DEFINE_INTRINSIC_FUNCTION(spectest,
							   "print_i64_f64",
							   void,
							   spectest_print_i64_f64,
							   I64 a,
							   F64 b)
{
	Log::printf(Log::debug, "%s : i64\n%s : f64\n", asString(a).c_str(), asString(b).c_str());
}

WAVM_DEFINE_INTRINSIC_GLOBAL(spectest, "global_i32", I32, spectest_global_i32, 666)
WAVM_DEFINE_INTRINSIC_GLOBAL(spectest, "global_i64", I64, spectest_global_i64, 666)
WAVM_DEFINE_INTRINSIC_GLOBAL(spectest, "global_f32", F32, spectest_global_f32, 666.0f)
WAVM_DEFINE_INTRINSIC_GLOBAL(spectest, "global_f64", F64, spectest_global_f64, 666.0)

WAVM_DEFINE_INTRINSIC_TABLE(
	spectest,
	spectest_table,
	table,
	TableType(ReferenceType::funcref, false, IndexType::i32, SizeConstraints{10, 20}))
WAVM_DEFINE_INTRINSIC_MEMORY(spectest,
							 spectest_memory,
							 memory,
							 MemoryType(false, IndexType::i32, SizeConstraints{1, 2}))
WAVM_DEFINE_INTRINSIC_MEMORY(spectest,
							 spectest_shared_memory,
							 shared_memory,
							 MemoryType(true, IndexType::i32, SizeConstraints{1, 2}))

struct SharedState
{
	Config config;

	Platform::Mutex mutex;
	std::vector<const char*> pendingFilenames;
};

static I64 threadMain(void* sharedStateVoid)
{
	auto sharedState = (SharedState*)sharedStateVoid;

	// Process files from sharedState->pendingFilenames until they've all been processed.
	I64 numErrors = 0;
	while(true)
	{
		const char* filename;
		{
			Platform::Mutex::Lock sharedStateLock(sharedState->mutex);
			if(!sharedState->pendingFilenames.size()) { break; }
			filename = sharedState->pendingFilenames.back();
			sharedState->pendingFilenames.pop_back();
		}

		// Read the file into a vector.
		std::vector<U8> testScriptBytes;
		if(!loadFile(filename, testScriptBytes))
		{
			++numErrors;
			continue;
		}

		// Make sure the file is null terminated.
		testScriptBytes.push_back(0);

		// Process the test script.
		TestScriptState testScriptState(filename, sharedState->config);
		std::vector<std::unique_ptr<Command>> testCommands;

		// Parse the test script.
		WAST::parseTestCommands((const char*)testScriptBytes.data(),
								testScriptBytes.size(),
								testScriptState.config.featureSpec,
								testCommands,
								testScriptState.errors);
		if(!testScriptState.errors.size())
		{
			// Process the test script commands.
			processCommands(testScriptState, testCommands);
		}
		numErrors += testScriptState.errors.size();

		// Print any errors.
		reportParseErrors(filename, (const char*)testScriptBytes.data(), testScriptState.errors);
	}

	return numErrors;
}

static void showHelp()
{
	Log::printf(
		Log::error,
		"Usage: wavm test script [options] in.wast [options]\n"
		"  -h|--help                  Display this message\n"
		"  -l <N>|--loop <N>          Run tests N times in a loop until an error occurs\n"
		"  --strict-assert-invalid    Strictly evaluate assert_invalid, failing if the\n"
		"                             module was malformed\n"
		"  --strict-assert-malformed  Strictly evaluate assert_malformed, failing if the\n"
		"                             module was invalid\n"
		"  --test-cloning             Run each test command in the original compartment\n"
		"                             and a clone of it, and compare the resulting state\n"
		"  --trace                    Prints instructions to stdout as they are compiled.\n"
		"  --trace-tests              Prints test commands to stdout as they are executed.\n"
		"  --trace-llvmir             Prints the LLVM IR for modules as they are compiled.\n"
		"  --trace-assembly           Prints the machine assembly for modules as they are\n"
		"                             compiled.\n");
}

int execRunTestScript(int argc, char** argv)
{
	// Parse the command-line.
	Uptr numLoops = 1;
	std::vector<const char*> filenames;
	Config config;
	for(int argIndex = 0; argIndex < argc; ++argIndex)
	{
		if(!strcmp(argv[argIndex], "--help") || !strcmp(argv[argIndex], "-h"))
		{
			showHelp();
			return EXIT_SUCCESS;
		}
		else if(!strcmp(argv[argIndex], "--loop") || !strcmp(argv[argIndex], "-l"))
		{
			if(argIndex + 1 >= argc)
			{
				showHelp();
				return EXIT_FAILURE;
			}
			++argIndex;
			long int numLoopsLongInt = strtol(argv[argIndex], nullptr, 10);
			if(numLoopsLongInt <= 0)
			{
				showHelp();
				return EXIT_FAILURE;
			}
			numLoops = Uptr(numLoopsLongInt);
		}
		else if(!strcmp(argv[argIndex], "--strict-assert-invalid"))
		{
			config.strictAssertInvalid = true;
		}
		else if(!strcmp(argv[argIndex], "--strict-assert-malformed"))
		{
			config.strictAssertMalformed = true;
		}
		else if(!strcmp(argv[argIndex], "--test-cloning"))
		{
			config.testCloning = true;
		}
		else if(!strcmp(argv[argIndex], "--trace"))
		{
			Log::setCategoryEnabled(Log::traceValidation, true);
			Log::setCategoryEnabled(Log::traceCompilation, true);
		}
		else if(!strcmp(argv[argIndex], "--trace-tests"))
		{
			config.traceTests = true;
		}
		else if(!strcmp(argv[argIndex], "--trace-llvmir"))
		{
			config.traceLLVMIR = true;
		}
		else if(!strcmp(argv[argIndex], "--trace-assembly"))
		{
			config.traceAssembly = true;
		}
		else if(!strcmp(argv[argIndex], "--enable") || !strcmp(argv[argIndex], "--disable"))
		{
			const bool enableFeature = !strcmp(argv[argIndex], "--enable");

			++argIndex;
			if(!argv[argIndex])
			{
				Log::printf(Log::error, "Expected feature name following '--enable'.\n");
				return EXIT_FAILURE;
			}

			if(!parseAndSetFeature(argv[argIndex], config.featureSpec, enableFeature))
			{
				Log::printf(Log::error,
							"Unknown feature '%s'. Supported features:\n"
							"%s"
							"\n",
							argv[argIndex],
							getFeatureListHelpText().c_str());
				return EXIT_FAILURE;
			}
		}
		else
		{
			filenames.push_back(argv[argIndex]);
		}
	}

	if(!filenames.size())
	{
		showHelp();
		return EXIT_FAILURE;
	}

	Uptr loopIndex = 0;
	while(true)
	{
		Timing::Timer timer;

		SharedState sharedState;
		sharedState.config = config;
		sharedState.pendingFilenames = filenames;

		// Create a thread for each hardware thread.
		std::vector<Platform::Thread*> threads;
		const Uptr numHardwareThreads = Platform::getNumberOfHardwareThreads();
		const Uptr numTestThreads
			= std::min(numHardwareThreads, Uptr(sharedState.pendingFilenames.size()));
		for(Uptr threadIndex = 0; threadIndex < numTestThreads; ++threadIndex)
		{
			threads.push_back(
				Platform::createThread(threadStackNumBytes, threadMain, &sharedState));
		}

		// Wait for the threads to exit, summing up their return code, which will be the number of
		// errors found by the thread.
		I64 numErrors = 0;
		for(Platform::Thread* thread : threads) { numErrors += Platform::joinThread(thread); }

		if(numErrors)
		{
			Log::printf(Log::error, "Testing failed with %" PRIi64 " error(s)\n", numErrors);
			return EXIT_FAILURE;
		}
		else
		{
			Log::printf(Log::output, "All tests succeeded in %.1fms!\n", timer.getMilliseconds());
			if(++loopIndex >= numLoops) { return EXIT_SUCCESS; }
		}
	}
}
