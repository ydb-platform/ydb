#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <utility>
#include <vector>

#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Thread.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"
#include "WAVM/WASTParse/WASTParse.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

template<typename Result> struct ContextAndResult
{
	ContextRuntimeData* contextRuntimeData;
	Result result;
};

typedef ContextAndResult<I32> (*NopFunctionPointer)(ContextRuntimeData*);

struct ThreadArgs
{
	Context* context = nullptr;
	Function* function = nullptr;
	F64 elapsedNanoseconds = 0;
	Platform::Thread* thread = nullptr;
};

void runBenchmark(Compartment* compartment,
				  Function* function,
				  Uptr numThreads,
				  const char* description,
				  I64 (*threadFunc)(void*))
{
	// Create a thread for each hardware thread.
	std::vector<ThreadArgs*> threads;
	for(Uptr threadIndex = 0; threadIndex < numThreads; ++threadIndex)
	{
		ThreadArgs* threadArgs = new ThreadArgs;
		threadArgs->context = createContext(compartment);
		threadArgs->function = function;
		threadArgs->thread = Platform::createThread(512 * 1024, threadFunc, threadArgs);
		threads.push_back(threadArgs);
	}

	// Wait for the threads to exit, and sum the results from each thread.
	F64 totalElapsedNanoseconds = 0;
	for(ThreadArgs* threadArgs : threads)
	{
		Platform::joinThread(threadArgs->thread);
		totalElapsedNanoseconds += threadArgs->elapsedNanoseconds;
		delete threadArgs;
	}

	// Print the results.
	const F64 averageNanoseconds = totalElapsedNanoseconds / F64(numThreads);

	Log::printf(Log::output,
				"ns/%s in %" WAVM_PRIuPTR " threads: %.2f\n",
				description,
				numThreads,
				averageNanoseconds);
}

void runBenchmarkSingleAndMultiThreaded(Compartment* compartment,
										Function* function,
										const char* description,
										I64 (*threadFunc)(void*))
{
	const Uptr numHardwareThreads = Platform::getNumberOfHardwareThreads() / 2;
	runBenchmark(compartment, function, 1, description, threadFunc);
	runBenchmark(compartment, function, numHardwareThreads, description, threadFunc);
}

void showBenchmarkHelp(WAVM::Log::Category outputCategory)
{
	Log::printf(outputCategory, "Usage: wavm test bench\n");
}

static constexpr Uptr numInvokesPerThread = 100000000;

void runInvokeBench()
{
	// Generate a nop function.
	Serialization::ArrayOutputStream codeStream;
	OperatorEncoderStream encoder(codeStream);
	encoder.i32_const({0});
	encoder.end();

	// Generate a module containing the nop function.
	IR::Module irModule;
	DisassemblyNames irModuleNames;
	irModule.types.push_back(FunctionType({ValueType::i32}, {ValueType::i32}));
	irModule.functions.defs.push_back({{0}, {}, std::move(codeStream.getBytes()), {}});
	irModule.exports.push_back({"nopFunction", IR::ExternKind::function, 0});
	irModuleNames.functions.push_back({"nopFunction", {}, {}});
	IR::setDisassemblyNames(irModule, irModuleNames);
	std::shared_ptr<IR::ModuleValidationState> moduleValidationState
		= IR::createModuleValidationState(irModule);
	IR::validatePreCodeSections(*moduleValidationState);
	IR::validatePostCodeSections(*moduleValidationState);

	// Instantiate the module.
	GCPointer<Compartment> compartment = Runtime::createCompartment();
	auto module = compileModule(irModule);
	auto instance = instantiateModule(compartment, module, {}, "nopModule");
	auto function = asFunction(getInstanceExport(instance, "nopFunction"));

	// Call the nop function once to ensure the time to create the invoke thunk isn't benchmarked.
	{
		IR::Value args[1]{I32(0)};
		IR::Value results[1];
		invokeFunction(createContext(compartment),
					   function,
					   FunctionType({ValueType::i32}, {ValueType::i32}),
					   args,
					   results);
	}

	// Benchmark calling the function directly.
	runBenchmarkSingleAndMultiThreaded(
		compartment, function, "direct call", [](void* argument) -> I64 {
			ThreadArgs* threadArgs = (ThreadArgs*)argument;
			ContextRuntimeData* contextRuntimeData = getContextRuntimeData(threadArgs->context);

			Timing::Timer timer;
			for(Uptr repeatIndex = 0; repeatIndex < numInvokesPerThread; ++repeatIndex)
			{ (*(NopFunctionPointer)&threadArgs->function->code[0])(contextRuntimeData); }
			timer.stop();

			threadArgs->elapsedNanoseconds = timer.getNanoseconds() / F64(numInvokesPerThread);

			return 0;
		});

	// Benchmark invokeFunction.
	runBenchmarkSingleAndMultiThreaded(
		compartment, function, "invokeFunction", [](void* argument) -> I64 {
			ThreadArgs* threadArgs = (ThreadArgs*)argument;

			FunctionType invokeSig({ValueType::i32}, {ValueType::i32});

			Timing::Timer timer;
			for(Uptr repeatIndex = 0; repeatIndex < numInvokesPerThread; ++repeatIndex)
			{
				UntaggedValue args[1]{I32(0)};
				UntaggedValue results[1];
				invokeFunction(threadArgs->context, threadArgs->function, invokeSig, args, results);
			}
			timer.stop();

			threadArgs->elapsedNanoseconds = timer.getNanoseconds() / F64(numInvokesPerThread);

			return 0;
		});

	// Free the compartment.
	WAVM_ERROR_UNLESS(tryCollectCompartment(std::move(compartment)));
}

WAVM_DEFINE_INTRINSIC_MODULE(benchmarkIntrinsics);

WAVM_DEFINE_INTRINSIC_FUNCTION(benchmarkIntrinsics, "identity", I32, intrinsicIdentity, I32 x)
{
	return x;
}

static constexpr Uptr numIntrinsicCallsPerThread = 1000000000;

static constexpr const char* intrinsicBenchModuleWAST
	= "(module\n"
	  "  (import \"benchmarkIntrinsics\" \"identity\" (func $identity (param i32) (result i32)))\n"
	  "  (func (export \"benchmarkIntrinsicFunc\") (param $numIterations i32) (result i32)\n"
	  "    (local $i i32)\n"
	  "    (local $acc i32)\n"
	  "    loop $loop\n"
	  "      (local.set $acc (i32.add (local.get $acc)\n"
	  "                               (call $identity (i32.const 1))))\n"
	  "      (local.set $i (i32.add (local.get $i) (i32.const 1)))\n"
	  "      (br_if $loop (i32.ne (local.get $i) (local.get $numIterations)))\n"
	  "    end\n"
	  "    (local.get $acc)\n"
	  "  )\n"
	  ")";

void runIntrinsicBench()
{
	// Parse the intrinsic benchmark module.
	std::vector<WAST::Error> parseErrors;
	IR::Module irModule;
	if(!WAST::parseModule(
		   intrinsicBenchModuleWAST, strlen(intrinsicBenchModuleWAST) + 1, irModule, parseErrors))
	{
		WAST::reportParseErrors(
			"intrinsic benchmark module", intrinsicBenchModuleWAST, parseErrors);
		Errors::fatal("Failed to parse intrinsic benchmark module WAST");
	}

	// Instantiate the intrinsic module
	GCPointer<Compartment> compartment = Runtime::createCompartment();
	auto intrinsicInstance = Intrinsics::instantiateModule(
		compartment, {WAVM_INTRINSIC_MODULE_REF(benchmarkIntrinsics)}, "benchmarkIntrinsics");
	auto intrinsicIdentityFunction = getInstanceExport(intrinsicInstance, "identity");

	// Instantiate the WASM module.
	auto module = compileModule(irModule);
	auto instance = instantiateModule(
		compartment, module, {intrinsicIdentityFunction}, "benchmarkIntrinsicModule");
	auto function = asFunction(getInstanceExport(instance, "benchmarkIntrinsicFunc"));

	// Call the benchmark function once to ensure the time to create the invoke thunk isn't
	// benchmarked.
	{
		IR::Value args[1]{I32(1)};
		IR::Value results[1];
		invokeFunction(createContext(compartment),
					   function,
					   FunctionType({ValueType::i32}, {ValueType::i32}),
					   args,
					   results);
	}

	// Run the benchmark.
	runBenchmarkSingleAndMultiThreaded(
		compartment, function, "intrinsic call", [](void* argument) -> I64 {
			ThreadArgs* threadArgs = (ThreadArgs*)argument;

			FunctionType invokeSig({ValueType::i32}, {ValueType::i32});

			Timing::Timer timer;
			UntaggedValue args[1]{I32(numIntrinsicCallsPerThread)};
			UntaggedValue results[1];
			invokeFunction(threadArgs->context, threadArgs->function, invokeSig, args, results);
			timer.stop();

			threadArgs->elapsedNanoseconds
				= timer.getNanoseconds() / F64(numIntrinsicCallsPerThread);

			return 0;
		});

	// Free the compartment.
	WAVM_ERROR_UNLESS(tryCollectCompartment(std::move(compartment)));
}

int execBenchmark(int argc, char** argv)
{
	if(argc != 0)
	{
		showBenchmarkHelp(Log::Category::error);
		return EXIT_FAILURE;
	}

	const auto targetSpec = LLVMJIT::getHostTargetSpec();
	Log::printf(Log::output,
				"Host triple: %s\nHost CPU: %s\n",
				targetSpec.triple.c_str(),
				targetSpec.cpu.c_str());

	runInvokeBench();
	runIntrinsicBench();

	return 0;
}
