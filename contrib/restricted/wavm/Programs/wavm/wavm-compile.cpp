#include <string>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Module.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Config.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/WASM/WASM.h"
#include "WAVM/WASTParse/WASTParse.h"
#include "wavm.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

bool loadTextOrBinaryModule(const char* filename, IR::Module& outModule)
{
	// Read the specified file into an array.
	std::vector<U8> fileBytes;
	if(!loadFile(filename, fileBytes)) { return false; }

	// If the file starts with the WASM binary magic number, load it as a binary irModule.
	if(fileBytes.size() >= sizeof(WASM::magicNumber)
	   && !memcmp(fileBytes.data(), WASM::magicNumber, sizeof(WASM::magicNumber)))
	{
		WASM::LoadError loadError;
		if(WASM::loadBinaryModule(fileBytes.data(), fileBytes.size(), outModule, &loadError))
		{ return true; }
		else
		{
			Log::printf(Log::error, "%s\n", loadError.message.c_str());
			return false;
		}
	}
	else
	{
		// Make sure the WAST file is null terminated.
		fileBytes.push_back(0);

		// Load it as a text irModule.
		std::vector<WAST::Error> parseErrors;
		if(!WAST::parseModule(
			   (const char*)fileBytes.data(), fileBytes.size(), outModule, parseErrors))
		{
			Log::printf(Log::error, "Error parsing WebAssembly text file:\n");
			WAST::reportParseErrors(filename, (const char*)fileBytes.data(), parseErrors);
			return false;
		}

		return true;
	}
}

static const char* getOutputFormatHelpText()
{
	return "  unoptimized-llvmir          Unoptimized LLVM IR for the input module.\n"
		   "  optimized-llvmir            Optimized LLVM IR for the input module.\n"
		   "  object                      The target platform's native object file format.\n"
		   "  assembly                    The target platform's native assembly format.\n"
		   "  precompiled-wasm (default)  The original WebAssembly module with object code\n"
		   "                              embedded in the wavm.precompiled_object section.\n";
}

void showCompileHelp(Log::Category outputCategory)
{
	LLVMJIT::TargetSpec hostTargetSpec = LLVMJIT::getHostTargetSpec();

	Log::printf(outputCategory,
				"Usage: wavm compile [options] <in.wast|wasm> <output file>\n"
				"  --target-triple <triple>  Set the target triple (default: %s)\n"
				"  --target-cpu <cpu>        Set the target CPU (default: %s)\n"
				"  --enable <feature>        Enable the specified feature. See the list of\n"
				"                            supported features below.\n"
				"  --format=<format>         Specifies the format of the output file. See the\n"
				"                            list of supported output formats below.\n"
				"\n"
				"Output formats:\n"
				"%s"
				"\n"
				"Features:\n"
				"%s"
				"\n",
				hostTargetSpec.triple.c_str(),
				hostTargetSpec.cpu.c_str(),
				getOutputFormatHelpText(),
				getFeatureListHelpText().c_str());
}

template<Uptr numPrefixChars>
static bool stringStartsWith(const char* string, const char (&prefix)[numPrefixChars])
{
	return !strncmp(string, prefix, numPrefixChars - 1);
}

enum class OutputFormat
{
	unspecified,
	precompiledModule,
	unoptimizedLLVMIR,
	optimizedLLVMIR,
	object,
	assembly,
};

int execCompileCommand(int argc, char** argv)
{
	const char* inputFilename = nullptr;
	const char* outputFilename = nullptr;
	bool useHostTargetSpec = true;
	LLVMJIT::TargetSpec targetSpec;
	IR::FeatureSpec featureSpec;
	OutputFormat outputFormat = OutputFormat::unspecified;
	for(int argIndex = 0; argIndex < argc; ++argIndex)
	{
		if(!strcmp(argv[argIndex], "--target-triple"))
		{
			if(argIndex + 1 == argc)
			{
				Log::printf(Log::error, "Expected target triple following '--target-triple'.\n");
				return EXIT_FAILURE;
			}
			++argIndex;
			targetSpec.triple = argv[argIndex];
			useHostTargetSpec = false;
		}
		else if(!strcmp(argv[argIndex], "--target-cpu"))
		{
			if(argIndex + 1 == argc)
			{
				Log::printf(Log::error, "Expected target CPU name following '--target-cpu'.\n");
				return EXIT_FAILURE;
			}
			++argIndex;
			targetSpec.cpu = argv[argIndex];
			useHostTargetSpec = false;
		}
		else if(!strcmp(argv[argIndex], "--enable"))
		{
			++argIndex;
			if(argIndex == argc)
			{
				Log::printf(Log::error, "Expected feature name following '--enable'.\n");
				return EXIT_FAILURE;
			}

			if(!parseAndSetFeature(argv[argIndex], featureSpec, true))
			{
				Log::printf(Log::error, "Unknown feature '%s'.\n", argv[argIndex]);
				return EXIT_FAILURE;
			}
		}
		else if(stringStartsWith(argv[argIndex], "--format="))
		{
			if(outputFormat != OutputFormat::unspecified)
			{
				Log::printf(Log::error, "'--format=' may only occur once on the command line.\n");
				return EXIT_FAILURE;
			}

			const char* formatString = argv[argIndex] + strlen("--format=");
			if(!strcmp(formatString, "precompiled-wasm"))
			{ outputFormat = OutputFormat::precompiledModule; }
			else if(!strcmp(formatString, "unoptimized-llvmir"))
			{
				outputFormat = OutputFormat::unoptimizedLLVMIR;
			}
			else if(!strcmp(formatString, "optimized-llvmir"))
			{
				outputFormat = OutputFormat::optimizedLLVMIR;
			}
			else if(!strcmp(formatString, "object"))
			{
				outputFormat = OutputFormat::object;
			}
			else if(!strcmp(formatString, "assembly"))
			{
				outputFormat = OutputFormat::assembly;
			}
			else
			{
				Log::printf(Log::error,
							"Invalid output format '%s'. Supported output formats:\n"
							"%s"
							"\n",
							formatString,
							getOutputFormatHelpText());
				return EXIT_FAILURE;
			}
		}
		else if(!inputFilename)
		{
			inputFilename = argv[argIndex];
		}
		else if(!outputFilename)
		{
			outputFilename = argv[argIndex];
		}
		else
		{
			Log::printf(Log::error, "Unrecognized argument: %s\n", argv[argIndex]);
			showCompileHelp(Log::error);
			return EXIT_FAILURE;
		}
	}

	if(!inputFilename || !outputFilename)
	{
		showCompileHelp(Log::error);
		return EXIT_FAILURE;
	}

	if(useHostTargetSpec) { targetSpec = LLVMJIT::getHostTargetSpec(); }

	// Validate the target.
	switch(LLVMJIT::validateTarget(targetSpec, featureSpec))
	{
	case LLVMJIT::TargetValidationResult::valid: break;

	case LLVMJIT::TargetValidationResult::invalidTargetSpec:
		Log::printf(Log::error,
					"Target triple (%s) or CPU (%s) is invalid.\n",
					targetSpec.triple.c_str(),
					targetSpec.cpu.c_str());
		return EXIT_FAILURE;
	case LLVMJIT::TargetValidationResult::unsupportedArchitecture:
		Log::printf(Log::error, "WAVM doesn't support the target architecture.\n");
		return EXIT_FAILURE;
	case LLVMJIT::TargetValidationResult::x86CPUDoesNotSupportSSE41:
		Log::printf(Log::error,
					"Target X86 CPU (%s) does not support SSE 4.1, which"
					" WAVM requires for WebAssembly SIMD code.\n",
					targetSpec.cpu.c_str());
		return EXIT_FAILURE;
	case LLVMJIT::TargetValidationResult::wavmDoesNotSupportSIMDOnArch:
		Log::printf(Log::error, "WAVM does not support SIMD on the target CPU architecture.\n");
		return EXIT_FAILURE;
	case LLVMJIT::TargetValidationResult::memory64Requires64bitTarget:
		Log::printf(Log::error,
					"Target CPU (%s) does not support 64-bit memories.\n",
					targetSpec.cpu.c_str());
		return EXIT_FAILURE;
	case LLVMJIT::TargetValidationResult::table64Requires64bitTarget:
		Log::printf(Log::error,
					"Target CPU (%s) does not support 64-bit tables.\n",
					targetSpec.cpu.c_str());
		return EXIT_FAILURE;

	default: WAVM_UNREACHABLE();
	};

	if(outputFormat == OutputFormat::unspecified)
	{ outputFormat = OutputFormat::precompiledModule; }

	// Load the module IR.
	IR::Module irModule(featureSpec);
	if(!loadTextOrBinaryModule(inputFilename, irModule)) { return EXIT_FAILURE; }

	switch(outputFormat)
	{
	case OutputFormat::precompiledModule: {
		// Compile the module to object code.
		std::vector<U8> objectCode = LLVMJIT::compileModule(irModule, targetSpec);

		// Extract the compiled object code and add it to the IR module as a user section.
		irModule.customSections.push_back(CustomSection{
			OrderedSectionID::moduleBeginning, "wavm.precompiled_object", std::move(objectCode)});

		// Serialize the WASM module.
		Timing::Timer saveTimer;
		std::vector<U8> wasmBytes = WASM::saveBinaryModule(irModule);

		Timing::logRatePerSecond(
			"Serialized WASM", saveTimer, wasmBytes.size() / 1024.0 / 1024.0, "MiB");

		// Write the serialized data to the output file.
		return saveFile(outputFilename, wasmBytes.data(), wasmBytes.size()) ? EXIT_SUCCESS
																			: EXIT_FAILURE;
	}
	case OutputFormat::object: {
		// Compile the module to object code.
		std::vector<U8> objectCode = LLVMJIT::compileModule(irModule, targetSpec);

		// Write the object code to the output file.
		return saveFile(outputFilename, objectCode.data(), objectCode.size()) ? EXIT_SUCCESS
																			  : EXIT_FAILURE;
	}
	case OutputFormat::assembly: {
		// Compile the module to object code.
		std::vector<U8> objectCode = LLVMJIT::compileModule(irModule, targetSpec);

		// Disassemble the object code.
		std::string disassembly = LLVMJIT::disassembleObject(targetSpec, objectCode);

		// Write the disassembly to the output file.
		return saveFile(outputFilename, disassembly.data(), disassembly.size()) ? EXIT_SUCCESS
																				: EXIT_FAILURE;
	}
	case OutputFormat::optimizedLLVMIR:
	case OutputFormat::unoptimizedLLVMIR: {
		// Compile the module to LLVM IR.
		std::string llvmIR = LLVMJIT::emitLLVMIR(
			irModule, targetSpec, outputFormat == OutputFormat::optimizedLLVMIR);

		// Write the LLVM IR to the output file.
		return saveFile(outputFilename, llvmIR.data(), llvmIR.size()) ? EXIT_SUCCESS : EXIT_FAILURE;
	}

	case OutputFormat::unspecified:
	default: WAVM_UNREACHABLE();
	};
}
