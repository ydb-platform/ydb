#include <stdlib.h>
#include <string>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Module.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/WASM/WASM.h"
#include "WAVM/WASTPrint/WASTPrint.h"
#include "wavm.h"

using namespace WAVM;

static bool loadBinaryModuleFromFile(const char* filename,
									 IR::Module& outModule,
									 Log::Category errorCategory = Log::error)
{
	std::vector<U8> wasmBytes;
	if(!loadFile(filename, wasmBytes)) { return false; }
	WASM::LoadError loadError;
	if(WASM::loadBinaryModule(wasmBytes.data(), wasmBytes.size(), outModule, &loadError))
	{ return true; }
	else
	{
		Log::printf(Log::error, "%s\n", loadError.message.c_str());
		return false;
	}
}

void showDisassembleHelp(Log::Category outputCategory)
{
	Log::printf(Log::error,
				"Usage: wavm disassemble [options] in.wasm [out.wast]\n"
				"  --enable <feature>    Enable the specified feature. See the list of supported\n"
				"                        features below.\n"
				"\n"
				"Features:\n"
				"%s"
				"\n",
				getFeatureListHelpText().c_str());
}

int execDisassembleCommand(int argc, char** argv)
{
	const char* inputFilename = nullptr;
	const char* outputFilename = nullptr;

	IR::FeatureSpec featureSpec;
	for(int argIndex = 0; argIndex < argc; ++argIndex)
	{
		if(!strcmp(argv[argIndex], "--enable"))
		{
			++argIndex;
			if(!argv[argIndex])
			{
				Log::printf(Log::error, "Expected feature name following '--enable'.\n");
				return false;
			}

			if(!parseAndSetFeature(argv[argIndex], featureSpec, true))
			{
				Log::printf(Log::error,
							"Unknown feature '%s'. Supported features:\n"
							"%s"
							"\n",
							argv[argIndex],
							getFeatureListHelpText().c_str());
				return false;
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
			showDisassembleHelp(Log::error);
			return EXIT_FAILURE;
		}
	}

	if(!inputFilename)
	{
		showDisassembleHelp(Log::error);
		return EXIT_FAILURE;
	}

	// Load the WASM file.
	IR::Module module(featureSpec);
	if(!loadBinaryModuleFromFile(inputFilename, module)) { return EXIT_FAILURE; }

	// Print the module to WAST.
	Timing::Timer printTimer;
	const std::string wastString = WAST::print(module);
	Timing::logRatePerSecond(
		"Printed WAST", printTimer, F64(wastString.size()) / 1024.0 / 1024.0, "MiB");

	if(!outputFilename) { Log::printf(Log::output, "%s", wastString.c_str()); }
	else
	{
		// Write the serialized data to the output file.
		if(!saveFile(outputFilename, wastString.data(), wastString.size())) { return EXIT_FAILURE; }
	}

	return EXIT_SUCCESS;
}
