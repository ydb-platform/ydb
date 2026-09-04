#include <stdlib.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Module.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/File.h"
#include "WAVM/VFS/VFS.h"
#include "WAVM/WASM/WASM.h"
#include "WAVM/WASTParse/TestScript.h"
#include "WAVM/WASTParse/WASTParse.h"
#include "WAVM/WASTPrint/WASTPrint.h"
#include "wavm-test.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::WAST;

enum class DumpFormat
{
	wast,
	wasm,
	both
};

static void dumpWAST(const std::string& wastString, const char* outputDir)
{
	const Uptr wastHash = Hash<std::string>()(wastString);
	const std::string outputPath
		= std::string(outputDir) + "/" + std::to_string(wastHash) + ".wast";
	WAVM_ERROR_UNLESS(saveFile(outputPath.c_str(), wastString.c_str(), wastString.size()));
}

static void dumpWASM(const U8* wasmBytes, Uptr numBytes, const char* outputDir)
{
	const Uptr wasmHash = XXH<Uptr>(wasmBytes, numBytes, 0);
	const std::string outputPath
		= std::string(outputDir) + "/" + std::to_string(wasmHash) + ".wasm";
	WAVM_ERROR_UNLESS(saveFile(outputPath.c_str(), wasmBytes, numBytes));
}

static void dumpModule(const Module& module, const char* outputDir, DumpFormat dumpFormat)
{
	if(dumpFormat == DumpFormat::wast || dumpFormat == DumpFormat::both)
	{
		const std::string wastString = WAST::print(module);
		dumpWAST(wastString, outputDir);
	}

	if(dumpFormat == DumpFormat::wasm || dumpFormat == DumpFormat::both)
	{
		std::vector<U8> wasmBytes = WASM::saveBinaryModule(module);
		dumpWASM(wasmBytes.data(), wasmBytes.size(), outputDir);
	}
}

static void dumpCommandModules(const char* filename,
							   const Command* command,
							   const char* outputDir,
							   DumpFormat dumpFormat)
{
	switch(command->type)
	{
	case Command::action: {
		auto actionCommand = (ActionCommand*)command;
		switch(actionCommand->action->type)
		{
		case ActionType::_module: {
			auto moduleAction = (ModuleAction*)actionCommand->action.get();
			Log::printf(Log::output,
						"Dumping module at %s:%s...\n",
						filename,
						moduleAction->locus.describe().c_str());
			dumpModule(*moduleAction->module, outputDir, dumpFormat);
			break;
		}
		case ActionType::invoke:
		case ActionType::get:
		default: break;
		}
		break;
	}
	case Command::assert_unlinkable: {
		auto assertUnlinkableCommand = (AssertUnlinkableCommand*)command;
		Log::printf(Log::output,
					"Dumping unlinkable module at %s:%s...\n",
					filename,
					assertUnlinkableCommand->locus.describe().c_str());
		dumpModule(*assertUnlinkableCommand->moduleAction->module, outputDir, dumpFormat);
		break;
	}
	case Command::assert_invalid:
	case Command::assert_malformed: {
		auto assertInvalidOrMalformedCommand = (AssertInvalidOrMalformedCommand*)command;
		Log::printf(Log::output,
					"Dumping malformed or invalid module at %s:%s...\n",
					filename,
					assertInvalidOrMalformedCommand->locus.describe().c_str());

		if(assertInvalidOrMalformedCommand->quotedModuleType == QuotedModuleType::text
		   && (dumpFormat == DumpFormat::wast || dumpFormat == DumpFormat::both))
		{ dumpWAST(assertInvalidOrMalformedCommand->quotedModuleString, outputDir); }
		else if(assertInvalidOrMalformedCommand->quotedModuleType == QuotedModuleType::binary
				&& (dumpFormat == DumpFormat::wasm || dumpFormat == DumpFormat::both))
		{
			dumpWASM((const U8*)assertInvalidOrMalformedCommand->quotedModuleString.data(),
					 assertInvalidOrMalformedCommand->quotedModuleString.size(),
					 outputDir);
		}

		break;
	}
	case Command::thread: {
		auto threadCommand = (ThreadCommand*)command;
		for(auto& innerCommand : threadCommand->commands)
		{ dumpCommandModules(filename, innerCommand.get(), outputDir, dumpFormat); }
		break;
	}

	case Command::_register:
	case Command::assert_return:
	case Command::assert_return_arithmetic_nan:
	case Command::assert_return_canonical_nan:
	case Command::assert_return_arithmetic_nan_f32x4:
	case Command::assert_return_canonical_nan_f32x4:
	case Command::assert_return_arithmetic_nan_f64x2:
	case Command::assert_return_canonical_nan_f64x2:
	case Command::assert_return_func:
	case Command::assert_trap:
	case Command::assert_throws:
	case Command::benchmark:
	case Command::wait:
	default: break;
	};
}

int execDumpTestModules(int argc, char** argv)
{
	const char* filename = nullptr;
	const char* outputDir = ".";
	DumpFormat dumpFormat = DumpFormat::both;
	bool showHelpAndExit = false;

	for(Iptr argumentIndex = 0; argumentIndex < argc; ++argumentIndex)
	{
		if(!strcmp(argv[argumentIndex], "--output-dir"))
		{
			++argumentIndex;
			if(argumentIndex < argc) { outputDir = argv[argumentIndex]; }
			else
			{
				Log::printf(Log::error, "Expected directory after '--output-dir'\n");
				showHelpAndExit = true;
				break;
			}
		}
		else if(!strcmp(argv[argumentIndex], "--wast"))
		{
			dumpFormat = dumpFormat == DumpFormat::wasm ? DumpFormat::both : DumpFormat::wast;
		}
		else if(!strcmp(argv[argumentIndex], "--wasm"))
		{
			dumpFormat = dumpFormat == DumpFormat::wast ? DumpFormat::both : DumpFormat::wasm;
		}
		else if(!filename)
		{
			filename = argv[argumentIndex];
		}
		else
		{
			Log::printf(Log::error, "Unrecognized argument: %s\n", argv[argumentIndex]);
			showHelpAndExit = true;
			break;
		}
	}

	if(!filename) { showHelpAndExit = true; }

	if(showHelpAndExit)
	{
		Log::printf(
			Log::error,
			"Usage: wavm test dumpmodules [--output-dir <dir>] [--wast] [--wasm] <input .wast>\n");
		return EXIT_FAILURE;
	}

	WAVM_ASSERT(filename);
	Log::printf(Log::output, "Dumping test modules from '%s'...\n", filename);

	// Read the file into a vector.
	std::vector<U8> testScriptBytes;
	if(!loadFile(filename, testScriptBytes)) { return EXIT_FAILURE; }

	// Make sure the file is null terminated.
	testScriptBytes.push_back(0);

	// Process the test script.
	std::vector<std::unique_ptr<Command>> testCommands;
	std::vector<WAST::Error> testErrors;

	// Parse the test script.
	IR::FeatureSpec featureSpec(IR::FeatureLevel::wavm);
	WAST::parseTestCommands((const char*)testScriptBytes.data(),
							testScriptBytes.size(),
							featureSpec,
							testCommands,
							testErrors);
	if(!testErrors.size())
	{
		for(auto& command : testCommands)
		{ dumpCommandModules(filename, command.get(), outputDir, dumpFormat); }
		return EXIT_SUCCESS;
	}
	else
	{
		reportParseErrors(filename, (const char*)testScriptBytes.data(), testErrors);
		return EXIT_FAILURE;
	}
}
