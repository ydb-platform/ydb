#include "wavm.h"
#include <stdlib.h>
#include <string.h>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Config.h"
#include "WAVM/Inline/Version.h"
#include "WAVM/Logging/Logging.h"

using namespace WAVM;

enum class Command
{
	invalid,

	assemble,
	disassemble,
	help,
	test,
	version,

#if WAVM_ENABLE_RUNTIME
	compile,
	run,
#endif
};

static Command parseCommand(const char* string)
{
	if(!strcmp(string, "assemble")) { return Command::assemble; }
	else if(!strcmp(string, "disassemble"))
	{
		return Command::disassemble;
	}
	else if(!strcmp(string, "help"))
	{
		return Command::help;
	}
	else if(!strcmp(string, "test"))
	{
		return Command::test;
	}
	else if(!strcmp(string, "version"))
	{
		return Command::version;
	}
#if WAVM_ENABLE_RUNTIME
	else if(!strcmp(string, "compile"))
	{
		return Command::compile;
	}
	else if(!strcmp(string, "run"))
	{
		return Command::run;
	}
#endif
	else
	{
		return Command::invalid;
	}
}

static const char* getCommandListHelpText()
{
	return "Commands:\n"
		   "  assemble     Assemble WAST/WAT to WASM\n"
		   "  disassemble  Disassemble WASM to WAST/WAT\n"
#if WAVM_ENABLE_RUNTIME
		   "  compile      Compile a WebAssembly module\n"
#endif
		   "  help         Display help about command-line usage of WAVM\n"
#if WAVM_ENABLE_RUNTIME
		   "  run          Run a WebAssembly program\n"
#endif
		   "  test         Groups subcommands used to test WAVM\n"
		   "  version      Display information about the WAVM version\n";
}

std::string getFeatureListHelpText()
{
	char buffer[2048];
	char* bufferNext = buffer;

	auto formatFeature
		= [&buffer, &bufferNext](const char* name, const char* desc, bool isNonStandard) {
			  const int result = snprintf(bufferNext,
										  buffer + sizeof(buffer) - bufferNext,
										  "  %-23s %s %s\n",
										  name,
										  isNonStandard ? "*" : " ",
										  desc);
			  WAVM_ERROR_UNLESS(result >= 0);
			  bufferNext += result;
			  WAVM_ERROR_UNLESS(bufferNext < buffer + sizeof(buffer));
		  };

#define VISIT_STANDARD_FEATURE(_, name, desc) formatFeature(name, desc, false);
#define VISIT_NONSTANDARD_FEATURE(_, name, desc) formatFeature(name, desc, true);
	WAVM_ENUM_STANDARD_FEATURES(VISIT_STANDARD_FEATURE)
	WAVM_ENUM_MATURE_FEATURES(VISIT_STANDARD_FEATURE)
	WAVM_ENUM_PROPOSED_FEATURES(VISIT_STANDARD_FEATURE)
	formatFeature("all-proposed", "All features proposed for standardization", false);
	formatFeature("", "", false);
	WAVM_ENUM_NONSTANDARD_FEATURES(VISIT_NONSTANDARD_FEATURE)
	formatFeature("", "", false);
	formatFeature("all", "All features supported by WAVM", true);
#undef VISIT_STANDARD_FEATURE
#undef VISIT_NONSTANDARD_FEATURE

	formatFeature("", "", false);
	formatFeature("", "Indicates a non-standard feature", true);

	return std::string(buffer);
}

bool parseAndSetFeature(const char* featureName, IR::FeatureSpec& featureSpec, bool enable)
{
	if(!strcmp(featureName, "all-proposed"))
	{
#define VISIT_FEATURE(cName, ...) featureSpec.cName = true;
		WAVM_ENUM_STANDARD_FEATURES(VISIT_FEATURE)
		WAVM_ENUM_MATURE_FEATURES(VISIT_FEATURE)
		WAVM_ENUM_PROPOSED_FEATURES(VISIT_FEATURE)
#undef VISIT_FEATURE
		return true;
	}

	if(!strcmp(featureName, "all"))
	{
#define VISIT_FEATURE(cName, ...) featureSpec.cName = true;
		WAVM_ENUM_STANDARD_FEATURES(VISIT_FEATURE)
		WAVM_ENUM_MATURE_FEATURES(VISIT_FEATURE)
		WAVM_ENUM_PROPOSED_FEATURES(VISIT_FEATURE)
		WAVM_ENUM_NONSTANDARD_FEATURES(VISIT_FEATURE)
#undef VISIT_FEATURE
		return true;
	}

#define VISIT_FEATURE(cName, cliName, ...)                                                         \
	if(!strcmp(featureName, cliName))                                                              \
	{                                                                                              \
		featureSpec.cName = enable;                                                                \
		return true;                                                                               \
	}
	WAVM_ENUM_PROPOSED_FEATURES(VISIT_FEATURE)
	WAVM_ENUM_NONSTANDARD_FEATURES(VISIT_FEATURE)
	WAVM_ENUM_MATURE_FEATURES(VISIT_FEATURE)
	WAVM_ENUM_STANDARD_FEATURES(VISIT_FEATURE)
#undef VISIT_FEATURE

	return false;
}

static void showTopLevelHelp(Log::Category outputCategory)
{
	Log::printf(outputCategory,
				"Usage: wavm <command> [command arguments]\n"
				"\n"
				"%s"
				"\n"
				"%s\n",
				getCommandListHelpText(),
				getEnvironmentVariableHelpText());
}

static void showHelpHelp(Log::Category outputCategory)
{
	Log::printf(outputCategory,
				"Usage: wavm help <command>\n"
				"\n"
				"%s",
				getCommandListHelpText());
}

static int execHelpCommand(int argc, char** argv)
{
	if(argc == 1)
	{
		const Command helpCommand = parseCommand(argv[0]);
		switch(helpCommand)
		{
		case Command::assemble: showAssembleHelp(Log::output); return EXIT_SUCCESS;
		case Command::disassemble: showDisassembleHelp(Log::output); return EXIT_SUCCESS;
		case Command::help: showHelpHelp(Log::output); return EXIT_SUCCESS;
		case Command::test: showTestHelp(Log::output); return EXIT_SUCCESS;
		case Command::version: showVersionHelp(Log::output); return EXIT_SUCCESS;
#if WAVM_ENABLE_RUNTIME
		case Command::compile: showCompileHelp(Log::output); return EXIT_SUCCESS;
		case Command::run: showRunHelp(Log::output); return EXIT_SUCCESS;
#endif
		case Command::invalid:
			Log::printf(Log::error,
						"Invalid command: %s\n"
						"\n"
						"%s",
						argv[0],
						getCommandListHelpText());
			return EXIT_FAILURE;

		default: WAVM_UNREACHABLE();
		};
	}
	else
	{
		showHelpHelp(Log::error);
		return EXIT_FAILURE;
	}
}

void showVersionHelp(Log::Category outputCategory)
{
	Log::printf(outputCategory, "Usage: wavm version\n");
}

#define LOG_BUILD_CONFIG_BOOL(variable)                                                            \
	Log::printf(Log::output, "%-30s %s\n", #variable ":", variable ? "true" : "false");

int execVersionCommand(int argc, char** argv)
{
	if(argc != 0)
	{
		showVersionHelp(Log::error);
		return EXIT_FAILURE;
	}
	Log::printf(Log::output, "WAVM version %s\n", WAVM_VERSION_STRING);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_RUNTIME);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_STATIC_LINKING);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_ASAN);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_UBSAN);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_TSAN);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_LIBFUZZER);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_RELEASE_ASSERTS);
	LOG_BUILD_CONFIG_BOOL(WAVM_ENABLE_UNWIND);
	return false;
}

int main(int argc, char** argv)
{
	if(!initLogFromEnvironment()) { return EXIT_FAILURE; }

	if(argc < 2)
	{
		showTopLevelHelp(Log::Category::error);
		return EXIT_FAILURE;
	}
	else
	{
		const Command command = parseCommand(argv[1]);
		switch(command)
		{
		case Command::assemble: return execAssembleCommand(argc - 2, argv + 2);
		case Command::disassemble: return execDisassembleCommand(argc - 2, argv + 2);
		case Command::help: return execHelpCommand(argc - 2, argv + 2);
		case Command::test: return execTestCommand(argc - 2, argv + 2);
		case Command::version: return execVersionCommand(argc - 2, argv + 2);
#if WAVM_ENABLE_RUNTIME
		case Command::compile: return execCompileCommand(argc - 2, argv + 2);
		case Command::run: return execRunCommand(argc - 2, argv + 2);
#endif

		case Command::invalid:
			Log::printf(Log::error,
						"Invalid command: %s\n"
						"\n"
						"%s",
						argv[1],
						getCommandListHelpText());
			return EXIT_FAILURE;

		default: WAVM_UNREACHABLE();
		};
	}
}
