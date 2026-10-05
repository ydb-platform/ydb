#include "wavm-test.h"
#include <string.h>
#include "WAVM/Inline/CLI.h"
#include "WAVM/Logging/Logging.h"

using namespace WAVM;

enum class TestCommand
{
	invalid,

	dumpModules,
	hashMap,
	hashSet,
	i128,

#if WAVM_ENABLE_RUNTIME
	cAPI,
	benchmark,
	script,
#endif
};

static const char* getTestCommandListHelpText()
{
	return "TestCommands:\n"
#if WAVM_ENABLE_RUNTIME
		   "  c-api         Test the C API\n"
#endif
		   "  dumpmodules   Dump WAST/WASM modules from WAST test scripts\n"
		   "  hashmap       Test HashMap\n"
		   "  hashset       Test HashSet\n"
		   "  i128          Test I128\n"
#if WAVM_ENABLE_RUNTIME
		   "  benchmark     Benchmark WAVM\n"
		   "  script        Run WAST test scripts\n"
#endif
		;
}

void showTestHelp(Log::Category outputCategory)
{
	Log::printf(outputCategory,
				"Usage: wavm test <command> [command arguments]\n"
				"\n"
				"%s",
				getTestCommandListHelpText());
}

static TestCommand parseTestCommand(const char* string)
{
	if(!strcmp(string, "dumpmodules")) { return TestCommand::dumpModules; }
	else if(!strcmp(string, "hashmap"))
	{
		return TestCommand::hashMap;
	}
	else if(!strcmp(string, "hashset"))
	{
		return TestCommand::hashSet;
	}
	else if(!strcmp(string, "i128"))
	{
		return TestCommand::i128;
	}
#if WAVM_ENABLE_RUNTIME
	else if(!strcmp(string, "c-api"))
	{
		return TestCommand::cAPI;
	}
	else if(!strcmp(string, "benchmark"))
	{
		return TestCommand::benchmark;
	}
	else if(!strcmp(string, "script"))
	{
		return TestCommand::script;
	}
#endif
	else
	{
		return TestCommand::invalid;
	}
}

int execTestCommand(int argc, char** argv)
{
	if(argc < 1)
	{
		showTestHelp(Log::Category::error);
		return EXIT_FAILURE;
	}
	else
	{
		const TestCommand command = parseTestCommand(argv[0]);
		switch(command)
		{
		case TestCommand::dumpModules: return execDumpTestModules(argc - 1, argv + 1);
		case TestCommand::hashMap: return execHashMapTest(argc - 1, argv + 1);
		case TestCommand::hashSet: return execHashSetTest(argc - 1, argv + 1);
		case TestCommand::i128: return execI128Test(argc - 1, argv + 1);
#if WAVM_ENABLE_RUNTIME
		case TestCommand::cAPI: return execCAPITest(argc - 1, argv + 1);
		case TestCommand::benchmark: return execBenchmark(argc - 1, argv + 1);
		case TestCommand::script: return execRunTestScript(argc - 1, argv + 1);
#endif

		case TestCommand::invalid:
			Log::printf(Log::error,
						"Invalid command: %s\n"
						"\n"
						"%s",
						argv[0],
						getTestCommandListHelpText());
			return EXIT_FAILURE;

		default: WAVM_UNREACHABLE();
		};
	}
}
