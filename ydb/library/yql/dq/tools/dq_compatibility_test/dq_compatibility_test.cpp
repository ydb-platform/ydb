#include <library/cpp/getopt/last_getopt.h>

#include "dq_wrapper.h"

#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/system/file.h>

using namespace NYql::NDq;

int main(int argc, char** argv) {
    TString inputFile;
    TString logFile;
    bool verbose = false;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('i', "input", "Input file with serialized task")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&inputFile);
    opts.AddLongOption('l', "log", "Log file")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&logFile);
    opts.AddLongOption('v', "verbose", "Verbose output")
        .Optional()
        .NoArgument()
        .SetFlag(&verbose);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    // Simple output stream setup
    THolder<IOutputStream> logStream;
    if (logFile) {
        logStream = MakeHolder<TFileOutput>(logFile);
    } else {
        logStream = MakeHolder<TFileOutput>(Duplicate(2)); // stderr
    }

    // Read input file
    TFileInput input(inputFile);
    TString serializedTask = input.ReadAll();

    // Create task runner context
    TDqTaskRunnerContext context;
    context.TypeEnv = nullptr; // Will be created by task runner
    context.FuncRegistry = nullptr; // Will be created by task runner
    context.RandomProvider = nullptr; // Will be created by task runner
    context.TimeProvider = nullptr; // Will be created by task runner

    // Create task runner settings
    TDqTaskRunnerSettings settings;
    settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_BASIC;
    settings.OptLLVM = "OFF"; // Disable LLVM optimization for compatibility testing

    // Create memory limits
    TDqTaskRunnerMemoryLimits memoryLimits;
    memoryLimits.ChannelBufferSize = 1024 * 1024; // 1MB
    memoryLimits.OutputChunkMaxSize = 1024 * 1024; // 1MB
    memoryLimits.ChunkSizeLimit = 1024 * 1024; // 1MB

    // Create execution context
    TDqTaskRunnerExecutionContextDefault execContext;

    // Create task runner
    auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(TSourceLocation(__FILE__, __LINE__));
    auto taskRunner = MakeDqTaskRunner(alloc, context, settings, [&logStream, verbose](const TString& msg) {
        if (verbose) {
            *logStream << "[DQ_COMPATIBILITY_TEST]: " << msg << Endl;
        }
    });

    try {
        // Parse task settings from serialized data
        NYql::NDqProto::TDqTask protoTask;
        Y_ENSURE(protoTask.ParseFromString(serializedTask), "Failed to parse task proto");
        
        TDqTaskSettings taskSettings(&protoTask);

        if (verbose) {
            *logStream << "Parsed task with ID: " << taskSettings.GetId() << Endl;
        }

        // Prepare task runner
        taskRunner->Prepare(taskSettings, memoryLimits, execContext);

        if (verbose) {
            *logStream << "Task prepared successfully" << Endl;
        }

        // Run task
        auto status = taskRunner->Run();
        while (status == ERunStatus::PendingInput || status == ERunStatus::PendingOutput) {
            status = taskRunner->Run();
        }

        if (status == ERunStatus::Finished) {
            Cerr << "Task completed successfully" << Endl;
            return 0;
        } else {
            Cerr << "Task failed with status: " << (int)status << Endl;
            return 1;
        }
    } catch (const std::exception& e) {
        Cerr << "Error: " << e.what() << Endl;
        return 1;
    }
} 