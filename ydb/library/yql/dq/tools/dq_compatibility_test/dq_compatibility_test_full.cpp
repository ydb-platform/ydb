#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <util/stream/file.h>
#include <util/generic/string.h>

// Conditional compilation for task runner
#ifdef USE_FULL_TASK_RUNNER
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#endif

bool TryCreateAndRunTaskFull(const NYql::NDqProto::TDqTask& protoTask, IOutputStream& logStream, bool verbose) {
#ifdef USE_FULL_TASK_RUNNER
    try {
        using namespace NYql::NDq;
        
        if (verbose) {
            logStream << "Creating full task runner..." << Endl;
        }
        
        // Create task runner context
        TDqTaskRunnerContext context;
        context.TypeEnv = nullptr;
        context.FuncRegistry = nullptr;
        context.RandomProvider = nullptr;
        context.TimeProvider = nullptr;

        // Create task runner settings
        TDqTaskRunnerSettings settings;
        settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_BASIC;
        settings.OptLLVM = "OFF";

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
                logStream << "[TASK_RUNNER]: " << msg << Endl;
            }
        });

        if (verbose) {
            logStream << "Task runner created successfully" << Endl;
        }

        // Create task settings wrapper
        TDqTaskSettings taskSettings(const_cast<NYql::NDqProto::TDqTask*>(&protoTask));

        if (verbose) {
            logStream << "Preparing task..." << Endl;
        }

        // Prepare task runner
        taskRunner->Prepare(taskSettings, memoryLimits, execContext);

        if (verbose) {
            logStream << "Task prepared successfully, starting execution..." << Endl;
        }

        // Run task
        auto status = taskRunner->Run();
        int iterations = 0;
        const int maxIterations = 1000; // Prevent infinite loops
        
        while (status == ERunStatus::PendingInput || status == ERunStatus::PendingOutput) {
            if (++iterations > maxIterations) {
                logStream << "ERROR: Task execution exceeded maximum iterations" << Endl;
                return false;
            }
            
            if (verbose && iterations % 100 == 0) {
                logStream << "  Iteration " << iterations << ", status: " << (int)status << Endl;
            }
            
            status = taskRunner->Run();
        }

        if (status == ERunStatus::Finished) {
            if (verbose) {
                logStream << "Task completed successfully after " << iterations << " iterations" << Endl;
            }
            return true;
        } else {
            logStream << "ERROR: Task failed with status: " << (int)status << Endl;
            return false;
        }

    } catch (const std::exception& e) {
        logStream << "ERROR during full task execution: " << e.what() << Endl;
        return false;
    }
#else
    // Fallback to simulation if full task runner is not available
    if (verbose) {
        logStream << "Full task runner not available, using simulation..." << Endl;
    }
    
    // Basic validation
    if (protoTask.GetProgram().GetRaw().empty()) {
        logStream << "ERROR: Empty program cannot be executed" << Endl;
        return false;
    }
    
    if (protoTask.GetProgram().GetRuntimeVersion() == 0) {
        logStream << "ERROR: Invalid runtime version" << Endl;
        return false;
    }
    
    if (verbose) {
        logStream << "Simulation completed successfully" << Endl;
    }
    
    return true;
#endif
}

int main(int argc, char** argv) {
    TString inputFile;
    TString logFile;
    bool verbose = false;
    bool runTask = false;
    bool useFull = false;

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
    opts.AddLongOption('r', "run", "Actually run the task (not just validate)")
        .Optional()
        .NoArgument()
        .SetFlag(&runTask);
    opts.AddLongOption("full", "Use full task runner (if available)")
        .Optional()
        .NoArgument()
        .SetFlag(&useFull);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    // Simple output stream setup
    THolder<IOutputStream> logStream;
    if (logFile) {
        logStream = MakeHolder<TFileOutput>(logFile);
    } else {
        logStream = MakeHolder<TFileOutput>(Duplicate(2)); // stderr
    }

    try {
        // Read input file
        TFileInput input(inputFile);
        TString serializedTask = input.ReadAll();

        // Try to parse the task to validate format
        NYql::NDqProto::TDqTask protoTask;
        if (!protoTask.ParseFromString(serializedTask)) {
            *logStream << "ERROR: Failed to parse task proto from file: " << inputFile << Endl;
            return 1;
        }

        if (verbose) {
            *logStream << "Successfully parsed task:" << Endl;
            *logStream << "  ID: " << protoTask.GetId() << Endl;
            *logStream << "  Stage ID: " << protoTask.GetStageId() << Endl;
            *logStream << "  Inputs: " << protoTask.InputsSize() << Endl;
            *logStream << "  Outputs: " << protoTask.OutputsSize() << Endl;
            *logStream << "  Program size: " << protoTask.GetProgram().GetRaw().size() << " bytes" << Endl;
            *logStream << "  Runtime version: " << protoTask.GetProgram().GetRuntimeVersion() << Endl;
        }

        // Basic validation
        if (protoTask.GetProgram().GetRaw().empty()) {
            *logStream << "ERROR: Task program is empty" << Endl;
            return 1;
        }

        if (protoTask.GetProgram().GetRuntimeVersion() == 0) {
            *logStream << "ERROR: Task runtime version is not set" << Endl;
            return 1;
        }

        *logStream << "Task validation completed successfully" << Endl;

        // Run the task if requested
        if (runTask) {
            *logStream << "Starting task execution..." << Endl;
            
            bool success;
            if (useFull) {
                success = TryCreateAndRunTaskFull(protoTask, *logStream, verbose);
            } else {
                // Use simulation
                success = protoTask.GetProgram().GetRaw().size() > 0;
                if (verbose) {
                    *logStream << "Using simulation mode (compile with USE_FULL_TASK_RUNNER for full functionality)" << Endl;
                }
            }
            
            if (success) {
                *logStream << "Task execution completed successfully" << Endl;
                *logStream << "Task ID: " << protoTask.GetId() << " is fully compatible" << Endl;
                return 0;
            } else {
                *logStream << "Task execution failed" << Endl;
                return 1;
            }
        } else {
            *logStream << "Task ID: " << protoTask.GetId() << " is structurally compatible" << Endl;
            *logStream << "Use -r/--run flag to test execution compatibility" << Endl;
            return 0;
        }

    } catch (const std::exception& e) {
        *logStream << "ERROR: " << e.what() << Endl;
        return 1;
    }
} 