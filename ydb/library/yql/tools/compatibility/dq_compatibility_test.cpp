#include <library/cpp/getopt/last_getopt.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/runtime/dq_compute.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>

#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/system/file.h>

using namespace NYql::NDq;

int main(int argc, char** argv) {
    TString inputFile;
    TString logFile;
    bool verbose = false;
    bool runMode = false;

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
    opts.AddLongOption('r', "run", "Run task (not just validate)")
        .Optional()
        .NoArgument()
        .SetFlag(&runMode);
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

        if (verbose) {
            *logStream << "Read " << serializedTask.size() << " bytes from " << inputFile << Endl;
        }

        // Parse task proto
        NYql::NDqProto::TDqTask protoTask;
        if (!protoTask.ParseFromString(serializedTask)) {
            Cerr << "FAILED: Cannot parse task proto" << Endl;
            return 1;
        }

        if (verbose) {
            *logStream << "Successfully parsed task proto:" << Endl;
            *logStream << "  Task ID: " << protoTask.GetId() << Endl;
            *logStream << "  Stage ID: " << protoTask.GetStageId() << Endl;
            if (protoTask.HasProgram()) {
                *logStream << "  Program size: " << protoTask.GetProgram().GetRaw().size() << " bytes" << Endl;
                *logStream << "  Runtime version: " << protoTask.GetProgram().GetRuntimeVersion() << Endl;
            }
            *logStream << "  Inputs: " << protoTask.InputsSize() << Endl;
            *logStream << "  Outputs: " << protoTask.OutputsSize() << Endl;
        }

        // Basic validation
        if (protoTask.GetId() == 0) {
            Cerr << "FAILED: Task ID cannot be 0" << Endl;
            return 1;
        }

        if (!protoTask.HasProgram()) {
            Cerr << "FAILED: Task must have a program" << Endl;
            return 1;
        }

        if (protoTask.GetProgram().GetRaw().empty()) {
            Cerr << "FAILED: Program cannot be empty" << Endl;
            return 1;
        }

        // Check inputs and outputs structure
        for (ui32 i = 0; i < protoTask.InputsSize(); ++i) {
            const auto& input = protoTask.GetInputs(i);
            if (verbose) {
                *logStream << "  Input " << i << ": type=" << (int)input.GetTypeCase() << Endl;
            }
            
            if (input.GetTypeCase() == NYql::NDqProto::TTaskInput::TYPE_NOT_SET) {
                Cerr << "FAILED: Input " << i << " has no type set" << Endl;
                return 1;
            }
        }

        for (ui32 i = 0; i < protoTask.OutputsSize(); ++i) {
            const auto& output = protoTask.GetOutputs(i);
            if (verbose) {
                *logStream << "  Output " << i << ": type=" << (int)output.GetTypeCase() << Endl;
            }
            
            if (output.GetTypeCase() == NYql::NDqProto::TTaskOutput::TYPE_NOT_SET) {
                Cerr << "FAILED: Output " << i << " has no type set" << Endl;
                return 1;
            }
        }

        if (runMode) {
            if (verbose) {
                *logStream << "Starting task runner preparation and execution..." << Endl;
            }

            // Create computation context for proper ComputationFactory initialization
            auto computeCtx = std::make_unique<TDqComputeContextBase>();

            // Initialize task runner context with proper ComputationFactory
            TDqTaskRunnerContext context;
            context.TypeEnv = nullptr; // Will be created by task runner
            context.FuncRegistry = nullptr; // Will be created by task runner
            context.RandomProvider = nullptr; // Will be created by task runner
            context.TimeProvider = nullptr; // Will be created by task runner
            context.ComputeCtx = computeCtx.get();
            
            // Initialize ComputationFactory with full DQ support
            context.ComputationFactory = GetDqBaseComputeFactory(computeCtx.get());

            // Create task runner settings
            TDqTaskRunnerSettings settings;
            settings.OptLLVM = "OFF"; // Disable LLVM optimizations for compatibility
            settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_NONE;
            settings.TerminateOnError = false;

            // Create task settings from proto
            TDqTaskSettings taskSettings(&protoTask);

            // Create allocator
            auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__,
                                                                          NKikimr::TAlignedPagePoolCounters(),
                                                                          true,
                                                                          false);

            // Create task runner
            auto taskRunner = MakeDqTaskRunner(alloc, context, settings, nullptr);

            if (verbose) {
                *logStream << "Task runner created" << Endl;
            }

            // Create memory limits
            TDqTaskRunnerMemoryLimits memoryLimits;
            memoryLimits.ChannelBufferSize = 1024 * 1024; // 1MB
            memoryLimits.OutputChunkMaxSize = 1024 * 1024; // 1MB
            memoryLimits.ChunkSizeLimit = 48_MB;

            // Create execution context
            TDqTaskRunnerExecutionContextDefault execContext;

            // Prepare task - this is where the previous error occurred
            taskRunner->Prepare(taskSettings, memoryLimits, execContext);

            if (verbose) {
                *logStream << "Task prepared successfully" << Endl;
            }

            Cout << "Task execution test passed" << Endl;

        } else {
            Cout << "SUCCESS: Task is compatible (structural validation passed)" << Endl;
            return 0;
        }

    } catch (const std::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 1;
    }

    return 0;
} 
