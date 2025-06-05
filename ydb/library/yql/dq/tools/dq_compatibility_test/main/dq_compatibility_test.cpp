#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/system/file.h>

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
        for (int i = 0; i < protoTask.InputsSize(); ++i) {
            const auto& input = protoTask.GetInputs(i);
            if (verbose) {
                *logStream << "  Input " << i << ": type=" << (int)input.GetTypeCase() << Endl;
            }
            
            if (input.GetTypeCase() == NYql::NDqProto::TTaskInput::TYPE_NOT_SET) {
                Cerr << "FAILED: Input " << i << " has no type set" << Endl;
                return 1;
            }
        }

        for (int i = 0; i < protoTask.OutputsSize(); ++i) {
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
                *logStream << "NOTE: Full task runner execution is not implemented in this minimal version" << Endl;
                *logStream << "This version only validates task structure and protobuf compatibility" << Endl;
            }
        }

        Cout << "SUCCESS: Task is compatible (structural validation passed)" << Endl;
        return 0;

    } catch (const std::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 1;
    }
} 