#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <util/stream/file.h>
#include <util/generic/string.h>

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
        *logStream << "Task ID: " << protoTask.GetId() << " is compatible" << Endl;
        return 0;

    } catch (const std::exception& e) {
        *logStream << "ERROR: " << e.what() << Endl;
        return 1;
    }
} 