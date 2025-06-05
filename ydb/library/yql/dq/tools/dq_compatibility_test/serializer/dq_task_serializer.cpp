#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <util/stream/file.h>
#include <util/generic/string.h>

int main(int argc, char** argv) {
    TString outputFile;
    TString programData;
    ui64 taskId = 1;
    ui32 stageId = 1;
    bool verbose = false;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('o', "output", "Output file for serialized task")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&outputFile);
    opts.AddLongOption('p', "program", "Program data (or use test data)")
        .Optional()
        .RequiredArgument("DATA")
        .StoreResult(&programData);
    opts.AddLongOption("task-id", "Task ID")
        .Optional()
        .RequiredArgument("ID")
        .StoreResult(&taskId);
    opts.AddLongOption("stage-id", "Stage ID")
        .Optional()
        .RequiredArgument("ID")
        .StoreResult(&stageId);
    opts.AddLongOption('v', "verbose", "Verbose output")
        .Optional()
        .NoArgument()
        .SetFlag(&verbose);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    try {
        // Create a test DqTask
        NYql::NDqProto::TDqTask task;
        
        // Set basic task info
        task.SetId(taskId);
        task.SetStageId(stageId);
        
        // Set program
        auto* program = task.MutableProgram();
        program->SetRuntimeVersion(NYql::NDqProto::RUNTIME_VERSION_YQL_1_0);
        
        if (programData.empty()) {
            // Create test program data
            programData = "test_program_placeholder_data_for_compatibility_testing";
        }
        program->SetRaw(programData);
        
        // Add test input (source)
        auto* input = task.AddInputs();
        auto* source = input->MutableSource();
        source->SetType("TestSource");
        
        // Add test output (sink)
        auto* output = task.AddOutputs();
        auto* sink = output->MutableSink();
        sink->SetType("TestSink");
        
        // Serialize to string
        TString serializedData;
        if (!task.SerializeToString(&serializedData)) {
            Cerr << "ERROR: Failed to serialize task" << Endl;
            return 1;
        }
        
        // Write to file
        TFileOutput output_stream(outputFile);
        output_stream.Write(serializedData);
        output_stream.Finish();
        
        if (verbose) {
            Cout << "Successfully created serialized task:" << Endl;
            Cout << "  Task ID: " << task.GetId() << Endl;
            Cout << "  Stage ID: " << task.GetStageId() << Endl;
            Cout << "  Runtime Version: " << task.GetProgram().GetRuntimeVersion() << Endl;
            Cout << "  Program size: " << task.GetProgram().GetRaw().size() << " bytes" << Endl;
            Cout << "  Inputs: " << task.InputsSize() << Endl;
            Cout << "  Outputs: " << task.OutputsSize() << Endl;
            Cout << "  Serialized size: " << serializedData.size() << " bytes" << Endl;
            Cout << "  Output file: " << outputFile << Endl;
        }
        
        Cout << "Task serialized successfully to: " << outputFile << Endl;
        return 0;
        
    } catch (const std::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 1;
    }
} 