#include <ydb/library/plan2svg/plan2svg.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/stream/output.h>

// Offline converter using the same TPlanVisualizer as /viewer/plan2svg
// (ydb-embedded-ui "Open/Download Execution Plan").

int main(int argc, char* argv[]) {
    NLastGetopt::TOpts opts;

    TString inputPath;
    TString outputPath;
    bool simplified = false;

    opts.AddLongOption('i', "input", "Path to plan JSON (\"-\" for stdin)")
        .DefaultValue("-")
        .RequiredArgument("PATH")
        .StoreResult(&inputPath);

    opts.AddLongOption('o', "output", "Path to output SVG (\"-\" for stdout)")
        .DefaultValue("-")
        .RequiredArgument("PATH")
        .StoreResult(&outputPath);

    opts.AddLongOption("simplified", "Use SimplifiedPlan instead of Plan")
        .NoArgument()
        .SetFlag(&simplified);

    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    try {
        TString planJson;
        if (inputPath && inputPath != "-") {
            planJson = TFileInput(inputPath).ReadAll();
        } else {
            planJson = Cin.ReadAll();
        }

        NPlan2Svg::TPlanVisualizer planViz;
        planViz.LoadPlans(planJson, simplified);
        TString svg = planViz.PrintSvg();

        if (outputPath && outputPath != "-") {
            TFileOutput output(outputPath);
            output << svg;
        } else {
            Cout << svg;
        }

        return 0;
    } catch (const std::exception& e) {
        Cerr << "Conversion error: " << e.what() << Endl;
        return 1;
    }
}
