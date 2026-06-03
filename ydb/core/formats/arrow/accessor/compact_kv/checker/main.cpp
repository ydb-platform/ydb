/*
 * Line-by-line JSON equivalence checker.
 *
 * Reads two NDJSON files and compares them line by line for JSON equivalence
 * (not byte equality — key order and whitespace are ignored).
 * On the first differing pair, prints both lines and exits with code 1.
 * If files are equivalent, exits with code 0.
 *
 * Usage:
 *   checker --file1 A.ndjson --file2 B.ndjson
 */

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/file.h>
#include <util/string/strip.h>

#include <iostream>
#include <string>

// Canonical JSON: sorted keys, no extra whitespace.
static TString Canonicalize(const TString& jsonLine) {
    NJson::TJsonValue val;
    if (!NJson::ReadJsonFastTree(jsonLine, &val)) {
        // Not valid JSON — return as-is for comparison
        return jsonLine;
    }
    // formatOutput=false, sortkeys=true, validateUtf8=false
    return NJson::WriteJson(&val, false, true, false);
}

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("Compare two NDJSON files line by line for JSON equivalence");

    TString file1Path;
    TString file2Path;

    opts.AddLongOption("file1", "First NDJSON file").Required().StoreResult(&file1Path);
    opts.AddLongOption("file2", "Second NDJSON file").Required().StoreResult(&file2Path);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    TFileInput f1(file1Path);
    TFileInput f2(file2Path);

    TString line1, line2;
    uint64_t lineNum = 0;

    while (true) {
        bool has1 = f1.ReadLine(line1);
        bool has2 = f2.ReadLine(line2);

        if (!has1 && !has2) break;

        ++lineNum;

        if (!has1 || !has2) {
            Cerr << "Files have different number of lines. "
                 << "File1 " << (has1 ? "has" : "ended at") << " line " << lineNum << ", "
                 << "File2 " << (has2 ? "has" : "ended at") << " line " << lineNum << "\n";
            return 1;
        }

        line1 = StripString(line1);
        line2 = StripString(line2);

        if (line1.empty() && line2.empty()) continue;

        TString c1 = Canonicalize(line1);
        TString c2 = Canonicalize(line2);

        if (c1 != c2) {
            Cerr << "Difference at line " << lineNum << ":\n";
            Cerr << "  File1: " << line1 << "\n";
            Cerr << "  File2: " << line2 << "\n";
            return 1;
        }
    }

    Cerr << "OK: " << lineNum << " lines match\n";
    return 0;
}
