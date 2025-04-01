#include <util/system/types.h>

#include <library/cpp/getopt/last_getopt.h>

#include "aggr.h"

int main(int argc, const char* argv[]) {

    TString algo;
    ui16 keyCount;
    ui16 keyCardinality;
    ui64 rows;
    TString inputFileName;
    TString outputFileName;
    ui16 hashBits1 = 0;
    ui16 hashBits2 = 0;
    ui16 partBits = 0;
    ui16 fillRatio;
    ui32 partBufferSize1;
    ui32 partBufferSize2;

    NLastGetopt::TOpts options = NLastGetopt::TOpts::Default();
    options.AddLongOption('a', "algo", "Aggregation algorithm")
        .RequiredArgument("IDENT")
        .DefaultValue("mem-lp")
        .StoreResult(&algo);
    options.AddLongOption('i', "input", "Input data file name")
        .RequiredArgument("FILENAME")
        .DefaultValue("in.dat")
        .StoreResult(&inputFileName);
    options.AddLongOption('o', "output", "Output data file name")
        .RequiredArgument("FILENAME")
        .DefaultValue("out.dat")
        .StoreResult(&outputFileName);
    options.AddLongOption('r', "rows", "Rows number")
        .RequiredArgument("INT")
        .DefaultValue(1024 * 1024)
        .StoreResult(&rows);
    options.AddLongOption('k', "keys", "Keys number")
        .RequiredArgument("INT")
        .DefaultValue(1)
        .StoreResult(&keyCount);
    options.AddLongOption('c', "cardinality", "Cardinality of each key")
        .RequiredArgument("INT")
        .DefaultValue(10)
        .StoreResult(&keyCardinality);
    options.AddLongOption("hash-1-bits", "Bits in hash index in intermediate HT")
        .StoreResult(&hashBits1);
    options.AddLongOption("hash-2-bits", "Bits in hash index in final HT")
        .StoreResult(&hashBits2);
    options.AddLongOption('f', "fill-ratio", "Fill ratio in intermediate HT")
        .RequiredArgument("INT")
        .DefaultValue(50)
        .StoreResult(&fillRatio);
    options.AddLongOption("part-bits", "Bits in hash index in final HT")
        .StoreResult(&partBits);
    options.AddLongOption("part-1-buffer-size", "Row count of each part buffer in intermediate HT")
        .RequiredArgument("INT")
        .DefaultValue(32)
        .StoreResult(&partBufferSize1);
    options.AddLongOption("part-2-buffer-size", "Row count of each part buffer in final HT")
        .RequiredArgument("INT")
        .DefaultValue(32)
        .StoreResult(&partBufferSize2);
    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    ui64 cardinality = 1;
    for (ui16 i = 0; i < keyCount; i++) {
        cardinality *= keyCardinality;
    }

    TFileInput fi(inputFileName);
    TFileOutput fo(outputFileName);

    if (algo == "mem-lp") {
        aggr_memory_lp_ht(fi, fo, rows, keyCount, cardinality);
    } else if (algo == "mem-rh") {
        aggr_memory_rh_ht(fi, fo, rows, keyCount, cardinality, hashBits1);
    } else if (algo == "ext-rh") {
        aggr_external_rh_ht(fi, fo, rows, keyCount, cardinality, hashBits1, fillRatio, partBits, partBufferSize1, hashBits2, partBufferSize2);
    } else if (algo == "ext-me") {
        switch (keyCount) {
            case 0:
                break;
            case 1:
                aggr_external_merge<1>(fi, fo, rows, cardinality, hashBits1, fillRatio, partBufferSize2);
                break;
            case 2:
                aggr_external_merge<2>(fi, fo, rows, cardinality, hashBits1, fillRatio, partBufferSize2);
                break;
            case 3:
                aggr_external_merge<3>(fi, fo, rows, cardinality, hashBits1, fillRatio, partBufferSize2);
                break;
            case 4:
                aggr_external_merge<4>(fi, fo, rows, cardinality, hashBits1, fillRatio, partBufferSize2);
                break;
            default:
                Cerr << "Unsupported key count " << keyCount << Endl;
        }
    } else if (algo == "mem-rhi") {
        aggr_memory_rhi_ht(fi, fo, rows, keyCount, cardinality);
    }

    return 0;
}