#include <util/stream/file.h>
#include <util/system/types.h>
#include <util/system/fstat.h>
#include <util/stream/format.h>

#include <library/cpp/getopt/last_getopt.h>

int main(int argc, const char* argv[]) {

    TString inputFileName;
    ui32 keyCount;
    ui32 skipRowCount;
    ui32 printRowCount;

    NLastGetopt::TOpts options = NLastGetopt::TOpts::Default();
    options.AddLongOption('i', "input", "Input data file name")
        .RequiredArgument("FILENAME")
        .DefaultValue("in.dat")
        .StoreResult(&inputFileName);
    options.AddLongOption('k', "keys", "Keys number")
        .RequiredArgument("INT")
        .DefaultValue(1)
        .StoreResult(&keyCount);
    options.AddLongOption('l', "limit", "Rows number to print")
        .RequiredArgument("INT")
        .DefaultValue(10)
        .StoreResult(&printRowCount);
    options.AddLongOption('o', "offset", "Rows number to skip before print")
        .RequiredArgument("INT")
        .DefaultValue(0)
        .StoreResult(&skipRowCount);
    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    ui64 size;

    {
        TFileStat fs(inputFileName);
        size = fs.Size;
    }

    ui64 rowSizeBytes = (keyCount + 1) * 8;
    assert((size % rowSizeBytes) == 0);
    ui64 rowCount = size / rowSizeBytes;
    ui64 sum = 0;

    ui64 n = 16;
    ui64 * buffer = new ui64[(keyCount + 1) * n];

    TFileInput fi(inputFileName);

    while (rowCount) {
        auto d = std::min(n, rowCount);
        fi.Load(buffer, d * rowSizeBytes);
        auto record = buffer;
        for (ui64 i = 0; i < d; i++) {
            sum += record[keyCount];
            if (skipRowCount) {
                skipRowCount--;
            } else if (printRowCount) {
                for (ui32 j = 0; j < keyCount; j++) {
                    Cout << Hex(record[j]) << " ";
                }
                Cout << "COUNT: " << record[keyCount] << Endl;
                printRowCount--;
            }
            record += (keyCount + 1);
        }
        rowCount -= d;
    }
    Cout << "SUM: " << sum << Endl;

    delete[] buffer;

    return 0;
}