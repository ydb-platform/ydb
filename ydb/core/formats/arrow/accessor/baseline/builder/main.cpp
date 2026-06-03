/*
 * Reads NDJSON from --input and writes a serialized JsonDocument column to --output.
 *
 * Two modes:
 *   default        -- plain binary-JSON column (NPlain accessor format)
 *   --sub-columns  -- sub_columns accessor format (TSubColumnsArray)
 *
 * Usage:
 *   baseline_builder --input data.ndjson --output col.bin [--sub-columns]
 *                    [--columns-limit N] [--sparsed-kff N] [--others-fraction F]
 */

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/stream/input.h>
#include <util/string/strip.h>

#include <iostream>

using namespace NKikimr::NArrow::NAccessor;
using namespace NKikimr::NArrow;
using namespace NKikimr::NBinaryJson;

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetFreeArgsNum(0);

    TString inputPath;
    TString outputPath;
    bool subColumns = false;
    ui32 columnsLimit = 1024;
    ui32 sparsedKff = 20;
    double othersFraction = 0.05;
    int zstdLevel = -1;  // -1 means no zstd (use default LZ4 serializer)

    opts.AddLongOption('i', "input", "Input NDJSON file (one JSON object per line)")
        .RequiredArgument("FILE")
        .StoreResult(&inputPath);
    opts.AddLongOption('o', "output", "Output binary file")
        .RequiredArgument("FILE")
        .StoreResult(&outputPath);
    opts.AddLongOption("sub-columns", "Use sub_columns storage format")
        .NoArgument()
        .SetFlag(&subColumns);
    opts.AddLongOption("columns-limit", "Max number of separate sub-columns (default 1024)")
        .RequiredArgument("N")
        .StoreResult(&columnsLimit);
    opts.AddLongOption("sparsed-kff", "Sparsed detector coefficient (default 20)")
        .RequiredArgument("N")
        .StoreResult(&sparsedKff);
    opts.AddLongOption("others-fraction", "Allowed fraction for 'others' bucket (default 0.05)")
        .RequiredArgument("F")
        .StoreResult(&othersFraction);
    opts.AddLongOption("zstd-level", "Enable zstd compression at given level (1..22); omit for default LZ4")
        .RequiredArgument("N")
        .StoreResult(&zstdLevel);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    if (inputPath.empty() || outputPath.empty()) {
        Cerr << "Error: --input and --output are required\n";
        opts.PrintUsage(argv[0]);
        return 1;
    }

    // Read all lines from input
    std::vector<TString> lines;
    {
        TFileInput fin(inputPath);
        TString line;
        while (fin.ReadLine(line)) {
            line = StripString(line);
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }

    const ui32 recordsCount = lines.size();

    // Build a plain binary-JSON Arrow array
    TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder(recordsCount, 0);
    for (ui32 i = 0; i < recordsCount; ++i) {
        const TString& json = lines[i];
        auto res = SerializeToBinaryJson(json);
        if (const TString* err = std::get_if<TString>(&res)) {
            Cerr << "Error serializing row " << i << " to binary JSON: " << *err << "\n";
            Cerr << "  JSON: " << json << "\n";
            return 1;
        }
        const auto& bJson = std::get<TBinaryJson>(res);
        arrBuilder.AddRecord(i, std::string_view(bJson.data(), bJson.size()));
    }
    auto plainArray = arrBuilder.Finish(recordsCount);

    std::shared_ptr<NSerialization::ISerializer> serializer;
    if (zstdLevel >= 0) {
        arrow::ipc::IpcWriteOptions options;
        options.use_threads = false;
        auto codecResult = arrow::util::Codec::Create(arrow::Compression::ZSTD, zstdLevel);
        AFL_VERIFY(codecResult.ok())("error", codecResult.status().ToString());
        options.codec = std::move(*codecResult);
        serializer = std::make_shared<NSerialization::TNativeSerializer>(options);
    } else {
        serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
    }
    TChunkConstructionData chunkData(recordsCount, nullptr, arrow::binary(), serializer);

    TString serialized;
    if (!subColumns) {
        NPlain::TConstructor constructor;
        serialized = constructor.SerializeToString(plainArray, chunkData);
    } else {
        NSubColumns::TSettings settings;
        settings.SetColumnsLimit(columnsLimit);
        settings.SetSparsedDetectorKff(sparsedKff);
        settings.SetOthersAllowedFraction(othersFraction);
        settings.SetDataExtractor(NSubColumns::TDataAdapterContainer::GetDefault());

        auto subColsResult = TSubColumnsArray::Make(plainArray, settings, arrow::binary());
        if (subColsResult.IsFail()) {
            std::cerr << "Error building sub_columns array: " << subColsResult.GetErrorMessage() << "\n";
            return 1;
        }
        serialized = subColsResult.DetachResult()->SerializeToString(chunkData);
    }

    TFileOutput fout(outputPath);
    fout.Write(serialized.data(), serialized.size());

    TString compressionInfo = zstdLevel >= 0 ? TStringBuilder() << "zstd:" << zstdLevel : TStringBuilder() << "lz4";
    Cerr << "Written " << recordsCount << " records, " << serialized.size() << " bytes"
         << " [" << compressionInfo << "] -> " << outputPath << "\n";
    return 0;
}
