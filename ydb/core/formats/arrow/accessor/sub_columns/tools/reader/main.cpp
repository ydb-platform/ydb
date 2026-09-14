#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <yql/essentials/types/binary_json/read.h>

#include <library/cpp/getopt/last_getopt.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>

#include <util/stream/file.h>

#include <cstring>

using namespace NKikimr::NArrow::NAccessor;
using namespace NKikimr::NArrow;
using namespace NKikimr::NBinaryJson;

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetFreeArgsNum(0);

    TString inputPath;
    TString outputPath;

    opts.AddLongOption('i', "input", "Input serialized sub-columns blob")
        .RequiredArgument("FILE")
        .StoreResult(&inputPath);
    opts.AddLongOption('o', "output", "Output NDJSON file; write to stdout when omitted")
        .RequiredArgument("FILE")
        .StoreResult(&outputPath);
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    if (inputPath.empty()) {
        Cerr << "Error: --input is required\n";
        opts.PrintUsage(argv[0]);
        return 1;
    }

    const TString serialized = TFileInput(inputPath).ReadAll();
    if (serialized.size() < sizeof(ui32)) {
        Cerr << "Error: serialized sub-columns blob is too short\n";
        return 1;
    }
    ui32 recordsCount = 0;
    std::memcpy(&recordsCount, serialized.data(), sizeof(recordsCount));
    const TString blob(serialized.data() + sizeof(recordsCount), serialized.size() - sizeof(recordsCount));
    auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
    TChunkConstructionData chunkData(recordsCount, nullptr, arrow::binary(), serializer);

    NSubColumns::TSettings settings;
    settings.SetDataExtractor(NSubColumns::TDataAdapterContainer::GetDefault());
    NSubColumns::TConstructor constructor(settings);
    auto arrayConclusion = constructor.DeserializeFromString(blob, chunkData);
    if (arrayConclusion.IsFail()) {
        Cerr << "Error deserializing sub-columns blob: " << arrayConclusion.GetErrorMessage() << "\n";
        return 1;
    }

    std::unique_ptr<IOutputStream> output;
    if (outputPath) {
        output = std::make_unique<TFileOutput>(outputPath);
    } else {
        output = std::make_unique<TFileOutput>(Duplicate(1));
    }

    const auto chunks = arrayConclusion.GetResult()->GetChunkedArray();
    for (const auto& chunk : chunks->chunks()) {
        const auto binary = std::static_pointer_cast<arrow::BinaryArray>(chunk);
        for (i64 index = 0; index < binary->length(); ++index) {
            if (binary->IsNull(index)) {
                *output << "null\n";
            } else {
                const auto value = binary->GetView(index);
                *output << SerializeToJson(TStringBuf(value.data(), value.size())) << "\n";
            }
        }
    }
    return 0;
}
