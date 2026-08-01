#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/request.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/services/metadata/abstract/parsing.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/file.h>
#include <util/stream/input.h>
#include <util/stream/str.h>
#include <util/string/strip.h>

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace NKikimr::NArrow::NAccessor;
using namespace NKikimr::NArrow;
using namespace NKikimr::NBinaryJson;

namespace {

    class TParsedObjectSettings {
    private:
        NYql::NNodes::TCoNameValueTupleList Features_;

    public:
        explicit TParsedObjectSettings(NYql::NNodes::TCoNameValueTupleList features)
            : Features_(std::move(features))
        {
        }

        TString ObjectId() const {
            return {};
        }

        TString TypeId() const {
            return {};
        }

        const NYql::NNodes::TCoNameValueTupleList& Features() const {
            return Features_;
        }
    };

    // Boilerplate to create subcolumns constructor from settings.
    NKikimr::TConclusion<TConstructorContainer> BuildSubColumnsConstructor(const TString& settings) {
        NSubColumns::TRequestedConstuctor requestedConstructor;
        if (settings.empty()) {
            const THashMap<TString, TString> emptyFeatures;
            const std::unordered_set<TString> emptyResetFeatures;
            NYql::TFeaturesExtractor features(emptyFeatures, emptyResetFeatures);
            auto status = requestedConstructor.DeserializeFromRequest(features);
            if (status.IsFail()) {
                return status;
            }
            return requestedConstructor.BuildConstructor();
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(false);
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        NSQLTranslation::TTranslators translators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);

        NSQLTranslation::TTranslationSettings translationSettings;
        translationSettings.DefaultCluster = "local";
        translationSettings.ClusterMapping.emplace("local", "kikimr");
        translationSettings.EndOfQueryCommit = false;
        const TString query = TStringBuilder() << "ALTER OBJECT builder (TYPE SUB_COLUMNS) SET (" << settings << ");";
        auto queryAst = NSQLTranslation::SqlToYql(translators, query, translationSettings);
        if (!queryAst.IsOk()) {
            return NKikimr::TConclusionStatus::Fail(queryAst.Issues.ToString());
        }

        NYql::TExprContext context;
        NYql::TExprNode::TPtr queryGraph;
        if (!NYql::CompileExpr(*queryAst.Root, queryGraph, context, nullptr, nullptr)) {
            return NKikimr::TConclusionStatus::Fail(context.IssueManager.GetIssues().ToString());
        }
        auto write = NYql::FindNode(queryGraph, [](const NYql::TExprNode::TPtr& node) {
            return node->IsCallable("Write!");
        });
        if (!write) {
            return NKikimr::TConclusionStatus::Fail("cannot find ALTER OBJECT settings");
        }

        auto writeSettings = NYql::NCommon::ParseWriteObjectSettings(NYql::NNodes::TExprList(write->Child(4)), context);
        NYql::TObjectSettingsImpl objectSettings;
        objectSettings.DeserializeFromKi(TParsedObjectSettings(std::move(writeSettings.Features)));
        auto& features = objectSettings.GetFeaturesExtractor();
        auto status = requestedConstructor.DeserializeFromRequest(features);
        if (status.IsFail()) {
            return status;
        }
        if (!features.IsFinished()) {
            return NKikimr::TConclusionStatus::Fail(TStringBuilder() << "unknown settings: " << features.GetRemainedParamsString());
        }
        return requestedConstructor.BuildConstructor();
    }

} // namespace

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetFreeArgsNum(0);

    TString inputPath;
    TString outputPath;
    TString settings;
    int zstdLevel = -1;  // -1 means no zstd (use default LZ4 serializer)
    bool jsonStats = false;
    ui32 benchIterations = 0;

    opts.AddLongOption('i', "input", "Input newline-delimited JSON file (one JSON object per line)")
        .RequiredArgument("FILE")
        .StoreResult(&inputPath);
    opts.AddLongOption('o', "output", "Output binary file")
        .RequiredArgument("FILE")
        .StoreResult(&outputPath);
    opts.AddLongOption("settings", "Sub-columns settings in ALTER OBJECT SET format")
        .RequiredArgument("SETTINGS")
        .StoreResult(&settings);
    opts.AddLongOption("zstd-level", "Enable zstd compression at given level (1..22); omit for default LZ4")
        .RequiredArgument("N")
        .StoreResult(&zstdLevel);
    opts.AddLongOption("bench", "Repeat serialization N times and report timing")
        .RequiredArgument("N")
        .StoreResult(&benchIterations);
    opts.AddLongOption("json-stats", "Print compression stats as one JSON line to stdout")
        .NoArgument()
        .SetFlag(&jsonStats);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    if (inputPath.empty() || outputPath.empty()) {
        Cerr << "Error: --input and --output are required\n";
        opts.PrintUsage(argv[0]);
        return 1;
    }

    auto constructorResult = BuildSubColumnsConstructor(settings);
    if (constructorResult.IsFail()) {
        Cerr << "Error parsing --settings: " << constructorResult.GetErrorMessage() << "\n";
        return 1;
    }
    auto constructor = constructorResult.DetachResult();

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

    // Build a binary-JSON Arrow array for the sub-columns accessor.
    TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder(recordsCount, 0);
    ui64 inputSize = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        NJson::TJsonValue doc;
        if (!NJson::ReadJsonFastTree(TStringBuf(lines[i].data(), lines[i].size()), &doc)) {
            Cerr << "JSON parse error in row " << i << ": " << lines[i].substr(0, 80) << "\n";
            return 1;
        }
        if (doc.GetType() != NJson::JSON_MAP) {
            Cerr << "JSON row " << i << " is not a map: " << lines[i].substr(0, 80) << "\n";
            return 1;
        }
        const TString jsonText = NJson::WriteJson(&doc, /*formatOutput=*/false);
        auto res = SerializeToBinaryJson(TStringBuf(jsonText.data(), jsonText.size()));
        if (const TString* err = std::get_if<TString>(&res)) {
            Cerr << "Error serializing row " << i << " to binary JSON: " << *err << "\n";
            continue;  // leave as a null row rather than aborting the whole run
        }
        const auto& bJson = std::get<TBinaryJson>(res);
        arrBuilder.AddRecord(i, std::string_view(bJson.data(), bJson.size()));
        inputSize += lines[i].size();
    }
    auto plainArray = arrBuilder.Finish(recordsCount);

    std::shared_ptr<NSerialization::ISerializer> serializer;
    if (zstdLevel >= 0) {
        if (zstdLevel < 1 || zstdLevel > 22) {
            Cerr << "Error: --zstd-level must be in range [1..22]\n";
            return 1;
        }
        arrow::ipc::IpcWriteOptions options;
        options.use_threads = false;
        auto codecResult = arrow::util::Codec::Create(arrow::Compression::ZSTD, zstdLevel);
        if (!codecResult.ok()) {
            Cerr << "Error creating zstd codec: " << codecResult.status().ToString() << "\n";
            return 1;
        }
        options.codec = std::move(*codecResult);
        serializer = std::make_shared<NSerialization::TNativeSerializer>(options);
    } else {
        serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
    }
    TChunkConstructionData chunkData(recordsCount, nullptr, arrow::binary(), serializer);

    auto subColsResult = constructor->Construct(plainArray, chunkData);
    if (subColsResult.IsFail()) {
        Cerr << "Error building sub_columns array: " << subColsResult.GetErrorMessage() << "\n";
        return 1;
    }
    auto subArr = std::static_pointer_cast<TSubColumnsArray>(subColsResult.DetachResult());
    ui32 numSeparated = subArr->GetColumnsData().GetStats().GetColumnsCount();
    ui32 numOtherKeys = subArr->GetOthersData().GetStats().GetColumnsCount();
    const TString subColumnsBlob = subArr->SerializeToString(chunkData);
    TString serialized;
    TStringOutput serializedOutput(serialized);
    serializedOutput.Write(&recordsCount, sizeof(recordsCount));
    serializedOutput.Write(subColumnsBlob.data(), subColumnsBlob.size());

    if (benchIterations) {
        ui64 sink = 0;
        const TMonotonic start = TMonotonic::Now();
        for (ui32 i = 0; i < benchIterations; ++i) {
            sink += subArr->SerializeToString(chunkData).size();
        }
        const auto elapsed = TMonotonic::Now() - start;
        Cerr << "bench: serialize x" << benchIterations << " total=" << elapsed.MilliSeconds() << "ms avg="
             << (double)elapsed.MicroSeconds() / benchIterations / 1000.0 << "ms bytes=" << (sink / benchIterations) << "\n";
    }

    TFileOutput fout(outputPath);
    fout.Write(serialized.data(), serialized.size());

    TString compressionInfo = zstdLevel >= 0 ? TStringBuilder() << "zstd:" << zstdLevel : TStringBuilder() << "lz4";
    Cerr << "Written " << recordsCount << " records, " << serialized.size() << " bytes"
         << " [" << compressionInfo << "] -> " << outputPath << "\n";

    if (jsonStats) {
        ui64 keyColsBytes = 0, otherColsBytes = 0, statsBytes = 0;
        if (serialized.size() >= 2 * sizeof(ui32)) {
            ui32 protoSize = 0;
            std::memcpy(&protoSize, serialized.data() + sizeof(recordsCount), sizeof(protoSize));
            NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
            if (proto.ParseFromArray(serialized.data() + 2 * sizeof(ui32), protoSize)) {
                statsBytes = 2 * sizeof(ui32) + protoSize + proto.GetColumnStatsSize() + proto.GetOtherStatsSize();
                for (ui32 i = 0; i < proto.KeyColumnsSize(); ++i) keyColsBytes += proto.GetKeyColumns(i).GetSize();
                for (ui32 i = 0; i < proto.OtherColumnsSize(); ++i) otherColsBytes += proto.GetOtherColumns(i).GetSize();
            }
        }
        const ui64 totalOutputSize = serialized.size();
        const double compressionRatio = 1.0 * inputSize / totalOutputSize;
        auto esc = [](const TString& s) {
            std::string r;
            for (char c : s) { if (c == '"' || c == '\\') r += '\\'; r += c; }
            return r;
        };
        std::ostringstream js;
        js << "{\"input\":\"" << esc(inputPath) << "\",\"rows\":" << recordsCount
           << ",\"separated_columns\":" << numSeparated << ",\"other_columns\":" << numOtherKeys
           << ",\"zstd_level\":" << zstdLevel << ",\"sections\":[";
        auto section = [&](const char* name, const ui64 compressed, const bool first) {
            if (!first) js << ",";
            js << "{\"name\":\"" << name << "\",\"compressed\":" << compressed << "}";
        };
        section("stats", statsBytes, true);
        section("separated_columns", keyColsBytes, false);
        section("other_columns", otherColsBytes, false);
        js << "],\"total_compressed\":" << totalOutputSize
           << ",\"input_file_size\":" << inputSize
           << ",\"output_file_size\":" << totalOutputSize
           << ",\"compression_ratio\":" << std::fixed << std::setprecision(2) << compressionRatio << "}";
        Cout << js.str() << Endl;
    }
    return 0;
}
