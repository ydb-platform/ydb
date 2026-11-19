#include "engine.h"
#include "syntax.h"
#include "measurement.h"

#include <google/protobuf/util/message_differencer.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>

#include <algorithm>
#include <array>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

enum class EStage { Text, AntlrAst, YqlAst, KqlQueryList };
constexpr std::array<TStringBuf, 4> StageNames = {"text", "antlr_ast", "yql_ast", "kql_query_list"};

EStage GetStage(const TRequest& request) {
    Y_ENSURE(request.exec_mode() == Ydb::Query::EXEC_MODE_EXECUTE, "Expected execute mode");
    if (request.has_query_content()) {
        Y_ENSURE(request.query_content().syntax() == Ydb::Query::SYNTAX_YQL_V1, "Expected YQL v1");
        return EStage::Text;
    }
    switch (request.parsed_query().stage_case()) {
        case Ydb::Query::Benchmark::ParsedQuery::kAntlrAst: return EStage::AntlrAst;
        case Ydb::Query::Benchmark::ParsedQuery::kYqlAst: return EStage::YqlAst;
        case Ydb::Query::Benchmark::ParsedQuery::kKqlQueryList: return EStage::KqlQueryList;
        default: ythrow yexception() << "Missing or unsupported query stage";
    }
}

TString ReadFile(const char* name) {
    std::ifstream file(name, std::ios::binary | std::ios::ate);
    Y_ENSURE(file, "Cannot read " << name);
    const std::streamoff length = file.tellg();
    Y_ENSURE(length >= 0 && length <= static_cast<std::streamoff>(MaxRequestBytes), "Request file size limit exceeded");
    TString bytes(static_cast<size_t>(length), '\0');
    file.seekg(0);
    Y_ENSURE(file.read(bytes.Detach(), bytes.size()), "Could not read " << name);
    return bytes;
}

void WriteFile(const char* name, const TString& bytes) {
    Y_ENSURE(bytes.size() <= MaxRequestBytes, "Request file size limit exceeded");
    std::ofstream file(name, std::ios::binary);
    Y_ENSURE(file && file.write(bytes.data(), bytes.size()), "Could not write " << name);
}

template <class TMessage>
TString Encode(const TMessage& message) {
    TString bytes;
    Y_ENSURE(message.SerializeToString(&bytes) && bytes.size() <= MaxRequestBytes,
        "Cannot serialize protobuf message within the file size limit");
    return bytes;
}

struct TSample {
    TMeasurementTime Start;
    TMeasurementTime Decoded;
    TMeasurementTime Imported;
    TMeasurementTime Done;
};

TString Prepare(EStage stage, const TString& text, const TRequest& envelope, const TEngine& engine) {
    TRequest request(envelope);
    switch (stage) {
        case EStage::Text:
            request.mutable_query_content()->set_syntax(Ydb::Query::SYNTAX_YQL_V1);
            request.mutable_query_content()->set_text(text);
            break;
        case EStage::AntlrAst:
            ExportGrammar(text, envelope, engine, *request.mutable_parsed_query()->mutable_antlr_ast());
            break;
        case EStage::YqlAst: {
            const auto ast = TranslateText(text, envelope, engine);
            ExportAst(*ast.Root, *request.mutable_parsed_query()->mutable_yql_ast());
            break;
        }
        case EStage::KqlQueryList:
            engine.ExportKql(text, envelope.parameters(),
                *request.mutable_parsed_query()->mutable_kql_query_list());
            break;
    }
    return Encode(request);
}

NYql::TAstParseResult ToAst(const TRequest& request, EStage stage, const TEngine& engine) {
    switch (stage) {
        case EStage::AntlrAst: return TranslateGrammar(request.parsed_query().antlr_ast(), request, engine);
        case EStage::YqlAst: return ImportAst(request.parsed_query().yql_ast());
        default: ythrow yexception() << "Expected a syntax input";
    }
}

struct TInput {
    EStage Stage;
    TString Bytes;
};

TRequest ExecutionEnvelope() {
    TRequest request;
    request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
    return request;
}

TVector<TInput> ReadInputs(int count, const char** paths) {
    TVector<TInput> inputs;
    for (int i = 0; i < count; ++i) {
        auto bytes = ReadFile(paths[i]);
        TRequest request;
        DecodeRequest(bytes, request);
        inputs.push_back({GetStage(request), std::move(bytes)});
    }
    return inputs;
}

TLogicalResult Verify(const TVector<TInput>& inputs, const TEngine& engine) {
    const auto equals = google::protobuf::util::MessageDifferencer::Equals;
    std::array<const TInput*, StageNames.size()> ordered{};
    for (const auto& input : inputs) {
        const size_t index = static_cast<size_t>(input.Stage);
        Y_ENSURE(!ordered[index], "Duplicate benchmark stage: " << StageNames[index]);
        ordered[index] = &input;
    }
    Y_ENSURE(ordered[0] && ordered[1] && ordered[2], "Provide text, antlr_ast and yql_ast requests");
    TRequest envelope;
    DecodeRequest(ordered[0]->Bytes, envelope);
    const TString sql = envelope.query_content().text();
    envelope.clear_query();
    const auto baselineAst = TranslateText(sql, envelope, engine);
    Ydb::Query::Benchmark::YqlAst referenceAst;
    ExportAst(*baselineAst.Root, referenceAst);
    std::optional<TLogicalResult> baseline;
    for (const auto* input : ordered) {
        if (!input) {
            continue;
        }
        const auto& [stage, bytes] = *input;
        TRequest request;
        DecodeRequest(bytes, request);
        const auto name = StageNames[static_cast<size_t>(stage)];
        TRequest actualEnvelope(request);
        actualEnvelope.clear_query();
        Y_ENSURE(equals(envelope, actualEnvelope), "Request envelope differs for " << name);
        if (stage == EStage::AntlrAst) {
            Ydb::Query::Benchmark::SqlV1ParserAst expectedGrammar;
            ExportGrammar(sql, envelope, engine, expectedGrammar);
            Y_ENSURE(equals(expectedGrammar, request.parsed_query().antlr_ast()), "Grammar AST or tokens differ for " << name);
        } else if (stage == EStage::YqlAst) {
            Y_ENSURE(equals(referenceAst, request.parsed_query().yql_ast()), "Translated YQL AST differs for " << name);
        }

        TLogicalResult result;
        if (stage == EStage::Text) {
            engine.WithSession([&] {
                result = engine.LowerText(request.query_content().text(), request.parameters(), true);
            });
        } else if (stage == EStage::KqlQueryList) {
            engine.WithSession([&] {
                result = engine.LowerKql(request.parsed_query().kql_query_list(), request.parameters(), true, nullptr);
            });
            Ydb::Query::Benchmark::KqlQueryList expectedKql;
            engine.WithSession([&] {
                engine.ExportKql(sql, envelope.parameters(), expectedKql);
            });
            Y_ENSURE(equals(expectedKql, request.parsed_query().kql_query_list()), "KQL expression graph differs for " << name);
        } else {
            auto ast = ToAst(request, stage, engine);
            Ydb::Query::Benchmark::YqlAst actualAst;
            ExportAst(*ast.Root, actualAst);
            Y_ENSURE(equals(actualAst, referenceAst), "Translated YQL AST differs for " << name);
            engine.WithSession([&] { result = engine.Lower(std::move(ast), request.parameters(), true); });
        }
        if (baseline) {
            Y_ENSURE(baseline->RootCount == result.RootCount && baseline->Roots == result.Roots, "Logical roots, plan, ordered columns or types differ for " << name);
        } else {
            baseline = std::move(result);
        }
    }
    return std::move(*baseline);
}

TString ResultColumns(const TLogicalResult& result) {
    NJson::TJsonValue schema(NJson::JSON_ARRAY);
    for (const auto& root : result.Roots) {
        Y_ENSURE(root.ResultIndex < result.Roots.size(), "Invalid result index");
        auto& columns = schema[static_cast<size_t>(root.ResultIndex)];
        Y_ENSURE(!columns.IsDefined(), "Duplicate result index");
        columns.SetType(NJson::JSON_ARRAY);
        for (const auto& [name, type] : root.Columns) {
            auto& column = columns.AppendValue(NJson::TJsonValue(NJson::JSON_ARRAY));
            column.AppendValue(name);
            column.AppendValue(type);
        }
    }
    return NJson::WriteJson(schema, false);
}

TSample Measure(const TInput& input, const TEngine& engine) {
    TSample sample;
    engine.WithSession([&] {
        sample.Start = TMeasurementTime::Now();
        TRequest request;
        DecodeRequest(input.Bytes, request);
        sample.Decoded = TMeasurementTime::Now();
        const auto stage = GetStage(request);
        TLogicalResult logical;
        if (stage == EStage::Text) {
            logical = engine.LowerText(request.query_content().text(), request.parameters(), false, &sample.Imported);
        } else if (stage == EStage::KqlQueryList) {
            logical = engine.LowerKql(request.parsed_query().kql_query_list(), request.parameters(), false, &sample.Imported);
        } else {
            auto ast = ToAst(request, stage, engine);
            sample.Imported = TMeasurementTime::Now();
            logical = engine.Lower(std::move(ast), request.parameters(), false);
        }
        sample.Done = logical.Done;
        Y_ENSURE(logical.RootCount, "No logical roots");
    });
    return sample;
}

template <class TCases, class TMeasure, class TPrint>
void RunRounds(const TCases& cases, size_t warmup, size_t repeats, TMeasure&& measure, TPrint&& print) {
    TVector<decltype(measure(cases.front()))> samples;
    samples.reserve(repeats * cases.size());
    const auto index = [&](size_t round, size_t slot) {
        const auto size = cases.size();
        return (round + ((round / size) % 2 ? size - 1 - slot : slot)) % size;
    };
    for (size_t round = 0; round < warmup + repeats; ++round) {
        for (size_t j = 0; j < cases.size(); ++j) {
            auto sample = measure(cases[index(round, j)]);
            if (round >= warmup) {
                samples.push_back(std::move(sample));
            }
        }
    }
    for (size_t round = 0; round < repeats; ++round) {
        for (size_t j = 0; j < cases.size(); ++j) {
            print(cases[index(round + warmup, j)], round, samples[round * cases.size() + j]);
        }
    }
}

void Bench(const TVector<TInput>& inputs, const TEngine& engine, size_t warmup, size_t repeats) {
    Verify(inputs, engine);
    std::cout << "stage,iteration,bytes,decode_wall_ns,decode_cpu_ns,parse_or_import_wall_ns,parse_or_import_cpu_ns,"
        "frontend_wall_ns,frontend_cpu_ns,total_wall_ns,total_cpu_ns\n";
    RunRounds(inputs, warmup, repeats, [&](const TInput& input) {
        return Measure(input, engine);
    }, [](const TInput& input, size_t round, const TSample& sample) {
        const auto& [start, decoded, imported, done] = sample;
        std::cout << StageNames[static_cast<size_t>(input.Stage)] << ',' << round << ',' << input.Bytes.size()
            << ',' << decoded.Wall - start.Wall << ',' << decoded.Cpu - start.Cpu
            << ',' << imported.Wall - decoded.Wall << ',' << imported.Cpu - decoded.Cpu
            << ',' << done.Wall - imported.Wall << ',' << done.Cpu - imported.Cpu
            << ',' << done.Wall - start.Wall << ',' << done.Cpu - start.Cpu << '\n';
    });
}

struct TClientSample {
    size_t Bytes;
    TMeasurementTime Start;
    TMeasurementTime Done;
};

TVector<TInput> MakeInputs(const TString& sql, const TRequest& envelope, const TEngine& engine, size_t count) {
    TVector<TInput> inputs;
    for (size_t i = 0; i < count; ++i) {
        const auto stage = static_cast<EStage>(i);
        if (stage == EStage::KqlQueryList) {
            engine.WithSession([&] { inputs.push_back({stage, Prepare(stage, sql, envelope, engine)}); });
        } else {
            inputs.push_back({stage, Prepare(stage, sql, envelope, engine)});
        }
    }
    return inputs;
}

void Client(const TString& sql, const TRequest& envelope, const TEngine& engine, const TVector<EStage>& stages,
    size_t warmup, size_t repeats)
{
    const bool hasKql = std::find(stages.begin(), stages.end(), EStage::KqlQueryList) != stages.end();
    Verify(MakeInputs(sql, envelope, engine, hasKql ? StageNames.size() : StageNames.size() - 1), engine);
    std::cout << "stage,iteration,bytes,client_wall_ns,client_cpu_ns\n";
    RunRounds(stages, warmup, repeats, [&](EStage stage) {
        TClientSample sample;
        const auto measure = [&] {
            sample.Start = TMeasurementTime::Now();
            const auto bytes = Prepare(stage, sql, envelope, engine);
            sample.Done = TMeasurementTime::Now();
            sample.Bytes = bytes.size();
        };
        if (stage == EStage::KqlQueryList) {
            engine.WithSession(measure);
        } else {
            measure();
        }
        return sample;
    }, [](EStage stage, size_t round, const TClientSample& sample) {
        std::cout << StageNames[static_cast<size_t>(stage)] << ',' << round << ',' << sample.Bytes
            << ',' << sample.Done.Wall - sample.Start.Wall << ',' << sample.Done.Cpu - sample.Start.Cpu << '\n';
    });
}

void Export(const TString& sql, const char** paths, size_t stages, const TRequest& envelope, const TEngine& engine) {
    const auto inputs = MakeInputs(sql, envelope, engine, stages);
    Verify(inputs, engine);
    for (size_t i = 0; i < inputs.size(); ++i) {
        WriteFile(paths[i], inputs[i].Bytes);
    }
}

std::pair<size_t, size_t> Iterations(const char* warmupArg, const char* repeatsArg) {
    const auto warmup = FromString<size_t>(warmupArg);
    const auto repeats = FromString<size_t>(repeatsArg);
    Y_ENSURE(warmup <= 100000 && repeats > 0 && repeats <= 100000, "Invalid iteration count");
    return {warmup, repeats};
}

TRequest ReadEnvelope(const std::optional<TString>& templatePath) {
    auto envelope = ExecutionEnvelope();
    if (templatePath) {
        DecodeRequest(ReadFile(templatePath->c_str()), envelope);
    }
    envelope.clear_query();
    return envelope;
}

TVector<EStage> ReadStages(int count, const char** names) {
    TVector<EStage> stages;
    std::array<bool, StageNames.size()> seen{};
    for (int i = 0; i < count; ++i) {
        const auto stage = std::find(StageNames.begin(), StageNames.end(), TStringBuf(names[i]));
        Y_ENSURE(stage != StageNames.end(), "Unknown benchmark stage: " << names[i]);
        const auto index = stage - StageNames.begin();
        Y_ENSURE(!seen[index], "Duplicate benchmark stage: " << names[i]);
        seen[index] = true;
        stages.push_back(static_cast<EStage>(index));
    }
    return stages;
}

int Run(int argc, const char** argv) {
    std::optional<TString> templatePath;
    TString catalogPath;
    int first = 1;
    while (first + 1 < argc) {
        const TStringBuf option = argv[first];
        if (option == "--catalog") {
            Y_ENSURE(catalogPath.empty(), "Specify one offline catalog");
            catalogPath = argv[first + 1];
        } else if (option == "--template") {
            Y_ENSURE(!templatePath, "Specify one request template");
            templatePath = argv[first + 1];
        } else {
            break;
        }
        first += 2;
    }
    const TStringBuf mode = first < argc ? argv[first] : "";
    const int args = argc - first - 1;
    Y_ENSURE(!templatePath || mode == "--export" || mode == "--client", "Template requires --export or --client");
    TEngine engine(catalogPath);
    if (mode == "--export" && (args == 4 || args == 5)) {
        Export(ReadFile(argv[first + 1]), argv + first + 2, args - 1, ReadEnvelope(templatePath), engine);
        return 0;
    }
    if (mode == "--verify" && (args == 3 || args == 4)) {
        const auto result = Verify(ReadInputs(args, argv + first + 1), engine);
        std::cout << "BENCH_RESULT_COLUMNS=" << ResultColumns(result).c_str() << '\n';
        std::cout << args << "-stage logical equivalence verified\n";
        return 0;
    }
    if (mode == "--bench" && (args == 5 || args == 6)) {
        const auto [warmup, repeats] = Iterations(argv[first + 1], argv[first + 2]);
        Bench(ReadInputs(args - 2, argv + first + 3), engine, warmup, repeats);
        return 0;
    }
    if (mode == "--client" && args >= 4) {
        const auto [warmup, repeats] = Iterations(argv[first + 1], argv[first + 2]);
        Client(ReadFile(argv[first + 3]), ReadEnvelope(templatePath), engine,
            ReadStages(args - 3, argv + first + 4), warmup, repeats);
        return 0;
    }
    std::cerr << "Usage: parsed_query_legacy_bench [--catalog FILE] [--template FILE] MODE ARGS\n"
        << "  --export QUERY.sql TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --verify TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --bench WARMUP REPEATS TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --client WARMUP REPEATS QUERY.sql STAGE...\n";
    return 2;
}

} // namespace
} // namespace NKikimr::NKqp::NParsedQueryBench

int main(int argc, const char** argv) {
    try {
        return NKikimr::NKqp::NParsedQueryBench::Run(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
