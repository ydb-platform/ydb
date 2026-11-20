#include "engine.h"
#include "syntax.h"
#include "measurement.h"
#include "ready_client.h"

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

enum class EStage { Text, AntlrAst, YqlAst, KqlQueryList, LogicalPlan };
constexpr std::array<TStringBuf, 5> StageNames = {"text", "antlr_ast", "yql_ast", "kql_query_list", "logical_plan"};

EStage GetStage(const TRequest& request) {
    Y_ENSURE(request.exec_mode() == Ydb::Query::EXEC_MODE_EXECUTE, "Expected execute mode");
    if (request.has_query_content()) {
        Y_ENSURE(request.query_content().syntax() == Ydb::Query::SYNTAX_YQL_V1, "Expected YQL v1");
        return EStage::Text;
    }
    Y_ENSURE(request.has_parsed_query(), "Missing request query");
    switch (request.parsed_query().stage_case()) {
        case Ydb::Query::ParsedQuery::kAntlrAst: return EStage::AntlrAst;
        case Ydb::Query::ParsedQuery::kYqlAst: return EStage::YqlAst;
        case Ydb::Query::ParsedQuery::kKqlQueryList: return EStage::KqlQueryList;
        case Ydb::Query::ParsedQuery::kLogicalPlan: return EStage::LogicalPlan;
        case Ydb::Query::ParsedQuery::STAGE_NOT_SET:
            ythrow yexception() << "Missing or unknown parsed query stage";
        default:
            ythrow yexception() << "Parsed query stage is unsupported on main";
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
    TStringBuf Route;
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
        case EStage::LogicalPlan:
            ythrow yexception() << "Import LogicalPlan from an independent producer";
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
    Ydb::Query::YqlAst referenceAst;
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
            Ydb::Query::SqlV1ParserAst expectedGrammar;
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
            Ydb::Query::KqlQueryList expectedKql;
            engine.WithSession([&] {
                engine.ExportKql(sql, envelope.parameters(), expectedKql);
            });
            Y_ENSURE(equals(expectedKql, request.parsed_query().kql_query_list()), "KQL expression graph differs for " << name);
        } else {
            auto ast = ToAst(request, stage, engine);
            Ydb::Query::YqlAst actualAst;
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

TLogicalResult Dispatch(const TRequest& request, const TEngine& engine, bool verify,
    TStringBuf& route, TMeasurementTime* imported)
{
    const auto stage = GetStage(request);
    route = StageNames[static_cast<size_t>(stage)];
    if (stage == EStage::Text) {
        return engine.LowerText(request.query_content().text(), request.parameters(), verify, imported);
    }
    if (stage == EStage::KqlQueryList) {
        return engine.LowerKql(request.parsed_query().kql_query_list(), request.parameters(), verify, imported);
    }
    if (stage == EStage::LogicalPlan) {
        return engine.LowerLogical(request.parsed_query().logical_plan(), request.parameters(), verify, imported);
    }
    auto ast = ToAst(request, stage, engine);
    if (imported) {
        *imported = TMeasurementTime::Now();
    }
    return engine.Lower(std::move(ast), request.parameters(), verify);
}

TLogicalResult VerifyImported(const TVector<TInput>& inputs, const TEngine& engine) {
    Y_ENSURE(inputs.size() >= 2 && inputs[0].Stage == EStage::Text, "Provide the ordinary text request first");
    TRequest reference;
    DecodeRequest(inputs[0].Bytes, reference);
    const bool compareLogical = std::ranges::any_of(inputs, [](const auto& input) { return input.Stage == EStage::LogicalPlan; });
    TLogicalResult baseline;
    engine.WithSession([&] {
        baseline = engine.LowerText(reference.query_content().text(), reference.parameters(), true, nullptr, compareLogical);
    });
    reference.clear_query();
    std::array<bool, StageNames.size()> seen{true};
    for (size_t i = 1; i < inputs.size(); ++i) {
        const auto& [stage, bytes] = inputs[i];
        const auto index = static_cast<size_t>(stage);
        Y_ENSURE(!seen[index], "Duplicate benchmark stage: " << StageNames[index]);
        seen[index] = true;
        TRequest request;
        DecodeRequest(bytes, request);
        TRequest envelope(request);
        envelope.clear_query();
        Y_ENSURE(google::protobuf::util::MessageDifferencer::Equals(reference, envelope),
            "Request envelope differs for " << StageNames[index]);
        TLogicalResult imported;
        engine.WithSession([&] {
            TStringBuf route;
            imported = Dispatch(request, engine, true, route, nullptr);
        });
        if (stage == EStage::LogicalPlan) {
            Y_ENSURE(imported.RootCount == baseline.RootCount, "LogicalPlan result root count differs");
            for (size_t j = 0; j < imported.Roots.size(); ++j) {
                const auto& actual = imported.Roots[j];
                const auto& expected = baseline.Roots[j];
                Y_ENSURE(actual.ResultIndex == expected.ResultIndex && actual.Columns == expected.Columns
                    && imported.LogicalSignatures[j] == baseline.LogicalSignatures[j], "LogicalPlan relations, values or result columns differ");
            }
            continue;
        }
        Y_ENSURE(imported.RootCount == baseline.RootCount && imported.Roots == baseline.Roots,
            "Imported logical roots, typed plan, or ordered results differ for " << StageNames[index]);
    }
    return baseline;
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

TSample Measure(const TString& bytes, const TEngine& engine) {
    TSample sample;
    engine.WithSession([&] {
        sample.Start = TMeasurementTime::Now();
        TRequest request;
        DecodeRequest(bytes, request);
        sample.Decoded = TMeasurementTime::Now();
        const auto logical = Dispatch(request, engine, false, sample.Route, &sample.Imported);
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

void Bench(const TVector<TString>& requests, const TEngine& engine, size_t warmup, size_t repeats) {
    std::cout << "stage,iteration,bytes,decode_wall_ns,decode_cpu_ns,parse_or_import_wall_ns,parse_or_import_cpu_ns,"
        "frontend_wall_ns,frontend_cpu_ns,total_wall_ns,total_cpu_ns\n";
    RunRounds(requests, warmup, repeats, [&](const TString& bytes) {
        return Measure(bytes, engine);
    }, [](const TString& bytes, size_t round, const TSample& sample) {
        const auto& [start, decoded, imported, done, route] = sample;
        std::cout << route << ',' << round << ',' << bytes.size()
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
    Verify(MakeInputs(sql, envelope, engine, hasKql ? 4 : 3), engine);
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
    if (mode == "--client-ready" && args >= 3) {
        const auto [warmup, repeats] = Iterations(argv[first + 1], argv[first + 2]);
        return RunReadyClient(warmup, repeats, ReadInputs(args - 2, argv + first + 3), StageNames);
    }
    TEngine engine(catalogPath);
    if (mode == "--export" && (args == 4 || args == 5)) {
        Export(ReadFile(argv[first + 1]), argv + first + 2, args - 1, ReadEnvelope(templatePath), engine);
        return 0;
    }
    if (mode == "--check-request" && args == 1) {
        TRequest request;
        DecodeRequest(ReadFile(argv[first + 1]), request);
        engine.WithSession([&] {
            TStringBuf route;
            const auto result = Dispatch(request, engine, true, route, nullptr);
            std::cout << "BENCH_ROUTE=" << route << '\n';
            std::cout << "BENCH_RESULT_COLUMNS=" << ResultColumns(result).c_str() << '\n';
        });
        return 0;
    }
    if (mode == "--verify" && args == 1) {
        TRequest request;
        DecodeRequest(ReadFile(argv[first + 1]), request);
        Y_ENSURE(GetStage(request) == EStage::Text, "Expected a stock SQL text request");
        engine.WithSession([&] {
            const auto result = engine.LowerText(request.query_content().text(), request.parameters(), true);
            std::cout << "BENCH_RESULT_COLUMNS=" << ResultColumns(result).c_str() << '\n';
        });
        return 0;
    }
    if (mode == "--verify" && (args == 3 || args == 4)) {
        const auto result = Verify(ReadInputs(args, argv + first + 1), engine);
        std::cout << "BENCH_RESULT_COLUMNS=" << ResultColumns(result).c_str() << '\n';
        std::cout << args << "-stage logical equivalence verified\n";
        return 0;
    }
    if (mode == "--verify-import" && args >= 2 && args <= 5) {
        const auto result = VerifyImported(ReadInputs(args, argv + first + 1), engine);
        std::cout << "BENCH_RESULT_COLUMNS=" << ResultColumns(result).c_str() << '\n';
        std::cout << args - 1 << " independent input(s) matched the text logical roots\n";
        return 0;
    }
    if ((mode == "--bench" && (args == 5 || args == 6)) || (mode == "--bench-import" && args >= 4 && args <= 7)) {
        const auto [warmup, repeats] = Iterations(argv[first + 1], argv[first + 2]);
        auto inputs = ReadInputs(args - 2, argv + first + 3);
        if (mode == "--bench-import") {
            VerifyImported(inputs, engine);
        } else {
            Verify(inputs, engine);
        }
        TVector<TString> requests;
        for (auto& input : inputs) {
            requests.push_back(std::move(input.Bytes));
        }
        Bench(requests, engine, warmup, repeats);
        return 0;
    }
    if (mode == "--client" && args >= 4) {
        const auto [warmup, repeats] = Iterations(argv[first + 1], argv[first + 2]);
        Client(ReadFile(argv[first + 3]), ReadEnvelope(templatePath), engine,
            ReadStages(args - 3, argv + first + 4), warmup, repeats);
        return 0;
    }
    std::cerr << "Usage: parsed_query_legacy_bench [--catalog FILE] [--template FILE] MODE ARGS\n"
        << "  --check-request REQUEST.pb\n"
        << "  --verify TEXT.pb\n"
        << "  --export QUERY.sql TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --verify TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --verify-import TEXT.pb PARSED.pb [PARSED.pb ...]\n"
        << "  --bench WARMUP REPEATS TEXT.pb ANTLR_AST.pb YQL_AST.pb [KQL.pb]\n"
        << "  --bench-import WARMUP REPEATS TEXT.pb PARSED.pb [PARSED.pb ...]\n"
        << "  --client-ready WARMUP REPEATS REQUEST.pb...\n"
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
