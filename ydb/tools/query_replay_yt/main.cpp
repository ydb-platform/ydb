#include "query_replay.h"

#include "metadata.h"
#include "plan_check.h"
#include "replay_runner.h"

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/backend.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <util/system/fs.h>
#include <util/datetime/base.h>
#include <exception>

using namespace NActors;

namespace {

constexpr size_t MaxOutputFieldBytes = 1024 * 1024; // 1 MiB per heavy text field
constexpr TStringBuf TruncatedSuffix = "...[truncated]";

TString TruncateOutputField(TString value, TStringBuf fieldName, TStringBuf queryId) {
    if (value.size() <= MaxOutputFieldBytes) {
        return value;
    }

    Cerr << "Truncating output field '" << fieldName
        << "' for query_id=" << queryId
        << " from " << value.size()
        << " bytes to " << MaxOutputFieldBytes << " bytes" << Endl;

    if (MaxOutputFieldBytes > TruncatedSuffix.size()) {
        value.resize(MaxOutputFieldBytes - TruncatedSuffix.size());
        value += TruncatedSuffix;
    } else {
        value.resize(MaxOutputFieldBytes);
    }

    return value;
}

} // anonymous namespace

class TQueryReplayMapper
    : public NYT::IMapper<NYT::TTableReader<NYT::TNode>, NYT::TTableWriter<NYT::TNode>>
{
    TVector<TString> UdfFiles;
    ui32 ActorSystemThreadsCount = 5;
    NActors::NLog::EPriority YqlLogPriority = NActors::NLog::EPriority::PRI_ERROR;
    bool Antlr4ParserIsAmbiguityError = false;
    TQueryReplayRunner Runner;

public:
    TQueryReplayMapper() = default;

    Y_SAVELOAD_JOB(UdfFiles, ActorSystemThreadsCount, YqlLogPriority, Antlr4ParserIsAmbiguityError);

    TQueryReplayMapper(TVector<TString> udfFiles, ui32 actorSystemThreadsCount, bool antlr4ParserIsAmbiguityError,
        NActors::NLog::EPriority yqlLogPriority = NActors::NLog::EPriority::PRI_ERROR)
        : UdfFiles(std::move(udfFiles))
        , ActorSystemThreadsCount(actorSystemThreadsCount)
        , YqlLogPriority(yqlLogPriority)
        , Antlr4ParserIsAmbiguityError(antlr4ParserIsAmbiguityError)
    {}

    void Start(NYT::TTableWriter<NYT::TNode>*) override {
        Runner.Init(UdfFiles, ActorSystemThreadsCount, Antlr4ParserIsAmbiguityError, YqlLogPriority);
    }

    void Do(NYT::TTableReader<NYT::TNode>* in, NYT::TTableWriter<NYT::TNode>* out) override {
        for (; in->IsValid(); in->Next()) {
            const auto& row = in->GetRow();
            const TString queryId = row["query_id"].AsString();
            NJson::TJsonValue json(NJson::JSON_MAP);
            for (const auto& [key, child]: row.AsMap()) {
                if (key == "_logfeller_timestamp")
                    continue;
                json.InsertValue(key, NJson::TJsonValue(child.AsString()));
            }

            auto response = Runner.RunReplay(std::move(json));
            if (response == nullptr)
                continue;

            auto status = response.Get()->Status;

            TString failReason = NKikimr::NQueryReplay::StatusToFailReason(status);

            if (failReason == "unspecified" || status == TQueryReplayEvents::MissingTableMetadata) {
                continue;
            }

            const TString queryPlan = TruncateOutputField(row["query_plan"].AsString(), "query_plan", queryId);
            const TString queryText = TruncateOutputField(row["query_text"].AsString(), "query_text", queryId);
            const TString tableMetadata = TruncateOutputField(row["table_metadata"].AsString(), "table_metadata", queryId);
            const TString extraMessage = TruncateOutputField(response.Get()->Message, "extra_message", queryId);
            const TString newQueryPlan = TruncateOutputField(response.Get()->Plan, "new_query_plan", queryId);

            NYT::TNode result;
            result = result("query_id", queryId);

            result = result("created_at", row["created_at"].AsString());
            result = result("query_cluster", row["query_cluster"].AsString());
            result = result("query_database", row["query_database"].AsString());

            result = result("query_plan", queryPlan);
            result = result("query_syntax", row["query_syntax"].AsString());
            result = result("query_text", queryText);

            result = result("query_type", row["query_type"].AsString());
            result = result("table_metadata", tableMetadata);
            result = result("version", row["version"].AsString());

            result = result("fail_reason", failReason);
            result = result("extra_message", extraMessage);
            result = result("new_query_plan", newQueryPlan);

            out->AddRow(result);
        }
    }

    void Finish(NYT::TTableWriter<NYT::TNode>*) override {
        Runner.Stop();
    }
};

static NYT::TTableSchema OutputSchema() {
    NYT::TTableSchema schema;
    schema.AddColumn(NYT::TColumnSchema().Name("fail_reason").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("extra_message").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("new_query_plan").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_id").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("created_at").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_cluster").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_database").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_plan").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_syntax").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_text").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_type").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("table_metadata").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("version").Type(NYT::VT_STRING));
    return schema;
}

REGISTER_NAMED_MAPPER("Query replay mapper", TQueryReplayMapper);

int main(int argc, const char** argv) {
    NYT::TConfig::Get()->LogLevel = NYT::NLogLevel::Info;
    NYT::Initialize(argc, argv);

    TQueryReplayConfig config;
    config.ParseConfig(argc, argv);
    Cerr << "query_replay_yt config: "
        << "cluster=" << config.Cluster
        << ", src-path=" << config.SrcPath
        << ", dst-path=" << config.DstPath
        << ", core-table-path=" << config.CoreTablePath
        << ", threads=" << config.ActorSystemThreadsCount
        << ", udf-files=" << config.UdfFiles.size()
        << ", antlr4-ambiguity-error=" << (config.Antlr4ParserIsAmbiguityError ? "true" : "false")
        << Endl;

    if (config.QueryFile) {
        Cerr << "Running in local mode for single query file: " << config.QueryFile << Endl;
        TQueryReplayRunner runner;
        runner.Init(config.UdfFiles, config.ActorSystemThreadsCount, config.Antlr4ParserIsAmbiguityError, config.YqlLogLevel);
        Y_DEFER {
            runner.Stop();
        };

        NJson::TJsonValue queryJson;
        {
            static NJson::TJsonReaderConfig readConfig;
            TFileInput in(config.QueryFile);
            NJson::ReadJsonTree(&in, &readConfig, &queryJson, false);
        }

        Cerr << "Running local replay of the query:" << Endl
            << "Database: " << queryJson["query_database"].GetStringSafe() << Endl
            << UnescapeC(queryJson["query_text"].GetStringSafe()) << Endl;

        auto tableMetadata = ExtractStaticMetadata(queryJson);
        Cerr << "Tables: " << Endl;

        for (auto& [name, meta] : tableMetadata) {
            Cerr << "TableName: " << name << Endl;
            NKikimrKqp::TKqpTableMetadataProto protoDescription;
            meta->ToMessage(&protoDescription);
            Cerr << protoDescription.Utf8DebugString() << Endl;
        }

        auto result = runner.RunReplay(std::move(queryJson));
        if (!result) {
            Cerr << "Query type is not supported for replay" << Endl;
            return EXIT_FAILURE;
        }
        const auto status = result->Status;
        const TString failReason = NKikimr::NQueryReplay::StatusToFailReason(status);
        Cerr << failReason << Endl;
        Cerr << result->Message << Endl;
        return status == TQueryReplayEvents::Success ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (config.Cluster.empty() || config.SrcPath.empty() || config.DstPath.empty()) {
        Cerr << "Non local execution requires Cluster and SrcPath and DstPath options to be specified.";
        return EXIT_FAILURE;
    }

    Cerr << "Creating YT client for cluster: " << config.Cluster << Endl;
    auto client = NYT::CreateClient(config.Cluster);
    Cerr << "YT client created successfully." << Endl;

    NYT::TMapOperationSpec spec;
    spec.AddInput<NYT::TNode>(config.SrcPath);
    spec.AddOutput<NYT::TNode>(NYT::TRichYPath(config.DstPath).Schema(OutputSchema()));

    auto userJobSpec = NYT::TUserJobSpec();
    userJobSpec.MemoryLimit(5_GB);

    for(const auto& [udf, udfInJob]: GetJobFiles(config.UdfFiles)) {
        userJobSpec.AddLocalFile(udf, NYT::TAddLocalFileOptions().PathInJob(udfInJob));
    }

    spec.MapperSpec(userJobSpec);
    if (!config.CoreTablePath.empty()) {
        spec.CoreTablePath(config.CoreTablePath);
    }
    spec.MaxFailedJobCount(10000);

    Cerr << "Starting map operation. src-path=" << config.SrcPath
        << ", dst-path=" << config.DstPath << Endl;
    const auto mapStart = TInstant::Now();
    try {
        client->Map(spec, new TQueryReplayMapper(config.UdfFiles, config.ActorSystemThreadsCount, config.Antlr4ParserIsAmbiguityError, config.YqlLogLevel));
    } catch (const std::exception& e) {
        Cerr << "Map operation failed after " << (TInstant::Now() - mapStart) << ": " << e.what() << Endl;
        return EXIT_FAILURE;
    } catch (...) {
        Cerr << "Map operation failed after " << (TInstant::Now() - mapStart) << ": unknown exception" << Endl;
        return EXIT_FAILURE;
    }
    Cerr << "Map operation finished in " << (TInstant::Now() - mapStart) << Endl;

    auto mergeSpec = NYT::TMergeOperationSpec();
    mergeSpec.AddInput(NYT::TRichYPath(config.DstPath));
    mergeSpec.Output(NYT::TRichYPath(config.DstPath));
    mergeSpec.CombineChunks(true);
    mergeSpec.ForceTransform(true);

    Cerr << "Starting merge operation for dst-path=" << config.DstPath << Endl;
    const auto mergeStart = TInstant::Now();
    try {
        client->Merge(mergeSpec);
    } catch (const std::exception& e) {
        Cerr << "Merge operation failed after " << (TInstant::Now() - mergeStart) << ": " << e.what() << Endl;
        return EXIT_FAILURE;
    } catch (...) {
        Cerr << "Merge operation failed after " << (TInstant::Now() - mergeStart) << ": unknown exception" << Endl;
        return EXIT_FAILURE;
    }
    Cerr << "Merge operation finished in " << (TInstant::Now() - mergeStart) << Endl;

    return EXIT_SUCCESS;
}
