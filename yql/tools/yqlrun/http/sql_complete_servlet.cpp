#include "sql_complete_servlet.h"

#include <yt/yql/providers/yt/common/yql_names.h>

#include <yql/essentials/sql/v1/ide/completion/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/ide/completion/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/ide/completion/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/impatient/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/static/name_set.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/union/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/charset/utf8.h>
#include <util/stream/buffer.h>

namespace NYql {
namespace NHttp {

namespace {

using namespace NSQLComplete;

TLexerSupplier MakeLexerSupplier(NSQLTranslationV1::TLexers lexers) {
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers,
            ansi,
            NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

TVector<TString> ExtractColumnsFromTableAttr(const std::string_view tableAttr) try {
    TVector<TString> columns;

    if (tableAttr.empty()) {
        return columns;
    }

    const auto root = NYT::NodeFromYsonString(tableAttr);
    if (!root.IsMap() || !root.HasKey(NYql::YqlRowSpecAttribute)) {
        return columns;
    }

    const auto& rowSpec = root[NYql::YqlRowSpecAttribute];
    if (!rowSpec.IsMap() || !rowSpec.HasKey("Type")) {
        return columns;
    }

    const auto& type = rowSpec["Type"];
    if (!type.IsList() || type.AsList().size() < 2U) {
        return columns;
    }

    const auto& typeList = type.AsList();
    if (!typeList[0].IsString() || typeList[0].AsString() != "StructType" || !typeList[1].IsList()) {
        return columns;
    }

    for (const auto& member : typeList[1].AsList()) {
        if (member.IsList() && !member.AsList().empty() && member[0].IsString()) {
            columns.emplace_back(member[0].AsString());
        }
    }

    return columns;
} catch (const std::exception& ex) {
    Cerr << "Error on parsing columns: " << ex.what() << Endl;
    return {};
}

TEnvironment ParseEnvironment(const std::string_view parameters) {
    TEnvironment env;

    if (parameters.empty()) {
        return env;
    }

    const auto root = NYT::NodeFromYsonString(parameters);
    if (!root.IsMap()) {
        return env;
    }

    for (const auto& [name, value] : root.AsMap()) {
        if (value.IsMap() && value.HasKey("Data")) {
            env.Parameters[name] = value["Data"];
        } else {
            env.Parameters[name] = value;
        }
    }

    return env;
}

void AddTable(TSchemaData& data, const std::string_view cluster, const std::string_view table, const TVector<TString>& columns) {
    if (table.empty()) {
        return;
    }

    TString path(table);
    if (!path.StartsWith('/')) {
        path.prepend('/');
    }

    data.Tables[cluster][path].Columns = columns;

    TString name(TStringBuf(path).Skip(1));

    auto& entries = data.Folders[cluster]["/"];
    for (const auto& entry : entries) {
        if (entry.Name == name && entry.Type == TFolderEntry::Table) {
            return;
        }
    }
    entries.emplace_back(TFolderEntry::Table, std::move(name));
}

void WriteCompletedToken(NJson::TJsonWriter& writer, const TCompletedToken& token) {
    const auto length = token.Content.size();

    writer.Write("completedToken"sv);
    writer.OpenMap();
    writer.Write("content"sv, token.Content);
    writer.Write("sourcePosition"sv, token.SourcePosition);
    writer.Write("length"sv, length);
    writer.CloseMap();
}

void WriteCandidate(NJson::TJsonWriter& writer, const TCandidate& candidate) {
    writer.OpenMap();
    writer.Write("kind"sv, ToString(candidate.Kind));
    writer.Write("content"sv, candidate.Content);
    writer.Write("cursorShift"sv, candidate.CursorShift);
    writer.Write("filterText"sv, candidate.FilterText());
    if (candidate.Documentation) {
        writer.Write("documentation"sv, *candidate.Documentation);
    }
    writer.CloseMap();
}

void WriteCompletion(NJson::TJsonWriter& writer, const TCompletion& completion) {
    writer.OpenMap();

    WriteCompletedToken(writer, completion.CompletedToken);

    writer.Write("candidates"sv);
    writer.OpenArray();
    for (const TCandidate& candidate : completion.Candidates) {
        WriteCandidate(writer, candidate);
    }
    writer.CloseArray();

    writer.CloseMap();
}

} // namespace

TSqlCompleteServlet::TSqlCompleteServlet() {
    Lexers_.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    Lexers_.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();

    TFrequencyData frequency = LoadFrequencyData();
    Ranking_ = MakeDefaultRanking(frequency);
    StaticNameService_ = MakeStaticNameService(LoadDefaultNameSet(), frequency);
    ClusterNameService_ = MakeClusterNameService(
        MakeStaticClusterDiscovery({
            "plato",
            "plato_rtmr",
            "pg_catalog",
            "information_schema",
        })
    );
}

INameService::TPtr TSqlCompleteServlet::MakeRequestNameService(const std::string_view tableAttr, const std::string_view outputTable) const
{
    TSchemaData data;

    const TVector<TString> inputColumns = ExtractColumnsFromTableAttr(tableAttr);
    const TVector<TString> clusters = {"", "plato"};

    for (const auto& cluster : clusters) {
        AddTable(data, cluster, "Input", inputColumns);
        AddTable(data, cluster, "Output", {});
        AddTable(data, cluster, outputTable, {});
    }

    TVector<INameService::TPtr> children = {
        StaticNameService_,
        MakeSchemaNameService(
            MakeSimpleSchema(
                MakeStaticSimpleSchema(std::move(data)))),
                ClusterNameService_,
    };

    return MakeUnionNameService(std::move(children), Ranking_);
}

void TSqlCompleteServlet::DoPost(const TRequest& req, TResponse& resp) const {
    NJson::TJsonValue value;
    const std::string_view body(req.Body.AsCharPtr(), req.Body.Size());
    const bool parsed = NJson::ReadJsonFastTree(body, &value, true);
    Y_ENSURE_EX(parsed, THttpError(HTTP_BAD_REQUEST) << "can't parse json");

    const auto program = value["program"].GetStringSafe();
    const auto tableAttr = value["tableAttr"].GetStringSafe();
    const auto parameters = value["parameters"].GetStringSafe();
    const auto outputTable = value["outputTable"].GetStringSafe();
    const auto cursorPosition = std::min<size_t>(value["cursorPosition"].GetUIntegerSafe(), GetNumberOfUTF8Chars(program));

    TConfiguration configuration;
    configuration.Limit = 41;

    const auto engine = MakeSqlCompletionEngine(
        MakeLexerSupplier(Lexers_),
        MakeRequestNameService(tableAttr, outputTable),
        configuration,
        Ranking_
    );

    const auto completion = engine->Complete(
        {
            .Text = program,
            .CursorPosition = cursorPosition,
        },
        ParseEnvironment(parameters)
    ).GetValueSync();

    TBufferOutput output;
    NJson::TJsonWriter writer(&output, false);
    WriteCompletion(writer, completion);
    writer.Flush();

    resp.Body = TBlob::FromBuffer(output.Buffer());
    resp.ContentType = "application/json"sv;
}

} // namespace NHttp
} // namespace NYql
