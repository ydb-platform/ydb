#include "explain_query_tool.h"
#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TExplainQueryTool final : public TToolBase, public TInterruptableCommand {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Explain query in Yandex Data Base (YDB) on YQL (SQL dialect). Use cases:
- Explain data query to get query plan, statistics and AST
- Validate query syntax, column and table names and semantics

IMPORTANT:
- Tool CAN NOT be used for any DDL queries.
- NEVER guess column names, types or keys. If you do not know the exact schema of a table, use the `describe` tool FIRST.
- NEVER guess data values for filtering. Do not assume specific values exist in the table.
  Instead, query the distinct values first (e.g., `SELECT DISTINCT column_name FROM my_table LIMIT 20`) to verify the actual values in the database.
- To get the schema of a table (columns, types, etc.), use the `describe` tool instead of this one.
- If path to table contains '/' or '@', wrap it into back ticks, for example `path/to/table`. Add back ticks only if they are really needed, for example table some_table do not need backticks.

Returns query plan and AST.
)";

    static constexpr char QUERY_PROPERTY[] = "query";

    static constexpr ui64 MAX_INFO_SIZE = 100_KB;

public:
    explicit TExplainQueryTool(const TExplainQueryToolSettings& settings)
        : TBase(CreateParametersSchema(), DESCRIPTION)
        , YQLHighlighter(MakeYQLHighlighter(GetColorSchema()))
        , ExplainRunner(settings.Driver)
        , QueryPlanPrinter(EDataFormat::Default)
    {}

private:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Query = Strip(parser.GetKey(QUERY_PROPERTY).GetString());
    }

    bool AskPermissions() final {
        TColors colors;
        try {
            colors.resize(Query.size(), replxx::Replxx::Color::DEFAULT);
            YQLHighlighter->Apply(Query, colors);
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Warning, "Error highlighting query: " << e.what());
            colors.assign(Query.size(), replxx::Replxx::Color::DEFAULT);
        }

        YDB_CLI_LOG(Notice, "Agent wants to explain query:\n" << PrintYqlHighlightAnsiColors(Query, colors));
        PrintFtxuiMessage(PrintYqlHighlightFtxuiColors(Query, colors), "Explaining query", ftxui::Color::Green);
        return true;
    }

    TResponse DoExecute() final {
        Y_DEFER { ResetInterrupted(); };

        try {
            auto result = ExplainRunner.Explain(Query);

            if (IsInterrupted()) {
                YDB_CLI_LOG(Notice, "Query explain was interrupted by user");
                return TResponse::Error("Query explain was interrupted by user");
            }

            NJson::TJsonValue resultJson;
            resultJson["plan"] = TruncateString(std::move(result.PlanJson));
            resultJson["ast"] = TruncateString(std::move(result.Ast));

            if (GetGlobalLogger().IsVerbose()) {
                Cout << Colors.Green() << "\nQuery Plan:" << Colors.OldColor() << Endl << Endl;
                QueryPlanPrinter.Print(result.PlanJson);
                Cout << Colors.Green() << "\nQuery AST:" << Colors.OldColor() << Endl << Endl << Strip(result.Ast) << Endl;
            }

            return TResponse::Success(std::move(resultJson));
        } catch (const std::exception& e) {
            Cout << Endl << Colors.Red() << "Query explain failed:\n" << Colors.OldColor() << e.what() << Flush;
            return TResponse::Error(TStringBuilder() << "Query explain failed with error:\n" << e.what());
        }
    }

    static TString TruncateString(TString&& str) {
        if (str.size() <= MAX_INFO_SIZE) {
            return str;
        }
        return TStringBuilder() << str.substr(0, MAX_INFO_SIZE / 2) << "...\n(TRUNCATED, real size: " << str.size() << " bytes)\n...." << str.substr(str.size() - MAX_INFO_SIZE / 2);
    }

    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Property(QUERY_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Query to explain on YQL (SQL dialect), for example 'SELECT * FROM my_table'")
                .Done()
            .Build();
    }

    const IYQLHighlighter::TPtr YQLHighlighter;
    TExplainGenericQuery ExplainRunner;
    TQueryPlanPrinter QueryPlanPrinter;
    TString Query;
};

} // anonymous namespace

ITool::TPtr CreateExplainQueryTool(const TExplainQueryToolSettings& settings) {
    return std::make_shared<TExplainQueryTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
