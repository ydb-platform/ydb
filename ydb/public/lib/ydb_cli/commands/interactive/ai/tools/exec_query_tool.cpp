#include "exec_query_tool.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TExecQueryTool final : public ITool {
    static constexpr char DESCRIPTION[] = R"(
Execute query in Yandex Data Base (YDB) on YQL (SQL dialect). Returns list of result sets for query, each contains list of rows and column metadata.
For example if there exists table 'my_table' with string column 'Data' and we execute query:
```
$filtered = SELECT * FROM my_table WHERE Data IS NOT NULL;
SELECT Data || "-first" FROM $filtered;
SELECT Data || "-second" FROM $filtered;
```
Tool will return:
[
    {
        "rows": [
            {"Data": "A-first"},
            {"Data": "B-first"}
        ],
        "columns": [
            {"name": "Data", "type": "string"}
        ]
    },
    {
        "rows": [
            {"Data": "A-second"},
            {"Data": "B-second"}
        ],
        "columns": [
            {"name": "Data", "type": "string"}
        ]
    }
])";

    static constexpr char QUERY_PROPERTY[] = "query";

public:
    explicit TExecQueryTool(TClientCommand::TConfig& config)
        :  ParametersSchema(TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(QUERY_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Query to execute on YQL (SQL dialect), for example 'SELECT * FROM my_table'")
                .Done()
            .Build()
        )
        , Description(DESCRIPTION)
        , Client(TDriver(config.CreateDriverConfig()))
    {}

    const NJson::TJsonValue& GetParametersSchema() const final {
        return ParametersSchema;
    }

    const TString& GetDescription() const final {
        return Description;
    }

    TResponse Execute(const NJson::TJsonValue& parameters) final try {
        const TString& query = ParseParameters(parameters);
        const auto response = Client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
        if (!response.IsSuccess()) {
            return TResponse(TStringBuilder() << "Query execution failed with status " << response.GetStatus() << ", reason:\n" << response.GetIssues().ToString());
        }

        NJson::TJsonValue result;
        auto& resultArray = result.SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& resultSet : response.GetResultSets()) {
            auto& item = resultArray.emplace_back();

            const auto& columnMeta = resultSet.GetColumnsMeta();
            auto& columns = item["columns"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            for (const auto& column : columnMeta) {
                auto& item = columns.emplace_back();
                item["name"] = column.Name;
                item["type"] = column.Type.ToString();
            }

            auto& rows = item["rows"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                TJsonParser row;
                Y_DEBUG_VERIFY(row.Parse(FormatResultRowJson(parser, resultSet.GetColumnsMeta(), EBinaryStringEncoding::Unicode)), "Internal error. Invalid serialized JSON row value.");
                rows.emplace_back(row.GetValue());
            }

            TResultSetPrinter(EDataFormat::Pretty).Print(resultSet);
        }

        return TResponse(std::move(result));
    } catch (const std::exception& e) {
        return TResponse(TStringBuilder() << "Query execution failed. " << e.what());
    }

private:
    TString ParseParameters(const NJson::TJsonValue& parameters) const {
        TJsonParser parser(parameters);
        return Strip(parser.GetKey(QUERY_PROPERTY).GetString());
    }

private:
    const NJson::TJsonValue ParametersSchema;
    const TString Description;
    NQuery::TQueryClient Client;
};

} // anonymous namespace

ITool::TPtr CreateExecQueryTool(TClientCommand::TConfig& config) {
    return std::make_shared<TExecQueryTool>(config);
}

} // namespace NYdb::NConsoleClient::NAi
