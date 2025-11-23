#include "exec_query_tool.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/writer/json.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TExecQueryTool final : public ITool {
public:
    explicit TExecQueryTool(TClientCommand::TConfig& config)
        : Client(TDriver(config.CreateDriverConfig()))
    {
        NJson::TJsonValue sqlParam;
        sqlParam["type"] = "string";

        ParametersSchema["sql"] = sqlParam;
    }

    TString GetName() const final {
        return "execute_sql_query";
    }

    NJson::TJsonValue GetParametersSchema() const final {
        return ParametersSchema;
    }

    TString GetDescription() const final {
        return "Execute SQL query";  // AI-TODO: proper description
    }

    TString Execute(const NJson::TJsonValue& parameters) final {
        ValidateJsonType(parameters, NJson::JSON_MAP);

        const auto& sql = ValidateJsonKey(parameters, "sql");
        ValidateJsonType(sql, NJson::JSON_STRING, "sql");

        const auto& sqlString = sql.GetString();
        Cerr << "\n!! Execute SQL query: " << sqlString << Endl;  // AI-TODO: proper query execution printing

        // AI-TODO: streaming execution
        auto result = Client.ExecuteQuery(sqlString, NQuery::TTxControl::NoTx()).ExtractValueSync();

        // AI-TODO: proper error printing
        if (!result.IsSuccess()) {
            return TStringBuilder() << "Error executing SQL query, status: " << result.GetStatus() << ", issues: " << result.GetIssues().ToString();
        }

        const auto& resultSets = result.GetResultSets();

        // AI-TODO: proper result formating
        TStringBuilder resultBuilder;
        for (ui64 i = 0; i < resultSets.size(); ++i) {
            resultBuilder << "Result set " << i << ":\n" << Endl;

            TResultSetParser parser(resultSets[i]);
            while (parser.TryNextRow()) {
                NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &resultBuilder.Out);
                FormatResultRowJson(parser, resultSets[i].GetColumnsMeta(), writer, EBinaryStringEncoding::Unicode);
                resultBuilder << "\n";
            }
        }

        Cerr << "\n!! Execute query result: " << resultBuilder << Endl;  // AI-TODO: proper query result printing

        return resultBuilder;
    }

private:
    void ValidateJsonType(const NJson::TJsonValue& value, NJson::EJsonValueType expectedType, const std::optional<TString>& fieldName = std::nullopt) const {
        if (const auto valueType = value.GetType(); valueType != expectedType) {
            throw yexception() << "Tool request " << (fieldName ? " field '" + *fieldName + "'" : "") << " has unexpected type: " << valueType << ", expected type: " << expectedType;
        }
    }

    const NJson::TJsonValue& ValidateJsonKey(const NJson::TJsonValue& value, const TString& key, const std::optional<TString>& fieldName = std::nullopt) const {
        const auto* output = value.GetMap().FindPtr(key);
        if (!output) {
            throw yexception() << "Tool request does not contain '" << key << "' field" << (fieldName ? " in '" + *fieldName + "'" : "");
        }

        return *output;
    }

private:
    NQuery::TQueryClient Client;
    NJson::TJsonValue ParametersSchema;
};

} // anonymous namespace

ITool::TPtr CreateExecQueryTool(TClientCommand::TConfig& config) {
    return std::make_shared<TExecQueryTool>(config);
}

} // namespace NYdb::NConsoleClient::NAi
