#include "list_directory_tool.h"

#include <ydb/core/base/path.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/common/tabbed_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TListDirectoryTool final : public ITool {
    static constexpr char DESCRIPTION[] = R"(
List directory in Yandex Data Base (YDB) scheme tree. Returns list of item names inside directory and their types.
For example if called on directory 'data/', which contains two tables 'my_table1', 'my_table2' and one topic 'my_topic', then tool will return:
[
    {"name": "my_table1", "type": "table"},
    {"name": "my_table2", "type": "table"},
    {"name": "my_topic", "type": "topic"}
])";

    static constexpr char DIRECTORY_PROPERTY[] = "directory";

public:
    explicit TListDirectoryTool(TClientCommand::TConfig& config)
        : ParametersSchema(TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(DIRECTORY_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Path to directory which should be listed (use empty string to list database root), for example 'data/cold/'")
                .Done()
            .Build()
        )
        , Description(DESCRIPTION)
        , Database(NKikimr::CanonizePath(config.Database))
        , Client(TDriver(config.CreateDriverConfig()))
    {}

    const NJson::TJsonValue& GetParametersSchema() const final {
        return ParametersSchema;
    }

    const TString& GetDescription() const final {
        return Description;
    }

    TResponse Execute(const NJson::TJsonValue& parameters) final try {
        const auto& directory = ParseParameters(parameters);
        const auto response = Client.ListDirectory(directory).ExtractValueSync();
        if (!response.IsSuccess()) {
            return TResponse(TStringBuilder() << "Listing directory failed with status " << response.GetStatus() << ", reason:\n" << response.GetIssues().ToString());
        }

        const auto& children = response.GetChildren();

        NJson::TJsonValue result;
        auto& resultArray = result.SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& child : children) {
            auto& item = resultArray.emplace_back();
            item["name"] = child.Name;
            item["type"] = EntryTypeToString(child.Type);
        }

        Cout << TAdaptiveTabbedTable(children);

        return TResponse(std::move(result));
    } catch (const std::exception& e) {
        return TResponse(TStringBuilder() << "Listing directory failed. " << e.what());
    }

private:
    TString ParseParameters(const NJson::TJsonValue& parameters) const {
        TJsonParser parser(parameters);

        TString directory = Strip(parser.GetKey(DIRECTORY_PROPERTY).GetString());
        if (!directory.StartsWith('/')) {
            directory = NKikimr::JoinPath({Database, directory});
        }

        return NKikimr::CanonizePath(directory);
    }

private:
    const NJson::TJsonValue ParametersSchema;
    const TString Description;
    const TString Database;
    NScheme::TSchemeClient Client;
};

} // anonymous namespace

ITool::TPtr CreateListDirectoryTool(TClientCommand::TConfig& config) {
    return std::make_shared<TListDirectoryTool>(config);
}

} // namespace NYdb::NConsoleClient::NAi
