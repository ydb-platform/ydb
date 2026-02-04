#include "list_directory_tool.h"
#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/ydb_path.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/common/tabbed_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TListDirectoryTool final : public TToolBase, public TInterruptableCommand {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
List directory in Yandex Data Base (YDB) scheme tree. Returns list of item names inside directory and their types.
For example if called on directory "data/", which contains two tables "my_table1", "my_table2" and one topic "my_topic", then tool will return:
[
    {"name": "my_table1", "type": "table"},
    {"name": "my_table2", "type": "table"},
    {"name": "my_topic", "type": "topic"}
])";

    static constexpr char DIRECTORY_PROPERTY[] = "directory";

public:
    explicit TListDirectoryTool(const TListDirectoryToolSettings& settings)
        : TBase(CreateParametersSchema(), DESCRIPTION)
        , Database(CanonizeYdbPath(settings.Database))
        , Client(settings.Driver)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);

        Directory = Strip(parser.GetKey(DIRECTORY_PROPERTY).GetString());
        if (!Directory.StartsWith('/')) {
            Directory = JoinYdbPath({Database, Directory});
        }
        Directory = CanonizeYdbPath(Directory);
    }

    bool AskPermissions() final {
        TString message;
        if (Directory == Database || Directory == Database + "/") {
            message = "Listing database root directory";
        } else {
            message = TStringBuilder() << "Listing directory " << Directory;
        }

        PrintFtxuiMessage("", message, ftxui::Color::Green);
        Cout << Endl;

        // Directory listing is always allowed
        return true;
    }

    TResponse DoExecute() final {
        Y_DEFER { ResetInterrupted(); };

        auto feature = Client.ListDirectory(Directory);
        if (!WaitInterruptable(feature)) {
            return TResponse::Error(TStringBuilder() << "Listing directory \"" << Directory << "\" was interrupted by user");
        }

        const auto& response = feature.GetValue();
        if (!response.IsSuccess()) {
            Cout << Colors.Red() << "Listing directory \"" << Directory << "\" failed: " << Colors.OldColor() << response.GetStatus() << Endl;
            return TResponse::Error(TStringBuilder() << "Listing directory \"" << Directory << "\" failed with status " << response.GetStatus() << ", reason:\n" << response.GetIssues().ToString());
        }

        const auto& children = response.GetChildren();

        NJson::TJsonValue result;
        auto& resultArray = result.SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& child : children) {
            auto& item = resultArray.emplace_back();
            item["name"] = child.Name;
            item["type"] = EntryTypeToString(child.Type);
        }

        if (GetGlobalLogger().IsVerbose()) {
            Cout << TAdaptiveTabbedTable(children) << Endl;
        }

        return TResponse::Success(std::move(result));
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(DIRECTORY_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Path to directory which should be listed (use empty string to list database root), for example 'data/cold/'")
                .Done()
            .Build();
    }

private:
    const TString Database;
    NScheme::TSchemeClient Client;
    TString Directory;
};

} // anonymous namespace

ITool::TPtr CreateListDirectoryTool(const TListDirectoryToolSettings& settings) {
    return std::make_shared<TListDirectoryTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
