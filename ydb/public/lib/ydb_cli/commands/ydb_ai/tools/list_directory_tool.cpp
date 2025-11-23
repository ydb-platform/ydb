#include "list_directory_tool.h"

#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TListDirectoryTool final : public ITool {
public:
    explicit TListDirectoryTool(TClientCommand::TConfig& config)
        : Database(NKikimr::CanonizePath(config.Database))
        , Client(TDriver(config.CreateDriverConfig()))
    {
        NJson::TJsonValue dirParam;
        dirParam["type"] = "string";
        dirParam["description"] = "Directory path to list (use empty to list root directory)";  // AI-TODO: proper description

        ParametersSchema["properties"]["directory"] = dirParam;
        ParametersSchema["type"] = "object";
        ParametersSchema["required"][0] = "directory";
    }

    TString GetName() const final {
        return "list_directory";
    }

    NJson::TJsonValue GetParametersSchema() const final {
        return ParametersSchema;
    }

    TString GetDescription() const final {
        return "List directory";  // AI-TODO: proper description
    }

    TString Execute(const NJson::TJsonValue& parameters) final {
        ValidateJsonType(parameters, NJson::JSON_MAP);

        const auto& dir = ValidateJsonKey(parameters, "directory");
        ValidateJsonType(dir, NJson::JSON_STRING, "directory");

        TString dirString = dir.GetString();
        Cerr << "\n!! List directory: " << dirString << Endl;  // AI-TODO: proper list directory printing

        if (!dirString.StartsWith('/')) {
            dirString = NKikimr::JoinPath({Database, dirString});
        }

        // AI-TODO: progress printing
        auto result = Client.ListDirectory(dirString).ExtractValueSync();

        // AI-TODO: proper error printing
        if (!result.IsSuccess()) {
            Cerr << "\n!! List directory error [" << result.GetStatus() << "]: " << result.GetIssues().ToString() << Endl;
            return TStringBuilder() << "Error listing directory, status: " << result.GetStatus() << ", issues: " << result.GetIssues().ToString();
        }

        const auto& children = result.GetChildren();

        // AI-TODO: proper result formating
        TStringBuilder resultBuilder;
        for (const auto& child : children) {
            resultBuilder << child.Name << " (" << child.Type << ")" << "\n";
        }

        Cerr << "\n!! List directory result: " << resultBuilder << Endl;  // AI-TODO: proper query result printing

        return resultBuilder;
    }

private:
    // AI-TODO: reduce copypaste
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
    const TString Database;
    NScheme::TSchemeClient Client;
    NJson::TJsonValue ParametersSchema;
};

} // anonymous namespace

ITool::TPtr CreateListDirectoryTool(TClientCommand::TConfig& config) {
    return std::make_shared<TListDirectoryTool>(config);
}

} // namespace NYdb::NConsoleClient::NAi
