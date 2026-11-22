#include "convert.h"

#include <yql/essentials/core/project_settings/proto/project_settings.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/yexception.h>

#include <utility>

namespace NYql {
namespace {

TLangVersion ParseProjectLangVersion(TStringBuf value) {
    TLangVersion version;
    YQL_ENSURE(ParseLangVersion(value, version), "Invalid project LangVer");
    return version;
}

TString FormatProjectLangVersion(TLangVersion version) {
    const auto result = FormatLangVersion(version);
    YQL_ENSURE(result, "Invalid project LangVer");
    return *result;
}

} // namespace

TProjectSettings ProjectSettingsFromJson(TStringBuf json) {
    NYT::TNode node;
    try {
        node = NYT::NodeFromJsonString(json);
    } catch (const std::exception& error) {
        ythrow yexception() << "Failed to parse project settings JSON: " << error.what();
    }
    return ProjectSettingsFromYson(node);
}

TProjectSettings ProjectSettingsFromProto(const NProto::TProjectSettings& proto) {
    auto node = NYT::TNode()("DefaultCluster", proto.GetDefaultCluster());
    if (proto.HasLangVer()) {
        node["LangVer"] = proto.GetLangVer();
    }
    if (proto.HasAttributes()) {
        try {
            node["Attributes"] = NYT::NodeFromYsonString(proto.GetAttributes());
        } catch (const std::exception& error) {
            ythrow yexception() << "Failed to parse project settings protobuf Attributes: " << error.what();
        }
    }
    auto settings = ProjectSettingsFromYson(node);
    for (const auto& pragma : proto.GetPragmas()) {
        settings.AddPragma(pragma.GetName(), TVector<TString>(pragma.GetValue().begin(), pragma.GetValue().end()));
    }
    return settings;
}

TString ProjectSettingsToJson(const TProjectSettings& settings) {
    return NJson2Yson::ConvertYson2Json(NYT::NodeToYsonString(ProjectSettingsToYson(settings)));
}

TProjectSettings ProjectSettingsFromYson(const NYT::TNode& settings) {
    TStringBuf field = "root";
    try {
        YQL_ENSURE(settings.IsMap(), "Project settings must be a map");
        TProjectSettings projectSettings;
        field = "DefaultCluster";
        if (settings.HasKey("DefaultCluster")) {
            projectSettings.DefaultCluster = settings["DefaultCluster"].AsString();
        }
        field = "LangVer";
        if (settings.HasKey("LangVer")) {
            projectSettings.LangVer = ParseProjectLangVersion(settings["LangVer"].AsString());
        }
        field = "Attributes";
        if (settings.HasKey("Attributes")) {
            YQL_ENSURE(settings["Attributes"].IsMap(), "Project attributes must be a map");
            projectSettings.Attributes = settings["Attributes"];
        }
        field = "Pragmas";
        if (settings.HasKey("Pragmas")) {
            for (const auto& pragma : settings["Pragmas"].AsList()) {
                TVector<TString> values;
                field = "Pragmas.Value";
                for (const auto& value : pragma["Value"].AsList()) {
                    values.push_back(value.AsString());
                }
                field = "Pragmas.Name";
                projectSettings.AddPragma(pragma["Name"].AsString(), std::move(values));
            }
        }
        return projectSettings;
    } catch (const std::exception& error) {
        ythrow yexception() << "Failed to parse project settings field " << field << ": " << error.what();
    }
}

NYT::TNode ProjectSettingsToYson(const TProjectSettings& settings) {
    auto pragmas = NYT::TNode::CreateList();
    for (const auto& pragma : settings.Pragmas) {
        auto values = NYT::TNode::CreateList();
        for (const auto& value : pragma.Value) {
            values.Add(value);
        }
        pragmas.Add(NYT::TNode()("Name", pragma.Name)("Value", values));
    }
    auto node = NYT::TNode()("Pragmas", pragmas)("DefaultCluster", settings.DefaultCluster)("Attributes", settings.Attributes);
    if (settings.LangVer) {
        node["LangVer"] = FormatProjectLangVersion(*settings.LangVer);
    }
    return node;
}

} // namespace NYql
