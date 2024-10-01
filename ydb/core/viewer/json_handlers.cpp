#include "json_handlers.h"
#include <library/cpp/yaml/as/tstring.h>

namespace NKikimr::NViewer {

TSimpleYamlBuilder::TSimpleYamlBuilder(TInitializer initializer) {
    Method = Root[TString(initializer.Method)];
    if (initializer.Url) {
        Method["tags"].push_back(TString(initializer.Url.After('/').Before('/')));
    }
    if (initializer.Tag) {
        Method["tags"].push_back(TString(initializer.Tag));
    }
    if (initializer.Summary) {
        Method["summary"] = TString(initializer.Summary);
    }
    if (initializer.Description) {
        Method["description"] = TString(initializer.Description);
    }
}

void TSimpleYamlBuilder::SetParameters(YAML::Node parameters) {
    Method["parameters"] = parameters;
}

void TSimpleYamlBuilder::AddParameter(TParameter parameter) {
    YAML::Node param;
    param["in"] = "query";
    param["name"] = TString(parameter.Name);
    if (parameter.Description) {
        param["description"] = TString(parameter.Description);
    }
    if (parameter.Type) {
        param["type"] = TString(parameter.Type);
    }
    if (parameter.Default) {
        param["default"] = TString(parameter.Default);
    }
    param["required"] = parameter.Required;
    Method["parameters"].push_back(param);
}

void TSimpleYamlBuilder::SetResponseSchema(YAML::Node schema) {
    Method["responses"]["200"]["content"]["application/json"]["schema"] = schema;
}

}
