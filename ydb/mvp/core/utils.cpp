#include "utils.h"

#include <ydb/library/yaml_json/yaml_to_json.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <yaml-cpp/yaml.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NMVP {

const TStringBuf CONFIG_ERROR_PREFIX = "Configuration error: ";

void MergeYamlNodeToProto(const YAML::Node& node, google::protobuf::Message& proto) {
    if (!node || !node.IsDefined() || node.IsNull()) {
        return;
    }

    const NJson::TJsonValue json = NKikimr::NYaml::Yaml2Json(node, true);
    NProtobufJson::MergeJson2Proto(
        json,
        proto,
        NProtobufJson::TJson2ProtoConfig()
            .SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense)
            .SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive)
            .SetAllowUnknownFields(false));
}

bool TryLoadTokenFromFile(const TString& tokenPath, TString& token, TString& error, const TString& tokenName) {
    const TString tokenSuffix = tokenName.empty() ? TString() : TStringBuilder() << " for token " << tokenName;
    try {
        token = Strip(TUnbufferedFileInput(tokenPath).ReadAll());
    } catch (const yexception& ex) {
        error = TStringBuilder() << "Failed to read token from '" << tokenPath << "'" << tokenSuffix << ": " << ex.what();
        return false;
    }
    if (token.empty()) {
        error = TStringBuilder() << "Token read from '" << tokenPath << "'" << tokenSuffix << " is empty";
        return false;
    }
    return true;
}

} // namespace NMVP
