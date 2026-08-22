#pragma once

#include <util/generic/string.h>

namespace YAML {
class Node;
}

namespace google::protobuf {
class Message;
}

namespace NMVP {

extern const TStringBuf CONFIG_ERROR_PREFIX;

void MergeYamlNodeToProto(const YAML::Node& node, google::protobuf::Message& proto);

bool TryLoadTokenFromFile(const TString& tokenPath, TString& token, TString& error, const TString& tokenName = TString());

} // namespace NMVP
