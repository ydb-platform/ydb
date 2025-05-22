#pragma once

#include <library/cpp/json/json_value.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

namespace NKikimr::NYaml {

    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot);

}