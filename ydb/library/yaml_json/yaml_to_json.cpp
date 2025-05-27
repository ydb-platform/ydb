#include "yaml_to_json.h"
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/node.h>
#include <library/cpp/yaml/as/tstring.h>

namespace NKikimr::NYaml {

    template<typename T>
    bool SetScalarFromYaml(const YAML::Node& yaml, NJson::TJsonValue& json, NJson::EJsonValueType jsonType) {
        T data;
        if (YAML::convert<T>::decode(yaml, data)) {
            json.SetType(jsonType);
            json.SetValue(data);
            return true;
        }
        return false;
    }

    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot) {
        Y_ENSURE_BT(!isRoot || yaml.IsMap(), "YAML root is expected to be a map");

        NJson::TJsonValue json;

        if (yaml.IsMap()) {
            for (const auto& it : yaml) {
                const auto& key = it.first.as<TString>();

                Y_ENSURE_BT(!json.Has(key), "Duplicate key entry: " << key);

                json[key] = Yaml2Json(it.second, false);
            }
            return json;
        } else if (yaml.IsSequence()) {
            json.SetType(NJson::EJsonValueType::JSON_ARRAY);
            for (const auto& it : yaml) {
                json.AppendValue(Yaml2Json(it, false));
            }
            return json;
        } else if (yaml.IsScalar()) {
            if (SetScalarFromYaml<ui64>(yaml, json, NJson::EJsonValueType::JSON_UINTEGER)) {
                return json;
            }

            if (SetScalarFromYaml<i64>(yaml, json, NJson::EJsonValueType::JSON_INTEGER)) {
                return json;
            }

            if (SetScalarFromYaml<bool>(yaml, json, NJson::EJsonValueType::JSON_BOOLEAN)) {
                return json;
            }

            if (SetScalarFromYaml<double>(yaml, json, NJson::EJsonValueType::JSON_DOUBLE)) {
                return json;
            }

            if (SetScalarFromYaml<TString>(yaml, json, NJson::EJsonValueType::JSON_STRING)) {
                return json;
            }
        } else if (yaml.IsNull()) {
            json.SetType(NJson::EJsonValueType::JSON_NULL);
            return json;
        } else if (!yaml.IsDefined()) {
            json.SetType(NJson::EJsonValueType::JSON_UNDEFINED);
            return json;
        }

        ythrow yexception() << "Unknown type of YAML node: '" << yaml.as<TString>() << "'";
    }
}