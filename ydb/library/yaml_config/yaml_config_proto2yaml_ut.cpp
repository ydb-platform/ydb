#include <ydb/library/yaml_config/yaml_config_parser.cpp>

#include <library/cpp/testing/unittest/registar.h>


Y_UNIT_TEST_SUITE(YamlConfigProto2Yaml) {
    Y_UNIT_TEST(StorageConfig) {
        NKikimrConfig::StorageConfig storageConfig;
        auto *hostConfig = storageConfig.add_host_config();
        hostConfig->set_host_config_id(1);
        auto *drive = hostConfig->add_drive();
        drive->set_path("/dev/disk/by-partlabel/kikimr_nvme_01");
        drive->set_type("NVME");
        drive = hostConfig->add_drive();
        drive->set_path("/dev/disk/by-partlabel/kikimr_nvme_02");
        drive->set_type("NVME");
        hostConfig = storageConfig.add_host_config();
        hostConfig->set_host_config_id(2);
        drive = hostConfig->add_drive();
        drive->set_path("/dev/disk/by-partlabel/kikimr_nvme_01");
        drive->set_type("SSD");
        auto *host = storageConfig.add_host();
        host->set_host_config_id(1);
        auto *key = host->mutable_key();
        auto *endpoint = key->mutable_endpoint();
        endpoint->set_fqdn("sas8-6954.search.yandex.net");
        endpoint->set_ic_port(19000);
        host = storageConfig.add_host();
        host->set_host_config_id(2);
        key = host->mutable_key();
        endpoint = key->mutable_endpoint();
        endpoint->set_fqdn("sas8-6955.search.yandex.net");
        endpoint->set_ic_port(19000);
        NJson::TJsonValue json;
        NProtobufJson::Proto2Json(storageConfig, json);
        YAML::Node yamlNode;
        TString hostConfigString = (TStringBuilder() << json["host_config"]);
        yamlNode["host_config"] = hostConfigString;
        auto output = YAML::Dump(yamlNode);
        Cerr << output << Endl;
        Cerr << json["host_config"][0]["drive"][1]["path"] << Endl;
        UNIT_ASSERT_EQUAL(json["host_config"][0]["drive"][1]["path"], "/dev/disk/by-partlabel/kikimr_nvme_02");
        NJson::TJsonValue jsonHostConfigs = json["host_config"];
        for (const auto& host_config: jsonHostConfigs.GetArray()) {
            YAML::Node configNode;
            configNode["host_config_id"] = host_config["host_config_id"].GetInteger();
            configNode["drive"] = YAML::Node(YAML::NodeType::Sequence);
            for (const auto& drive : host_config["drive"].GetArray()) {
                YAML::Node driveNode;
                driveNode["path"] = drive["path"].GetString();
                driveNode["type"] = drive["type"].GetString();
                driveNode["expected_slot_count"] = 9; // Default value
                configNode["drive"].push_back(driveNode);
            }
            
            yamlNode["host_configs"].push_back(configNode);
        }
        yamlNode["hosts"] = YAML::Node(YAML::NodeType::Sequence);
        auto jsonHosts = json["host"];
        for (const auto& host : jsonHosts.GetArray()) {
            YAML::Node hostNode;
            hostNode["host"] = host["key"]["endpoint"]["fqdn"].GetString();
            hostNode["port"] = host["key"]["endpoint"]["ic_port"].GetInteger();
            hostNode["host_config_id"] = host["host_config_id"].GetInteger();
            yamlNode["hosts"].push_back(hostNode);
        }
        yamlNode["item_config_generation"] = json["item_config_generation"].GetInteger();
        output = YAML::Dump(yamlNode);
        Cerr << output << Endl;
    }
}