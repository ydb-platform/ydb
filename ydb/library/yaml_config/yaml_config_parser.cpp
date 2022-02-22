#include "yaml_config_parser.h"

#include <util/generic/string.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/erasure/erasure.h>


namespace NKikimr::NYaml {

    template<typename T>
    static bool SetScalarFromYaml(const YAML::Node& yaml, NJson::TJsonValue& json, NJson::EJsonValueType jsonType) {
        T data;
        if (YAML::convert<T>::decode(yaml, data)) {
            json.SetType(jsonType);
            json.SetValue(data);
            return true;
        }
        return false;
    }

    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot) {
        Y_ENSURE(!isRoot || yaml.IsMap(), "YAML root is expected to be a map");

        NJson::TJsonValue json;

        if (yaml.IsMap()) {
            for (const auto& it : yaml) {
                const auto& key = it.first.as<TString>();

                Y_ENSURE(!json.Has(key), "Duplicate key entry: " << key);

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
            if (SetScalarFromYaml<ui64>(yaml, json, NJson::EJsonValueType::JSON_UINTEGER))
                return json;

            if (SetScalarFromYaml<i64>(yaml, json, NJson::EJsonValueType::JSON_INTEGER))
                return json;

            if (SetScalarFromYaml<bool>(yaml, json, NJson::EJsonValueType::JSON_BOOLEAN))
                return json;

            if (SetScalarFromYaml<double>(yaml, json, NJson::EJsonValueType::JSON_DOUBLE))
                return json;

            if (SetScalarFromYaml<TString>(yaml, json, NJson::EJsonValueType::JSON_STRING))
                return json;

        } else if (yaml.IsNull()) {
            json.SetType(NJson::EJsonValueType::JSON_NULL);
            return json;
        } else if (!yaml.IsDefined()) {
            json.SetType(NJson::EJsonValueType::JSON_UNDEFINED);
            return json;
        }

        ythrow yexception() << "Unknown type of YAML node: '" << yaml.as<TString>() << "'";
    }

    static NProtobufJson::TJson2ProtoConfig GetJsonToProtoConfig() {
        NProtobufJson::TJson2ProtoConfig config;
        config.SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense);
        config.SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive);
        config.CastRobust = true;
        config.MapAsObject = true;
        config.AllowUnknownFields = false;
        return config;
    }

   static NJson::TJsonValue BuildDefaultChannels(NJson::TJsonValue& json) {
        TString erasureName = json["static_erasure"].GetStringSafe();
        NJson::TJsonValue channelsInfo;
        channelsInfo.SetType(NJson::EJsonValueType::JSON_ARRAY);
        for(ui32 channelId = 0; channelId < 3; ++channelId) {
            NJson::TJsonValue channelInfo;
            channelInfo.InsertValue("channel", channelId);
            channelInfo.InsertValue("channel_erasure_name", erasureName);

            NJson::TJsonValue history;
            history.SetType(NJson::EJsonValueType::JSON_ARRAY);

            NJson::TJsonValue historyEntry;
            historyEntry.InsertValue("from_generation", NJson::TJsonValue(0));
            historyEntry.InsertValue("group_id", NJson::TJsonValue(0));

            history.AppendValue(historyEntry);

            channelInfo.InsertValue("history", history);
            channelsInfo.AppendValue(channelInfo);
        }
        return channelsInfo;
    }

    std::vector<std::pair<TString, ui32>> GetDefaultTablets() {
        return {
            {"FLAT_HIVE", 1},
            {"FLAT_BS_CONTROLLER", 1},
            {"FLAT_SCHEMESHARD", 1},
            {"NODE_BROKER", 1},
            {"TENANT_SLOT_BROKER", 1},
            {"CONSOLE", 1},
            {"CMS", 1},
            {"TX_MEDIATOR", 3},
            {"FLAT_TX_COORDINATOR", 3},
            {"TX_ALLOCATOR", 3},
        };
    }

    ui32 GetDefaultTabletCount(TString& type) {
        auto defaults = GetDefaultTablets();
        for(auto [type_, cnt] : defaults) {
            if (type == type_) {
                return cnt;
            }
        }
        Y_ENSURE(false, "unknown tablet " << type);
    }

    bool isUnique(TString& type) {
        return GetDefaultTabletCount(type) == 1;
    }

    std::vector<TString> GetTabletTypes() {
        auto defaults = GetDefaultTablets();
        std::vector<TString> types;
        for(auto [type, cnt] : defaults) {
            types.push_back(type);
        }
        return types;
    }

    TString HostAndICPort(const NJson::TJsonValue& host) {
        TString hostname = host["host"].GetStringSafe();
        ui32 InterconnectPort = 19001;
        if (host.Has("port")) {
            InterconnectPort = host["port"].GetUIntegerSafe();
        }

        return TStringBuilder() << hostname << ":" << InterconnectPort;
    }

    NJson::TJsonValue FindNodeByString(NJson::TJsonValue& json, const TString& data) {
        ui32 foundCandidates = 0;
        NJson::TJsonValue result;
        for(auto& host: json["nameservice_config"]["node"].GetArraySafe()) {
            if (data == host["host"].GetStringSafe()) {
                result = host;
                ++foundCandidates;
            }
        }

        if (foundCandidates == 1) {
            return result;
        }

        foundCandidates = 0;
        for(auto& host: json["nameservice_config"]["node"].GetArraySafe()) {
            if (data == HostAndICPort(host)) {
                result = host;
                ++foundCandidates;
            }
        }

        Y_ENSURE(foundCandidates == 1, "Cannot find node_id for " << data);
        return result;
    }

    ui32 FindNodeId(NJson::TJsonValue& json, NJson::TJsonValue& host) {
        if (host.IsUInteger()) {
            return host.GetUIntegerSafe();
        }

        auto node = FindNodeByString(json, host.GetStringSafe());
        return node["node_id"].GetUIntegerSafe();
    }

    ui64 GetNextTabletID(TString& type, ui32 idx) {
        std::unordered_map<TString, ui64> tablets = {
            {"FLAT_HIVE", 72057594037968897},
            {"FLAT_BS_CONTROLLER", 72057594037932033},
            {"FLAT_SCHEMESHARD", 72075186232723360},
            {"CMS", 72057594037936128},
            {"NODE_BROKER", 72057594037936129},
            {"TENANT_SLOT_BROKER", 72057594037936130},
            {"CONSOLE", 72057594037936131},
            {"TX_ALLOCATOR", TDomainsInfo::MakeTxAllocatorIDFixed(1, idx)},
            {"FLAT_TX_COORDINATOR", TDomainsInfo::MakeTxCoordinatorIDFixed(1, idx)},
            {"TX_MEDIATOR", TDomainsInfo::MakeTxMediatorIDFixed(1, idx)},
        };

        auto it = tablets.find(type);
        Y_ENSURE(it != tablets.end());
        return it->second;
    }

    const NJson::TJsonArray::TArray& GetTabletsFor(NJson::TJsonValue& json, TString type) {
        auto& systemTabletsConfig = json["system_tablets"];
        Y_ENSURE(json.Has("static_erasure"));
        TString erasureName = json["static_erasure"].GetStringSafe();
        TString toLowerType = to_lower(type);

        if (!systemTabletsConfig.Has(toLowerType)) {
            auto& stubs = systemTabletsConfig[toLowerType];
            stubs.SetType(NJson::EJsonValueType::JSON_ARRAY);
            for(ui32 idx = 0; idx < GetDefaultTabletCount(type); ++idx) {
                NJson::TJsonValue stub;
                stub.SetType(NJson::EJsonValueType::JSON_MAP);
                stub.InsertValue("type", type);

                stubs.AppendValue(std::move(stub));
            }
        }

        ui32 idx = 0;
        for(NJson::TJsonValue& tablet : systemTabletsConfig[toLowerType].GetArraySafe()) {
            ++idx;

            if (!tablet.Has("node")) {
                auto defaultNode = systemTabletsConfig["default_node"];
                tablet.InsertValue("node", defaultNode);
            }

            if (!tablet.Has("type")) {
                tablet.InsertValue("type", type);
            }

            Y_ENSURE(tablet["node"].IsArray());

            NJson::TJsonValue& tabletInfo = tablet["info"];

            if (!tabletInfo.Has("tablet_id")) {
                Y_ENSURE(idx <= GetDefaultTabletCount(type));
                tabletInfo.InsertValue("tablet_id", NJson::TJsonValue(GetNextTabletID(type, idx)));
            }

            if (!tabletInfo.Has("channels")) {
                tabletInfo.InsertValue("channels", BuildDefaultChannels(json));
            }
        }

        return json["system_tablets"][toLowerType].GetArraySafe();
    }

    void PrepareActorSystemConfig(NJson::TJsonValue& json) {
        if (!json.Has("actor_system_config")) {
            return;
        }

        auto& config = json["actor_system_config"];
        auto& executors = config["executor"];

        const std::vector<std::pair<TString, TString>> defaultExecutors = {{"io_executor", "IO"}, {"sys_executor", "SYSTEM"}, {"user_executor", "USER"}, {"batch_executor", "BATCH"}};
        for(const auto& [fieldName, name]: defaultExecutors) {
            if (!config.Has(fieldName)) {
                ui32 executorID = 0;
                for(const auto& executor: executors.GetArraySafe()) {
                    if (to_lower(executor["name"].GetStringSafe()) == to_lower(name)) {
                        config.InsertValue(fieldName, executorID);
                        break;
                    }

                    ++executorID;
                }
            }

            Y_ENSURE(config.Has(fieldName), "cannot deduce executor id for " << fieldName);
        }

        if (!config.Has("service_executor")) {
            auto& se = config["service_executor"];
            se.SetType(NJson::EJsonValueType::JSON_ARRAY);
            ui32 executorID = 0;

            for(const auto& executor: executors.GetArraySafe()) {
                if (to_lower(executor["name"].GetStringSafe()) == "ic") {
                    NJson::TJsonValue val;
                    val.InsertValue("service_name", "Interconnect");
                    val.InsertValue("executor_id", executorID);
                    se.AppendValue(val);
                }

                ++executorID;
            }
        }
    }

    ui32 PdiskCategoryFromString(TString& data) {
        if (data == "ROT") {
            return 0;
        } else if (data == "SSD") {
            return 1;
        } else if (data == "NVME") {
            return 2;
        }

        Y_ENSURE(false, "unknown pdisk category " << data);
    }

    ui32 ErasureStrToNum(const TString& info) {
        TErasureType::EErasureSpecies species = TErasureType::ErasureSpeciesByName(info);
        Y_ENSURE(species != TErasureType::ErasureSpeciesCount, "unknown erasure " << info);
        return species;
    }

    void PrepareStaticGroup(NJson::TJsonValue& json) {
        if (!json.Has("blob_storage_config")) {
            return;
        }

        auto& config = json["blob_storage_config"];
        Y_ENSURE(config["service_set"].IsMap());

        auto& serviceSet = config["service_set"];
        if (!serviceSet.Has("availability_domains")) {
            NJson::TJsonValue arr;
            arr.SetType(NJson::EJsonValueType::JSON_ARRAY);
            arr.AppendValue(NJson::TJsonValue(1));
            serviceSet.InsertValue("availability_domains", arr);
        }

        Y_ENSURE(serviceSet.Has("groups"));
        auto& groups = serviceSet["groups"];
        bool shouldFillVdisks = !serviceSet.Has("vdisks");
        auto& vdisksServiceSet = serviceSet["vdisks"];
        if (shouldFillVdisks) {
            vdisksServiceSet.SetType(NJson::EJsonValueType::JSON_ARRAY);
        }

        bool shouldFillPdisks = !serviceSet.Has("pdisks");
        auto& pdisksServiceSet = serviceSet["pdisks"];
        if (shouldFillPdisks) {
            pdisksServiceSet.SetType(NJson::EJsonValueType::JSON_ARRAY);
        }

        ui32 groupID = 0;

        for(auto& group: groups.GetArraySafe()) {
            if (!group.Has("group_generation")) {
                group.InsertValue("group_generation", NJson::TJsonValue(1));
            }

            if (!group.Has("group_id")) {
                group.InsertValue("group_id", NJson::TJsonValue(groupID));
            }

            ui32 groupID = group["group_id"].GetUIntegerSafe();
            ui32 groupGeneration = group["group_generation"].GetUIntegerSafe();
            Y_ENSURE(group.Has("erasure_species"));
            if (group["erasure_species"].IsString()) {
                auto species = group["erasure_species"].GetStringSafe();
                ui32 num = ErasureStrToNum(species);
                group.EraseValue("erasure_species");
                group.InsertValue("erasure_species", NJson::TJsonValue(num));
            }

            Y_ENSURE(group.Has("rings"));
            Y_ENSURE(group["rings"].IsArray());

            auto& ringsInfo = group["rings"].GetArraySafe();

            ui32 ringID = 0;
            std::unordered_map<ui32, std::unordered_set<ui32>> UniquePdiskIds;
            std::unordered_map<ui32, std::unordered_set<ui32>> UniquePdiskGuids;

            for(auto& ring: ringsInfo) {
                Y_ENSURE(ring["fail_domains"].IsArray());

                auto& failDomains = ring["fail_domains"].GetArraySafe();
                ui32 failDomainID = 0;
                for(auto& failDomain: failDomains) {
                    Y_ENSURE(failDomain["vdisk_locations"].IsArray());
                    Y_ENSURE(failDomain["vdisk_locations"].GetArraySafe().size() == 1);

                    for(auto& vdiskLocation: failDomain["vdisk_locations"].GetArraySafe()) {
                        Y_ENSURE(vdiskLocation.Has("node_id"));
                        vdiskLocation.InsertValue("node_id", FindNodeId(json, vdiskLocation["node_id"]));
                        if (!vdiskLocation.Has("vdisk_slot_id")) {
                            vdiskLocation.InsertValue("vdisk_slot_id", NJson::TJsonValue(0));
                        }

                        ui64 myNodeId = vdiskLocation["node_id"].GetUIntegerSafe();
                        if (!vdiskLocation.Has("pdisk_guid")) {
                            for(ui32 pdiskGuid = 1; ; pdiskGuid++) {
                                if (UniquePdiskGuids[myNodeId].find(pdiskGuid) == UniquePdiskGuids[myNodeId].end()) {
                                    vdiskLocation.InsertValue("pdisk_guid",  NJson::TJsonValue(pdiskGuid));
                                    break;
                                }
                            }
                        }

                        Y_ENSURE(UniquePdiskGuids[myNodeId].insert(vdiskLocation["pdisk_guid"].GetUIntegerSafe()).second, "pdisk guids should be unique");

                        if (!vdiskLocation.Has("pdisk_id")) {
                            for(ui32 pdiskID = 1; ; pdiskID++) {
                                if (UniquePdiskIds[myNodeId].find(pdiskID) == UniquePdiskIds[myNodeId].end()) {
                                    vdiskLocation.InsertValue("pdisk_id",  NJson::TJsonValue(pdiskID));
                                    break;
                                }
                            }
                        }

                        Y_ENSURE(UniquePdiskIds[myNodeId].insert(vdiskLocation["pdisk_id"].GetUIntegerSafe()).second, "pdisk ids should be unique");

                        if (shouldFillPdisks) {
                            NJson::TJsonValue pdiskInfo = vdiskLocation;
                            if (pdiskInfo.Has("vdisk_slot_id")) {
                                pdiskInfo.EraseValue("vdisk_slot_id");
                            }

                            if (pdiskInfo.Has("pdisk_category")) {
                                if (pdiskInfo["pdisk_category"].IsString()) {
                                    auto cat = pdiskInfo["pdisk_category"].GetStringSafe();
                                    pdiskInfo.InsertValue("pdisk_category", NJson::TJsonValue(PdiskCategoryFromString(cat)));
                                }
                            }

                            pdisksServiceSet.AppendValue(pdiskInfo);
                        }

                        if (vdiskLocation.Has("path")) {
                            vdiskLocation.EraseValue("path");
                        }

                        if (vdiskLocation.Has("pdisk_category")) {
                            vdiskLocation.EraseValue("pdisk_category");
                        }

                        if (shouldFillVdisks) {

                            NJson::TJsonValue myVdisk;
                            auto loc = vdiskLocation;
                            myVdisk.InsertValue("vdisk_location", loc);
                            NJson::TJsonValue vdiskID;
                            vdiskID.InsertValue("domain", NJson::TJsonValue(failDomainID));
                            vdiskID.InsertValue("ring", NJson::TJsonValue(ringID));
                            vdiskID.InsertValue("vdisk", NJson::TJsonValue(0));
                            vdiskID.InsertValue("group_id", NJson::TJsonValue(groupID));
                            vdiskID.InsertValue("group_generation", NJson::TJsonValue(groupGeneration));
                            myVdisk.InsertValue("vdisk_id", vdiskID);
                            vdisksServiceSet.AppendValue(myVdisk);
                        }
                    }

                    ++failDomainID;
                }

                ++ringID;
            }

            ++groupID;
        }
    }

    void PrepareSystemTabletsInfo(NJson::TJsonValue& json)  {
        if (!json.Has("system_tablets")) {
            auto& config = json["system_tablets"];
            config.SetType(NJson::EJsonValueType::JSON_MAP);
        }

        if (!json["system_tablets"].Has("default_node")) {
            Y_ENSURE(json["nameservice_config"]["node"].IsArray());

            auto& config = json["system_tablets"]["default_node"];
            config.SetType(NJson::EJsonValueType::JSON_ARRAY);
            for(auto node: json["nameservice_config"]["node"].GetArraySafe()) {
                Y_ENSURE(node["node_id"].IsUInteger(), "node_id must be specified");
                auto nodeId = node["node_id"];
                config.AppendValue(nodeId);
            }
        }

    }

    void PrepareBootstrapConfig(NJson::TJsonValue& json) {
        if (json.Has("bootstrap_config") && json["bootstrap_config"].Has("tablet")) {
            return;
        }

        if (!json.Has("bootstrap_config")) {
            auto& bootstrapConfig = json["bootstrap_config"];
            bootstrapConfig.SetType(NJson::EJsonValueType::JSON_MAP);
        }

        auto& bootstrapConfig = json["bootstrap_config"];
        auto& tabletSet = bootstrapConfig["tablet"];
        tabletSet.SetType(NJson::EJsonValueType::JSON_ARRAY);
        for(auto type : GetTabletTypes()) {
            for(auto tablet: GetTabletsFor(json, type)) {
                tabletSet.AppendValue(tablet);
            }
        }
    }

    void PrepareDomainsConfig(NJson::TJsonValue& json) {

        Y_ENSURE(json.Has("domains_config"));
        Y_ENSURE(json["domains_config"].IsMap());
        NJson::TJsonValue& domainsConfig = json["domains_config"];

        Y_ENSURE(domainsConfig.Has("domain"));
        NJson::TJsonValue& domains = domainsConfig["domain"];

        Y_ENSURE(domains.GetArraySafe().size() == 1);

        if (!domainsConfig.Has("hive_config")) {
            auto& hiveConfig = domainsConfig["hive_config"];
            hiveConfig.SetType(NJson::EJsonValueType::JSON_ARRAY);
            NJson::TJsonValue defaultHiveInfo;
            defaultHiveInfo.SetType(NJson::EJsonValueType::JSON_MAP);
            defaultHiveInfo.InsertValue("hive_uid", 1);
            defaultHiveInfo.InsertValue("hive", std::move(NJson::TJsonValue(72057594037968897)));
            hiveConfig.AppendValue(defaultHiveInfo);
        }

        for(NJson::TJsonValue& domain: domains.GetArraySafe()) {
            Y_ENSURE(domain.Has("name"));

            if (domain.Has("domain_id")) {
                Y_ENSURE(domain["domain_id"].GetUIntegerSafe() == 1);
            } else {
                domain.InsertValue("domain_id", std::move(NJson::TJsonValue(static_cast<ui64>(1))));
            }

            if (!domain.Has("scheme_root")) {
                domain.InsertValue("scheme_root", std::move(NJson::TJsonValue(72075186232723360)));
            }

            if (!domain.Has("plan_resolution")){
                domain.InsertValue("plan_resolution", std::move(NJson::TJsonValue(10)));
            }

            Y_ENSURE(domain.Has("plan_resolution"));

            if (!domain.Has("hive_uid")) {
                auto& hiveUids = domain["hive_uid"];
                hiveUids.SetType(NJson::EJsonValueType::JSON_ARRAY);
                hiveUids.AppendValue(NJson::TJsonValue(1));
            }

            if (!domain.Has("ssid") ) {
                auto& ssids = domain["ssid"];
                ssids.SetType(NJson::EJsonValueType::JSON_ARRAY);
                ssids.AppendValue(NJson::TJsonValue(1));
            }

            const std::vector<std::pair<TString, TString>> exps = {{"explicit_coordinators", "FLAT_TX_COORDINATOR"}, {"explicit_allocators", "TX_ALLOCATOR"}, {"explicit_mediators", "TX_MEDIATOR"}};
            for(auto [field, type] : exps) {
                Y_ENSURE(!domain.Has(field));
                auto& arr = domain[field];
                arr.SetType(NJson::EJsonValueType::JSON_ARRAY);
                for(auto tablet: GetTabletsFor(json, type)) {
                    arr.AppendValue(tablet["info"]["tablet_id"].GetUIntegerSafe());
                }
            }
        }
    }

    void PrepareNameserviceConfig(NJson::TJsonValue& json) {
        if (json.Has("nameservice_config")) {
            Y_ENSURE(json["nameservice_config"].IsMap());
            if (json["nameservice_config"].Has("node")) {
                return;
            }
        }

        if (!json.Has("hosts")) {
            return;
        }

        if (!json.Has("nameservice_config")) {
            auto& f = json["nameservice_config"];
            f.SetType(NJson::EJsonValueType::JSON_MAP);
        }

        auto& config = json["nameservice_config"];
        if (!config.Has("node")) {
            auto& nodes = config["node"];
            nodes.SetType(NJson::EJsonValueType::JSON_ARRAY);
        }

        auto& nodes = config["node"];

        Y_ENSURE(json["hosts"].IsArray());
        ui32 nodeID = 0;

        for(auto& host : json["hosts"].GetArraySafe()) {
            nodeID++;

            if (!host.Has("node_id")) {
                host.InsertValue("node_id", nodeID);
            }

            if (!host.Has("port")) {
                // default interconnect port
                host.InsertValue("port", 19001);
            }

            NJson::TJsonValue hostCopy = host;
            if (hostCopy.Has("host_config_id")) {
                hostCopy.EraseValue("host_config_id");
            }

            nodes.AppendValue(hostCopy);
        }
    }

    void ClearFields(NJson::TJsonValue& json){
        json.EraseValue("system_tablets");
        json.EraseValue("static_erasure");
        json.EraseValue("hosts");
        json.EraseValue("host_configs");
        json.EraseValue("storage_config_generation");
    }

    void PrepareLogConfig(NJson::TJsonValue& json) {
        if (!json.Has("log_config")) {
            json["log_config"].SetType(NJson::EJsonValueType::JSON_MAP);
        }

        if (!json["log_config"].Has("default_level")) {
            json["log_config"].InsertValue("default_level", std::move(NJson::TJsonValue(5)));
        }
    }

    void PrepareIcConfig(NJson::TJsonValue& json) {
        if (!json.Has("interconnect_config")) {
            auto& config = json["interconnect_config"];
            config.SetType(NJson::EJsonValueType::JSON_MAP);
        }

        if (!json["interconnect_config"].Has("start_tcp")) {
            auto& config = json["interconnect_config"];
            config.InsertValue("start_tcp", std::move(NJson::TJsonValue(true)));
        }
    }

    void PrepareJson(NJson::TJsonValue& json){
        PrepareNameserviceConfig(json);
        PrepareActorSystemConfig(json);
        PrepareStaticGroup(json);
        PrepareIcConfig(json);
        PrepareLogConfig(json);
        PrepareSystemTabletsInfo(json);
        PrepareDomainsConfig(json);
        PrepareBootstrapConfig(json);
        ClearFields(json);
    }

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue json = Yaml2Json(yamlNode, true);
        Y_ENSURE(json.Has("hosts"));
        PrepareNameserviceConfig(json);

        NKikimrBlobStorage::TConfigRequest result;

        for(auto hostConfig: json["host_configs"].GetArraySafe()) {
            auto *hostConfigProto = result.AddCommand()->MutableDefineHostConfig();
            NProtobufJson::MergeJson2Proto(hostConfig, *hostConfigProto, GetJsonToProtoConfig());
        }

        auto *defineBox = result.AddCommand()->MutableDefineBox();
        defineBox->SetBoxId(1);
        if (json.Has("storage_config_generation")) {
            defineBox->SetItemConfigGeneration(json["storage_config_generation"].GetUIntegerSafe());
        } else {
            defineBox->SetItemConfigGeneration(0);
        }

        Y_ENSURE(json["hosts"].IsArray());
        Y_ENSURE(json["host_configs"].IsArray());

        for(auto jsonHost: json["hosts"].GetArraySafe()) {
            auto* host = defineBox->AddHost();
            host->MutableKey()->SetNodeId(FindNodeId(json, jsonHost["node_id"]));
            host->MutableKey()->SetFqdn(jsonHost["host"].GetStringSafe());
            host->MutableKey()->SetIcPort(jsonHost["port"].GetUIntegerSafe());
            host->SetHostConfigId(jsonHost["host_config_id"].GetUIntegerSafe());
        }

        return result;
    }

    void Parse(const TString& data, NKikimrConfig::TAppConfig& config) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue jsonNode = Yaml2Json(yamlNode, true);
        PrepareJson(jsonNode);
        NProtobufJson::MergeJson2Proto(jsonNode, config, GetJsonToProtoConfig());
    }
}
