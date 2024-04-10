#include "yaml_config_parser.h"

#include <util/generic/string.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/erasure/erasure.h>

#include <library/cpp/json/writer/json.h>

namespace NKikimr::NYaml::NDeprecated {

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

    static const TString& GetStringSafe(const NJson::TJsonValue& json, const TStringBuf& key) {
        auto& value = json[key];
        Y_ENSURE_BT(value.IsString(), "Unexpected value for key: " << key << ", expected string value.");
        return value.GetStringSafe();
    }

    static ui64 GetUnsignedIntegerSafe(const NJson::TJsonValue& json, const TStringBuf& key) {
        Y_ENSURE_BT(json.Has(key), "Value is not set for key: " << key);
        auto& value = json[key];
        Y_ENSURE_BT(value.IsUInteger(), "Unexpected value for key: " << key << ", expected integer value.");
        return value.GetUIntegerSafe();
    }

    void EnsureJsonFieldIsArray(const NJson::TJsonValue& json, const TStringBuf& key) {
        Y_ENSURE_BT(json.Has(key) && json[key].IsArray(), "Array field `" << key << "` must be specified.");
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
        const TString& erasureName = GetStringSafe(json, "static_erasure");
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
            {"FLAT_TX_COORDINATOR", 3},
            {"TX_MEDIATOR", 3},
            {"TX_ALLOCATOR", 3},
            {"CMS", 1},
            {"NODE_BROKER", 1},
            {"TENANT_SLOT_BROKER", 1},
            {"CONSOLE", 1},
        };
    }

    ui32 GetDefaultTabletCount(TString& type) {
        auto defaults = GetDefaultTablets();
        for(auto [type_, cnt] : defaults) {
            if (type == type_) {
                return cnt;
            }
        }
        Y_ENSURE_BT(false, "unknown tablet " << type);
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
        TString hostname = GetStringSafe(host, "host");
        ui32 InterconnectPort = 19001;
        if (host.Has("port")) {
            InterconnectPort = GetUnsignedIntegerSafe(host, "port");
        }

        return TStringBuilder() << hostname << ":" << InterconnectPort;
    }

    NJson::TJsonValue FindNodeByString(NJson::TJsonValue& json, const TString& data) {
        ui32 foundCandidates = 0;
        NJson::TJsonValue result;
        for(auto& host: json["nameservice_config"]["node"].GetArraySafe()) {
            if (data == GetStringSafe(host, "host")) {
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

        Y_ENSURE_BT(foundCandidates == 1, "Cannot find node_id for " << data);
        return result;
    }

    ui32 FindNodeId(NJson::TJsonValue& json, NJson::TJsonValue& host) {
        if (host.IsUInteger()) {
            return host.GetUIntegerSafe();
        }

        Y_ENSURE_BT(host.IsString(), "host value must be either integer or string.");
        auto node = FindNodeByString(json, host.GetStringSafe());
        return GetUnsignedIntegerSafe(node, "node_id");
    }

    ui64 GetNextTabletID(TString& type, ui32 idx) {
        std::unordered_map<TString, ui64> tablets = {
            {"FLAT_HIVE", 72057594037968897},
            {"FLAT_BS_CONTROLLER", 72057594037932033},
            {"FLAT_SCHEMESHARD", 72057594046678944},
            {"CMS", 72057594037936128},
            {"NODE_BROKER", 72057594037936129},
            {"TENANT_SLOT_BROKER", 72057594037936130},
            {"CONSOLE", 72057594037936131},
            {"TX_ALLOCATOR", TDomainsInfo::MakeTxAllocatorIDFixed(idx)},
            {"FLAT_TX_COORDINATOR", TDomainsInfo::MakeTxCoordinatorIDFixed(idx)},
            {"TX_MEDIATOR", TDomainsInfo::MakeTxMediatorIDFixed(idx)},
        };

        auto it = tablets.find(type);
        Y_ENSURE_BT(it != tablets.end());
        return it->second;
    }

    const NJson::TJsonArray::TArray& GetTabletIdsFor(NJson::TJsonValue& json, TString type) {
        auto& systemTabletsConfig = json["system_tablets"];
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

            NJson::TJsonValue& tabletInfo = tablet["info"];

            if (!tabletInfo.Has("tablet_id")) {
                Y_ENSURE_BT(idx <= GetDefaultTabletCount(type));
                tabletInfo.InsertValue("tablet_id", NJson::TJsonValue(GetNextTabletID(type, idx)));
            }
        }

        return json["system_tablets"][toLowerType].GetArraySafe();
    }

    const NJson::TJsonArray::TArray& GetTabletsFor(NJson::TJsonValue& json, TString type) {
        auto& systemTabletsConfig = json["system_tablets"];
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

            EnsureJsonFieldIsArray(tablet, "node");

            NJson::TJsonValue& tabletInfo = tablet["info"];

            if (!tabletInfo.Has("tablet_id")) {
                Y_ENSURE_BT(idx <= GetDefaultTabletCount(type));
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

        if (config.Has("use_auto_config") && config["use_auto_config"].GetBooleanSafe()) {
            return; // do nothing for auto config
        }

        auto& executors = config["executor"];

        const std::vector<std::pair<TString, TString>> defaultExecutors = {{"io_executor", "IO"}, {"sys_executor", "SYSTEM"}, {"user_executor", "USER"}, {"batch_executor", "BATCH"}};
        for(const auto& [fieldName, name]: defaultExecutors) {
            if (!config.Has(fieldName)) {
                ui32 executorID = 0;
                for(const auto& executor: executors.GetArraySafe()) {
                    if (to_lower(GetStringSafe(executor, "name")) == to_lower(name)) {
                        config.InsertValue(fieldName, executorID);
                        break;
                    }

                    ++executorID;
                }
            }

            Y_ENSURE_BT(config.Has(fieldName), "cannot deduce executor id for " << fieldName);
        }

        if (!config.Has("service_executor")) {
            auto& se = config["service_executor"];
            se.SetType(NJson::EJsonValueType::JSON_ARRAY);
            ui32 executorID = 0;

            for(const auto& executor: executors.GetArraySafe()) {
                if (to_lower(GetStringSafe(executor, "name")) == "ic") {
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

        Y_ENSURE_BT(false, "unknown pdisk category " << data);
    }

    ui32 ErasureStrToNum(const TString& info) {
        TErasureType::EErasureSpecies species = TErasureType::ErasureSpeciesByName(info);
        Y_ENSURE_BT(species != TErasureType::ErasureSpeciesCount, "unknown erasure " << info);
        return species;
    }

    void PrepareStaticGroup(NJson::TJsonValue& json) {
        if (!json.Has("blob_storage_config")) {
            return;
        }

        auto& config = json["blob_storage_config"];
        Y_ENSURE_BT(config["service_set"].IsMap(), "service_set field in blob_storage_config must be json map.");

        auto& serviceSet = config["service_set"];
        if (!serviceSet.Has("availability_domains")) {
            NJson::TJsonValue arr;
            arr.SetType(NJson::EJsonValueType::JSON_ARRAY);
            arr.AppendValue(NJson::TJsonValue(1));
            serviceSet.InsertValue("availability_domains", arr);
        }

        if (serviceSet.Has("groups")) {
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

                ui32 groupID = GetUnsignedIntegerSafe(group, "group_id");
                ui32 groupGeneration = GetUnsignedIntegerSafe(group, "group_generation");
                Y_ENSURE_BT(group.Has("erasure_species"), "erasure species are not specified for group, id " << groupID);
                if (group["erasure_species"].IsString()) {
                    auto species = GetStringSafe(group, "erasure_species");
                    ui32 num = ErasureStrToNum(species);
                    group.EraseValue("erasure_species");
                    group.InsertValue("erasure_species", NJson::TJsonValue(num));
                }

                EnsureJsonFieldIsArray(group, "rings");

                auto& ringsInfo = group["rings"].GetArraySafe();

                ui32 ringID = 0;
                std::unordered_map<ui32, std::unordered_set<ui32>> UniquePdiskIds;
                std::unordered_map<ui32, std::unordered_set<ui32>> UniquePdiskGuids;

                for(auto& ring: ringsInfo) {
                    EnsureJsonFieldIsArray(ring, "fail_domains");

                    auto& failDomains = ring["fail_domains"].GetArraySafe();
                    ui32 failDomainID = 0;
                    for(auto& failDomain: failDomains) {
                        EnsureJsonFieldIsArray(failDomain, "vdisk_locations");
                        Y_ENSURE_BT(failDomain["vdisk_locations"].GetArraySafe().size() == 1);

                        for(auto& vdiskLocation: failDomain["vdisk_locations"].GetArraySafe()) {
                            Y_ENSURE_BT(vdiskLocation.Has("node_id"));
                            vdiskLocation.InsertValue("node_id", FindNodeId(json, vdiskLocation["node_id"]));
                            if (!vdiskLocation.Has("vdisk_slot_id")) {
                                vdiskLocation.InsertValue("vdisk_slot_id", NJson::TJsonValue(0));
                            }

                            ui64 myNodeId = GetUnsignedIntegerSafe(vdiskLocation, "node_id");
                            if (!vdiskLocation.Has("pdisk_guid")) {
                                for(ui32 pdiskGuid = 1; ; pdiskGuid++) {
                                    if (UniquePdiskGuids[myNodeId].find(pdiskGuid) == UniquePdiskGuids[myNodeId].end()) {
                                        vdiskLocation.InsertValue("pdisk_guid",  NJson::TJsonValue(pdiskGuid));
                                        break;
                                    }
                                }
                            }

                            {
                                ui64 guid = GetUnsignedIntegerSafe(vdiskLocation, "pdisk_guid");
                                auto [it, success] = UniquePdiskGuids[myNodeId].insert(guid);
                                Y_ENSURE_BT(success, "pdisk guids should be unique, non-unique guid is " << guid);
                            }

                            if (!vdiskLocation.Has("pdisk_id")) {
                                for(ui32 pdiskID = 1; ; pdiskID++) {
                                    if (UniquePdiskIds[myNodeId].find(pdiskID) == UniquePdiskIds[myNodeId].end()) {
                                        vdiskLocation.InsertValue("pdisk_id",  NJson::TJsonValue(pdiskID));
                                        break;
                                    }
                                }
                            }

                            {
                                ui64 pdiskId = GetUnsignedIntegerSafe(vdiskLocation, "pdisk_id");
                                auto [it, success] = UniquePdiskIds[myNodeId].insert(pdiskId);
                                Y_ENSURE_BT(success, "pdisk ids should be unique, non unique pdisk_id : " << pdiskId);
                            }

                            if (shouldFillPdisks) {
                                NJson::TJsonValue pdiskInfo = vdiskLocation;
                                if (pdiskInfo.Has("vdisk_slot_id")) {
                                    pdiskInfo.EraseValue("vdisk_slot_id");
                                }

                                if (pdiskInfo.Has("pdisk_category")) {
                                    if (pdiskInfo["pdisk_category"].IsString()) {
                                        auto cat = GetStringSafe(pdiskInfo, "pdisk_category");
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

                            if (vdiskLocation.Has("pdisk_config")) {
                                vdiskLocation.EraseValue("pdisk_config");
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
                                myVdisk.InsertValue("vdisk_kind", NJson::TJsonValue("Default"));
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
    }

    void PrepareSystemTabletsInfo(NJson::TJsonValue& json, bool relaxed)  {
        if (relaxed && (!json.Has("nameservice_config") || !json["nameservice_config"].Has("node"))) {
            return;
        }

        if (!json.Has("system_tablets")) {
            auto& config = json["system_tablets"];
            config.SetType(NJson::EJsonValueType::JSON_MAP);
        }

        if (!json["system_tablets"].Has("default_node")) {
            Y_ENSURE_BT(json["nameservice_config"]["node"].IsArray());

            auto& config = json["system_tablets"]["default_node"];
            config.SetType(NJson::EJsonValueType::JSON_ARRAY);
            for(auto node: json["nameservice_config"]["node"].GetArraySafe()) {
                Y_ENSURE_BT(node["node_id"].IsUInteger(), "node_id must be specified");
                auto nodeId = node["node_id"];
                config.AppendValue(nodeId);
            }
        }

    }

    void PrepareBootstrapConfig(NJson::TJsonValue& json, bool relaxed) {
        if (json.Has("bootstrap_config") && json["bootstrap_config"].Has("tablet")) {
            return;
        }

        if (relaxed && (!json.Has("system_tablets") || !json.Has("static_erasure"))) {
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

    void PrepareDomainsConfig(NJson::TJsonValue& json, bool relaxed) {
        if (relaxed && !json.Has("domains_config")) {
            return;
        }

        Y_ENSURE_BT(json.Has("domains_config"));
        Y_ENSURE_BT(json["domains_config"].IsMap());
        NJson::TJsonValue& domainsConfig = json["domains_config"];

        Y_ENSURE_BT(domainsConfig.Has("domain"));
        NJson::TJsonValue& domains = domainsConfig["domain"];

        Y_ENSURE_BT(domains.GetArraySafe().size() == 1);

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
            Y_ENSURE_BT(domain.Has("name"));

            if (domain.Has("domain_id")) {
                Y_ENSURE_BT(GetUnsignedIntegerSafe(domain, "domain_id") == 1);
            } else {
                domain.InsertValue("domain_id", std::move(NJson::TJsonValue(static_cast<ui64>(1))));
            }

            if (!domain.Has("scheme_root")) {
                domain.InsertValue("scheme_root", std::move(NJson::TJsonValue(72057594046678944)));
            }

            if (!domain.Has("plan_resolution")){
                domain.InsertValue("plan_resolution", std::move(NJson::TJsonValue(10)));
            }

            Y_ENSURE_BT(domain.Has("plan_resolution"));

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
                if (relaxed && domain.Has(field)) {
                    continue;
                }
                Y_ENSURE_BT(!domain.Has(field));
                auto& arr = domain[field];
                arr.SetType(NJson::EJsonValueType::JSON_ARRAY);
                for(auto tablet: GetTabletIdsFor(json, type)) {
                    arr.AppendValue(GetUnsignedIntegerSafe(tablet["info"], "tablet_id"));
                }
            }
        }
    }

    void PrepareSecurityConfig(NJson::TJsonValue& json, bool relaxed) {
        if (relaxed && !json.Has("domains_config")) {
            return;
        }

        Y_ENSURE_BT(json.Has("domains_config"));
        Y_ENSURE_BT(json["domains_config"].IsMap());

        bool disabledDefaultSecurity = false;
        NJson::TJsonValue& domainsConfig = json["domains_config"];
        if (domainsConfig.Has("disable_builtin_security")) {
            disabledDefaultSecurity = domainsConfig["disable_builtin_security"].GetBooleanSafe();
            domainsConfig.EraseValue("disable_builtin_security");
        }

        NJson::TJsonValue& securityConfig = domainsConfig["security_config"];
        TString defaultUserName;

        if (securityConfig.Has("default_users")) {
            NJson::TJsonValue& defaultUsers = securityConfig["default_users"];
            Y_ENSURE_BT(defaultUsers.IsArray());
            Y_ENSURE_BT(defaultUsers.GetArraySafe().size() > 0);
            NJson::TJsonValue& defaultUser = defaultUsers.GetArraySafe()[0];
            Y_ENSURE_BT(defaultUser.IsMap());
            defaultUserName = defaultUser["name"].GetStringRobust();
        } else if (!disabledDefaultSecurity) {
            NJson::TJsonValue& defaultUser = securityConfig["default_users"].AppendValue({});
            defaultUser["name"] = defaultUserName = "root";
            defaultUser["password"] = "";
        }

        if (!securityConfig.Has("default_groups") && !disabledDefaultSecurity) {
            NJson::TJsonValue& defaultGroups = securityConfig["default_groups"];

            {
                NJson::TJsonValue& defaultGroupAdmins = defaultGroups.AppendValue({});
                defaultGroupAdmins["name"] = "ADMINS";
                defaultGroupAdmins["members"].AppendValue(defaultUserName);
            }

            {
                NJson::TJsonValue& defaultGroupDatabaseAdmins = defaultGroups.AppendValue({});
                defaultGroupDatabaseAdmins["name"] = "DATABASE-ADMINS";
                defaultGroupDatabaseAdmins["members"].AppendValue("ADMINS");
            }

            {
                NJson::TJsonValue& defaultGroupAccessAdmins = defaultGroups.AppendValue({});
                defaultGroupAccessAdmins["name"] = "ACCESS-ADMINS";
                defaultGroupAccessAdmins["members"].AppendValue("DATABASE-ADMINS");
            }

            {
                NJson::TJsonValue& defaultGroupDdlAdmins = defaultGroups.AppendValue({});
                defaultGroupDdlAdmins["name"] = "DDL-ADMINS";
                defaultGroupDdlAdmins["members"].AppendValue("DATABASE-ADMINS");
            }

            {
                NJson::TJsonValue& defaultGroupDataWriters = defaultGroups.AppendValue({});
                defaultGroupDataWriters["name"] = "DATA-WRITERS";
                defaultGroupDataWriters["members"].AppendValue("ADMINS");
            }

            {
                NJson::TJsonValue& defaultGroupDataReaders = defaultGroups.AppendValue({});
                defaultGroupDataReaders["name"] = "DATA-READERS";
                defaultGroupDataReaders["members"].AppendValue("DATA-WRITERS");
            }

            {
                NJson::TJsonValue& defaultGroupMetadataReaders = defaultGroups.AppendValue({});
                defaultGroupMetadataReaders["name"] = "METADATA-READERS";
                defaultGroupMetadataReaders["members"].AppendValue("DATA-READERS");
                defaultGroupMetadataReaders["members"].AppendValue("DDL-ADMINS");
            }

            {
                NJson::TJsonValue& defaultGroupUsers = defaultGroups.AppendValue({});
                defaultGroupUsers["name"] = "USERS";
                defaultGroupUsers["members"].AppendValue("METADATA-READERS");
                defaultGroupUsers["members"].AppendValue("DATA-READERS");
                defaultGroupUsers["members"].AppendValue("DATA-WRITERS");
                defaultGroupUsers["members"].AppendValue("DDL-ADMINS");
                defaultGroupUsers["members"].AppendValue("ACCESS-ADMINS");
                defaultGroupUsers["members"].AppendValue("DATABASE-ADMINS");
                defaultGroupUsers["members"].AppendValue("ADMINS");
                defaultGroupUsers["members"].AppendValue(defaultUserName);
            }
        }

        if (!securityConfig.Has("all_users_group") && !disabledDefaultSecurity) {
            securityConfig["all_users_group"] = "USERS";
        }

        if (!securityConfig.Has("default_access") && !disabledDefaultSecurity) {
            NJson::TJsonValue& defaultAccess = securityConfig["default_access"];
            defaultAccess.AppendValue("+(ConnDB):USERS"); // ConnectDatabase
            defaultAccess.AppendValue("+(DS|RA):METADATA-READERS"); // DescribeSchema | ReadAttributes
            defaultAccess.AppendValue("+(SR):DATA-READERS"); // SelectRow
            defaultAccess.AppendValue("+(UR|ER):DATA-WRITERS"); // UpdateRow | EraseRow
            defaultAccess.AppendValue("+(CD|CT|WA|AS|RS):DDL-ADMINS"); // CreateDirectory | CreateTable | WriteAttributes | AlterSchema | RemoveSchema
            defaultAccess.AppendValue("+(GAR):ACCESS-ADMINS"); // GrantAccessRights
            defaultAccess.AppendValue("+(CDB|DDB):DATABASE-ADMINS"); // CreateDatabase | DropDatabase
        }
    }

    void PrepareNameserviceConfig(NJson::TJsonValue& json) {
        if (json.Has("nameservice_config")) {
            Y_ENSURE_BT(json["nameservice_config"].IsMap());
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

        EnsureJsonFieldIsArray(json, "hosts");
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

            if (!hostCopy.Has("interconnect_host")) {
                auto hostjs = hostCopy["host"];
                hostCopy.InsertValue("interconnect_host", hostjs);
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

    void PrepareBlobStorageConfig(NJson::TJsonValue& json) {
        if (!json.Has("blob_storage_config")) {
            return;
        }
        auto& blobStorageConfig = json["blob_storage_config"];

        if (!blobStorageConfig.Has("autoconfig_settings")) {
            return;
        }
        auto& autoconfigSettings = blobStorageConfig["autoconfig_settings"];

        autoconfigSettings.EraseValue("define_host_config");
        autoconfigSettings.EraseValue("define_box");

        if (json.Has("host_configs")) {
            auto& array = autoconfigSettings.InsertValue("define_host_config", NJson::JSON_ARRAY);
            for (const auto& hostConfig : json["host_configs"].GetArraySafe()) {
                array.AppendValue(NJson::TJsonValue(hostConfig));
            }
        }

        THashMap<std::tuple<TString, ui32>, ui32> hostNodeMap;
        Y_ENSURE_BT(json.Has("nameservice_config"));
        const auto& nameserviceConfig = json["nameservice_config"];
        Y_ENSURE_BT(nameserviceConfig.Has("node"));
        for (const auto& item : nameserviceConfig["node"].GetArraySafe()) {
            const auto key = std::make_tuple(item["interconnect_host"].GetStringSafe(), item["port"].GetUIntegerSafe());
            hostNodeMap[key] = item["node_id"].GetUIntegerSafe();
        }

        NJson::TJsonValue *defineBox = nullptr;

        if (!json.Has("hosts")) {
            return;
        }
        for (const auto& host : json["hosts"].GetArraySafe()) {
            if (host.Has("host_config_id")) {
                if (!defineBox) {
                    defineBox = &autoconfigSettings.InsertValue("define_box", NJson::TJsonMap{
                        {"box_id", 1},
                        {"host", NJson::TJsonArray{}},
                    });
                }

                const TString fqdn = host["interconnect_host"].GetStringSafe(host["host"].GetStringSafe());
                const ui32 port = host["port"].GetUIntegerSafe(19001);
                const auto key = std::make_tuple(fqdn, port);
                Y_ENSURE_BT(hostNodeMap.contains(key));

                (*defineBox)["host"].AppendValue(NJson::TJsonMap{
                    {"host_config_id", host["host_config_id"].GetUIntegerSafe()},
                    {"enforced_node_id", hostNodeMap[key]},
                });
            }
        }
    }

    void TransformConfig(NJson::TJsonValue& json, bool relaxed) {
        PrepareNameserviceConfig(json);
        PrepareActorSystemConfig(json);
        PrepareStaticGroup(json);
        PrepareBlobStorageConfig(json);
        PrepareIcConfig(json);
        PrepareLogConfig(json);
        PrepareSystemTabletsInfo(json, relaxed);
        PrepareDomainsConfig(json, relaxed);
        PrepareSecurityConfig(json, relaxed);
        PrepareBootstrapConfig(json, relaxed);
        ClearFields(json);
    }

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue json = Yaml2Json(yamlNode, true);
        Y_ENSURE_BT(json.Has("hosts") && json["hosts"].IsArray(), "Specify hosts list to use blobstorage init command");
        Y_ENSURE_BT(json["host_configs"].IsArray(), "Specify host_configs to use blobstorage init command");

        PrepareNameserviceConfig(json);

        NKikimrBlobStorage::TConfigRequest result;

        const auto itemConfigGeneration = json.Has("storage_config_generation") ?
            GetUnsignedIntegerSafe(json, "storage_config_generation") : 0;

        for(auto hostConfig: json["host_configs"].GetArraySafe()) {
            auto *hostConfigProto = result.AddCommand()->MutableDefineHostConfig();
            NProtobufJson::MergeJson2Proto(hostConfig, *hostConfigProto, GetJsonToProtoConfig());
            // KIKIMR-16712
            // Avoid checking the version number for "host_config" configuration items.
            // This allows to add new host configuration items after the initial cluster setup.
            hostConfigProto->SetItemConfigGeneration(Max<ui64>());
        }

        auto *defineBox = result.AddCommand()->MutableDefineBox();
        defineBox->SetBoxId(1);
        defineBox->SetItemConfigGeneration(itemConfigGeneration);

        for(auto jsonHost: json["hosts"].GetArraySafe()) {
            auto* host = defineBox->AddHost();
            host->MutableKey()->SetNodeId(FindNodeId(json, jsonHost["node_id"]));
            host->MutableKey()->SetFqdn(GetStringSafe(jsonHost, "host"));
            host->MutableKey()->SetIcPort(GetUnsignedIntegerSafe(jsonHost, "port"));
            host->SetHostConfigId(GetUnsignedIntegerSafe(jsonHost, "host_config_id"));
        }

        return result;
    }

    void Parse(const TString& data, NKikimrConfig::TAppConfig& config, bool needsTransforming) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue jsonNode = Yaml2Json(yamlNode, true);
        if (needsTransforming) {
            TransformConfig(jsonNode);
        }
        NProtobufJson::MergeJson2Proto(jsonNode, config, GetJsonToProtoConfig());
    }
}
