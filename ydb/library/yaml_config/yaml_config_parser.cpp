#include "yaml_config_parser.h"
#include "yaml_config_parser_impl.h"
#include "yaml_config_helpers.h"
#include "core_constants.h"

#include <ydb/library/pdisk_io/device_type.h>
#include <ydb/library/yaml_config/protos/config.pb.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/blobstorage_pdisk_category.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/tablet.pb.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/json/util.h>

#include <util/generic/string.h>

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

    const NJson::TJsonValue::TMapType& GetMapSafe(const NJson::TJsonValue& json) {
        try {
            return json.GetMapSafe();
        } catch(const NJson::TJsonException&) {
            ythrow TWithBackTrace<yexception>() << "not a map";
        }
    }

    TString GetStringRobust(const NJson::TJsonValue& json, const TStringBuf& key) {
        Y_ENSURE_BT(json.Has(key), "Value is not set for key: " << key);
        auto& value = GetMapSafe(json).at(key);
        return value.GetStringRobust();
    }

    void EnsureJsonFieldIsArray(const NJson::TJsonValue& json, const TStringBuf& key) {
        Y_ENSURE_BT(json.Has(key) && GetMapSafe(json).at(key).IsArray(), "Array field `" << key << "` must be specified.");
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

    std::optional<bool> GetBoolByPathOrNone(const NJson::TJsonValue& json, const TStringBuf& path) {
        if (auto* elem = Traverse(json, path); elem != nullptr && elem->IsBoolean()) {
            return elem->GetBoolean();
        }
        return {};
    }

    std::optional<bool> CheckExplicitEmptyArrayByPathOrNone(const NJson::TJsonValue& json, const TStringBuf& path) {
        if (auto* elem = Traverse(json, path); elem != nullptr && elem->IsArray()) {
            return elem->GetArray().size();
        }
        return {};
    }

    void EraseByPath(NJson::TJsonValue& json, const TStringBuf& path) {
        TString lastName;
        if (auto* elem = Traverse(json, path, &lastName)) {
            if (elem->Has(lastName)) {
                elem->EraseValue(lastName);
            }
        }
    }

    void EraseMultipleByPath(NJson::TJsonValue& json, const TStringBuf& path) {
        IterateMut(json, path, [](const std::vector<ui32>&, NJson::TJsonValue& node) {
            Y_ENSURE_BT(node.IsMap());
            NJson::TJsonValue value;
            value.SetType(NJson::EJsonValueType::JSON_MAP);
            node = value;
        });
    }

    void EraseMultipleByPath(NJson::TJsonValue& json, const TStringBuf& path, const TStringBuf& name) {
        IterateMut(json, path, [&name](const std::vector<ui32>&, NJson::TJsonValue& node) {
            Y_ENSURE_BT(node.IsMap());
            node.EraseValue(name);
        });
    }

    /**
    * Config used to convert protobuf from/to json
    * changes how names are translated e.g. PDiskInfo -> pdisk_info instead of p_disk_info
    */
    NProtobufJson::TJson2ProtoConfig GetJsonToProtoConfig(
        bool allowUnknown,
        TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector)
    {
        NProtobufJson::TJson2ProtoConfig config;
        config.SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense);
        config.SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive);
        config.CastRobust = true;
        config.MapAsObject = true;
        config.AllowUnknownFields = allowUnknown;
        config.UnknownFieldsCollector = std::move(unknownFieldsCollector);
        return config;
    }

    void ExtractExtraFields(NJson::TJsonValue& json, TTransformContext& ctx) {
        // for static group
        Iterate(json, COMBINED_DISK_INFO_PATH, [&ctx](const std::vector<ui32>& ids, const NJson::TJsonValue& node) {
            Y_ENSURE_BT(ids.size() == 4);
            NKikimrConfig::TCombinedDiskInfo info;
            NProtobufJson::MergeJson2Proto(node, info, GetJsonToProtoConfig());
            TCombinedDiskInfoKey key{
                .Group = ids[0],
                .Ring = ids[1],
                .FailDomain = ids[2],
                .VDiskLocation = ids[3],
            };
            ctx.CombinedDiskInfo[key] = info;
        });
        EraseMultipleByPath(json, COMBINED_DISK_INFO_PATH);

        Iterate(json, POOL_CONFIG_PATH, [&ctx](const std::vector<ui32>& ids, const NJson::TJsonValue& node) {
            Y_ENSURE_BT(ids.size() == 2);

            TPoolConfigKey key{
                .Domain = ids[0],
                .StoragePoolType = ids[1],
            };

            ctx.PoolConfigInfo[key] = TPoolConfigInfo{
                .HasErasureSpecies = node.Has("erasure_species"),
                .HasKind = node.Has("kind"),
                .HasVDiskKind = node.Has("vdisk_kind"),
            };
        });

        Iterate(json, GROUP_PATH, [&ctx](const std::vector<ui32>& ids, const NJson::TJsonValue& node) {
            Y_ENSURE_BT(ids.size() == 1);
            Y_ENSURE_BT(node.IsMap());
            if (node.Has(ERASURE_SPECIES_FIELD)) {
                ctx.GroupErasureSpecies[ids[0]] = GetStringRobust(node, ERASURE_SPECIES_FIELD);
            }
        });
        EraseMultipleByPath(json, GROUP_PATH, ERASURE_SPECIES_FIELD);
        // for security config
        ctx.DisableBuiltinSecurity = GetBoolByPathOrNone(json, DISABLE_BUILTIN_SECURITY_PATH).value_or(false);
        EraseByPath(json, DISABLE_BUILTIN_SECURITY_PATH);
        ctx.ExplicitEmptyDefaultGroups = CheckExplicitEmptyArrayByPathOrNone(json, DEFAULT_GROUPS_PATH).value_or(false);
        ctx.ExplicitEmptyDefaultAccess = CheckExplicitEmptyArrayByPathOrNone(json, DEFAULT_ACCESS_PATH).value_or(false);
    }

    ui32 GetDefaultTabletCount(TString& type) {
        const auto& defaults = DEFAULT_TABLETS;
        for(const auto& [type_, cnt] : defaults) {
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
        const auto& defaults = DEFAULT_TABLETS;
        std::vector<TString> types;
        for(const auto& [type, cnt] : defaults) {
            types.push_back(TString(type));
        }
        return types;
    }

    ui64 GetNextTabletID(TString& type, ui32 idx) {
        const auto& tablets = GetTablets(idx);
        auto it = tablets.find(type);
        Y_ENSURE_BT(it != tablets.end());
        return it->second;
    }

    ui64 PdiskCategoryFromString(const TString& data) {
        ui64 pDiskCategory = 0;
        if (TryFromString(data, pDiskCategory)) {
            return pDiskCategory;
        }

        if (data == "ROT") {
            return TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw();
        } else if (data == "SSD") {
            return TPDiskCategory(NPDisk::DEVICE_TYPE_SSD, 0).GetRaw();
        } else if (data == "NVME") {
            return TPDiskCategory(NPDisk::DEVICE_TYPE_NVME, 0).GetRaw();
        }

        Y_ENSURE_BT(false, "unknown pdisk category " << data);
    }

    ui32 ErasureStrToNum(const TString& info) {
        ui32 result = 0;
        if (TryFromString(info, result)) {
            return result;
        }
        TErasureType::EErasureSpecies species = TErasureType::ErasureSpeciesByName(info);
        Y_ENSURE_BT(species != TErasureType::ErasureSpeciesCount, "unknown erasure " << info);
        return species;
    }

    TVector<TString> ListEphemeralFields() {
        TVector<TString> result;

        auto& inst = NKikimrConfig::TEphemeralInputFields::default_instance();
        const auto* descriptor = inst.GetDescriptor();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* fieldDescriptor = descriptor->field(i);
            result.push_back(fieldDescriptor->name());
        }
        for (int i = 0; i < descriptor->reserved_name_count(); ++i) {
            result.push_back(descriptor->reserved_name(i));
        }

        for (auto& str : result) {
            NProtobufJson::ToSnakeCaseDense(&str);
        }

        return result;
    }

    TVector<TString> ListNonEphemeralFields() {
        TVector<TString> result;

        auto& inst = NKikimrConfig::TAppConfig::default_instance();
        const auto* descriptor = inst.GetDescriptor();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* fieldDescriptor = descriptor->field(i);
            result.push_back(fieldDescriptor->name());
        }
        for (int i = 0; i < descriptor->reserved_name_count(); ++i) {
            result.push_back(descriptor->reserved_name(i));
        }

        for (auto& str : result) {
            NProtobufJson::ToSnakeCaseDense(&str);
        }

        return result;
    }

    void ClearNonEphemeralFields(NJson::TJsonValue& json){
        for (const auto& field : ListNonEphemeralFields()) {
            json.EraseValue(field);
        }
    }

    void ClearEphemeralFields(NJson::TJsonValue& json){
        for (const auto& field : ListEphemeralFields()) {
            json.EraseValue(field);
        }
    }

    void PrepareActorSystemConfig(NKikimrConfig::TAppConfig& config) {
        if (!config.HasActorSystemConfig()) {
            return;
        }

        auto* asConfig = config.MutableActorSystemConfig();

        if (asConfig->GetUseAutoConfig()) {
            return; // do nothing for auto config
        }

        auto* executors = asConfig->MutableExecutor();

        const auto& defaultExecutors = DEFAULT_EXECUTORS;
        const auto* descriptor = asConfig->GetDescriptor();
        const auto* reflection = asConfig->GetReflection();
        std::vector<const NProtoBuf::FieldDescriptor *> fields;
        reflection->ListFields(*asConfig, &fields);
        std::map<TString, const NProtoBuf::FieldDescriptor *> fieldsByName;
        for (auto* field : fields) {
            fieldsByName[field->name()] = field;
        }
        for(const auto& [fieldName, name]: defaultExecutors) {
            if (!fieldsByName.contains(fieldName)) {
                ui32 executorID = 0;
                for(const auto& executor: *executors) {
                    if (to_lower(executor.GetName()) == to_lower(name)) {
                        if (auto* fieldDescriptor = descriptor->FindFieldByName(fieldName)) {
                            reflection->SetUInt32(asConfig, fieldDescriptor, executorID);
                        } else {
                            Y_ENSURE_BT(false, "unknown executor " << fieldName);
                        }
                        break;
                    }

                    ++executorID;
                }
            }
        }

        fields.clear();
        reflection->ListFields(*asConfig, &fields);
        fieldsByName.clear();
        for (auto* field : fields) {
            fieldsByName[field->name()] = field;
        }
        for(const auto& [fieldName, name]: defaultExecutors) {
            Y_ENSURE_BT(fieldsByName.contains(fieldName), "cannot deduce executor id for " << fieldName);
        }

        if (!asConfig->ServiceExecutorSize()) {
            ui32 executorID = 0;
            for(const auto& executor: *executors) {
                if (to_lower(executor.GetName()) == "ic") {
                    auto* se = asConfig->AddServiceExecutor();
                    se->SetServiceName("Interconnect");
                    se->SetExecutorId(executorID);
                }

                ++executorID;
            }
        }
    }

    void PrepareLogConfig(NKikimrConfig::TAppConfig& config) {
        auto* logConfig = config.MutableLogConfig();
        if (!logConfig->HasDefaultLevel()) {
            logConfig->SetDefaultLevel(5);
        }
    }

    void PrepareIcConfig(NKikimrConfig::TAppConfig& config, const NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        auto* icConfig = config.MutableInterconnectConfig();
        icConfig->SetStartTcp(true);

        if (ephemeralConfig.HasTls()) {
            ephemeralConfig.GetTls().CopyToTInterconnectConfig(*icConfig);
        }

        if (icConfig->HasPathToCertificateFile() &&
            icConfig->HasPathToPrivateKeyFile() &&
            icConfig->HasPathToCaFile() &&
            !icConfig->HasEncryptionMode())
        {
            icConfig->SetEncryptionMode(NKikimrConfig::TInterconnectConfig::OPTIONAL);
        }
    }

    void PrepareGrpcConfig(NKikimrConfig::TAppConfig& config, const NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        if (ephemeralConfig.HasTls()) {
            ephemeralConfig.GetTls().CopyToTGRpcConfig(*config.MutableGRpcConfig());
        }
    }

    void PrepareSecurityConfig(const TTransformContext& ctx, NKikimrConfig::TAppConfig& config, bool relaxed) {
        if (relaxed && !config.HasDomainsConfig()) {
            return;
        }

        Y_ENSURE_BT(config.HasDomainsConfig());

        auto* domainsConfig = config.MutableDomainsConfig();

        bool disabledDefaultSecurity = ctx.DisableBuiltinSecurity;

        NKikimrConfig::TDomainsConfig::TSecurityConfig* securityConfig = nullptr;
        if (domainsConfig->HasSecurityConfig()) {
            securityConfig = domainsConfig->MutableSecurityConfig();
        }

        TString defaultUserName;
        if (securityConfig && securityConfig->DefaultUsersSize() > 0) {
            const auto& defaultUser = securityConfig->GetDefaultUsers(0);
            defaultUserName = defaultUser.GetName();
        } else if (!disabledDefaultSecurity) {
            defaultUserName = TString(DEFAULT_ROOT_USERNAME);
            securityConfig = domainsConfig->MutableSecurityConfig();
            auto* user = securityConfig->AddDefaultUsers();
            user->SetName(defaultUserName);
            user->SetPassword("");
        }

        if (!ctx.ExplicitEmptyDefaultGroups && !(securityConfig && securityConfig->DefaultGroupsSize()) && !disabledDefaultSecurity) {
            securityConfig = domainsConfig->MutableSecurityConfig();
            {
                auto* defaultGroupAdmins = securityConfig->AddDefaultGroups();
                defaultGroupAdmins->SetName("ADMINS");
                defaultGroupAdmins->AddMembers(defaultUserName);
            }

            {
                auto* defaultGroupDatabaseAdmins = securityConfig->AddDefaultGroups();
                defaultGroupDatabaseAdmins->SetName("DATABASE-ADMINS");
                defaultGroupDatabaseAdmins->AddMembers("ADMINS");
            }

            {
                auto* defaultGroupAccessAdmins = securityConfig->AddDefaultGroups();
                defaultGroupAccessAdmins->SetName("ACCESS-ADMINS");
                defaultGroupAccessAdmins->AddMembers("DATABASE-ADMINS");
            }

            {
                auto* defaultGroupDdlAdmins = securityConfig->AddDefaultGroups();
                defaultGroupDdlAdmins->SetName("DDL-ADMINS");
                defaultGroupDdlAdmins->AddMembers("DATABASE-ADMINS");
            }

            {
                auto* defaultGroupDataWriters = securityConfig->AddDefaultGroups();
                defaultGroupDataWriters->SetName("DATA-WRITERS");
                defaultGroupDataWriters->AddMembers("ADMINS");
            }

            {
                auto* defaultGroupDataReaders = securityConfig->AddDefaultGroups();
                defaultGroupDataReaders->SetName("DATA-READERS");
                defaultGroupDataReaders->AddMembers("DATA-WRITERS");
            }

            {
                auto* defaultGroupMetadataReaders = securityConfig->AddDefaultGroups();
                defaultGroupMetadataReaders->SetName("METADATA-READERS");
                defaultGroupMetadataReaders->AddMembers("DATA-READERS");
                defaultGroupMetadataReaders->AddMembers("DDL-ADMINS");
            }

            {
                auto* defaultGroupUsers = securityConfig->AddDefaultGroups();
                defaultGroupUsers->SetName("USERS");
                defaultGroupUsers->AddMembers("METADATA-READERS");
                defaultGroupUsers->AddMembers("DATA-READERS");
                defaultGroupUsers->AddMembers("DATA-WRITERS");
                defaultGroupUsers->AddMembers("DDL-ADMINS");
                defaultGroupUsers->AddMembers("ACCESS-ADMINS");
                defaultGroupUsers->AddMembers("DATABASE-ADMINS");
                defaultGroupUsers->AddMembers("ADMINS");
                defaultGroupUsers->AddMembers(defaultUserName);
            }
        }

        if (!(securityConfig && securityConfig->HasAllUsersGroup()) && !disabledDefaultSecurity) {
            securityConfig = domainsConfig->MutableSecurityConfig();
            securityConfig->SetAllUsersGroup("USERS");
        }

        if (!ctx.ExplicitEmptyDefaultAccess && !(securityConfig && securityConfig->DefaultAccessSize()) && !disabledDefaultSecurity) {
            securityConfig = domainsConfig->MutableSecurityConfig();
            securityConfig->AddDefaultAccess("+(ConnDB):USERS"); // ConnectDatabase
            securityConfig->AddDefaultAccess("+(DS|RA):METADATA-READERS"); // DescribeSchema | ReadAttributes
            securityConfig->AddDefaultAccess("+(SR):DATA-READERS"); // SelectRow
            securityConfig->AddDefaultAccess("+(UR|ER):DATA-WRITERS"); // UpdateRow | EraseRow
            securityConfig->AddDefaultAccess("+(CD|CT|WA|AS|RS):DDL-ADMINS"); // CreateDirectory | CreateTable | WriteAttributes | AlterSchema | RemoveSchema
            securityConfig->AddDefaultAccess("+(GAR):ACCESS-ADMINS"); // GrantAccessRights
            securityConfig->AddDefaultAccess("+(CDB|DDB):DATABASE-ADMINS"); // CreateDatabase | DropDatabase
        }
    }

    void PrepareHosts(NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        if (!ephemeralConfig.HostsSize()) {
            return;
        }

        // TODO validate
        auto defaultDiskType = ephemeralConfig.GetDefaultDiskType();

        ui64 nextHostConfigID = 1;

        // Find the next available host_config_id
        if (ephemeralConfig.HostConfigsSize()) {
            for(const auto& hostConfig : ephemeralConfig.GetHostConfigs()) {
                if (hostConfig.HasHostConfigId()) {
                    nextHostConfigID = Max(
                        nextHostConfigID,
                        hostConfig.GetHostConfigId() + 1
                    );
                }
            }
        }

        ui32 nextBodyID = 1;

        // Extract inline drives into host_configs and find the next empty body ID
        for(auto& host : *ephemeralConfig.MutableHosts()) {
            if (host.DriveSize()) {
                host.SetHostConfigId(nextHostConfigID);
                auto* hostConfig = ephemeralConfig.AddHostConfigs();
                hostConfig->SetHostConfigId(nextHostConfigID);
                for (const auto& drive : host.GetDrive()) {
                    hostConfig->AddDrive()->CopyFrom(drive);
                }
                ++nextHostConfigID;
            }

            if (host.HasLocation() && host.GetLocation().HasBody()) {
                nextBodyID = Max(
                    nextBodyID,
                    host.GetLocation().GetBody() + 1
                );
            }

            if (host.HasWalleLocation() && host.GetWalleLocation().HasBody()) {
                nextBodyID = Max(
                    nextBodyID,
                    host.GetWalleLocation().GetBody() + 1
                );
            }
        }

        // Patch disk types
        if (ephemeralConfig.HostConfigsSize()) {
            for(auto& hostConfig : *ephemeralConfig.MutableHostConfigs()) {
                int sectorMapIndex = 0;
                for(auto& drive : *hostConfig.MutableDrive()) {
                    if (!drive.HasType()) {
                       drive.SetType(defaultDiskType);
                    }

                    if (drive.GetType() == "RAM") {
                        ++sectorMapIndex;
                        drive.SetPath(Sprintf("SectorMap:%d:64", sectorMapIndex));
                        drive.SetType("SSD");
                    }
                }
            }
        }

        // Fill locations
        for(auto& host : *ephemeralConfig.MutableHosts()) {
            if (host.HasWalleLocation()) {
                continue;
            }

            auto& location = *host.MutableLocation();

            if (!location.HasBody()) {
                location.SetBody(nextBodyID);
                ++nextBodyID;
            }

            if (!location.HasRack()) {
                location.SetRack(Sprintf("generated-rack-%u", location.GetBody()));
            }

            if (!location.HasDataCenter()) {
                location.SetDataCenter("default");
            }
        }

        ui32 nodeID = 0;
        for(auto& host : *ephemeralConfig.MutableHosts()) {
            nodeID++;

            if (!host.HasNodeId()) {
                host.SetNodeId(nodeID);
            }

            if (!host.HasPort()) {
                host.SetPort(DEFAULT_INTERCONNECT_PORT);
            }
        }
    }

    void ApplySingleNodeDefaults(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        const TString erasureName = "none";

        std::optional<TString> diskType;
        std::optional<TString> diskTypeLower;
        std::optional<TString> drivePath;

        if (ephemeralConfig.HostConfigsSize() && ephemeralConfig.GetHostConfigs(0).DriveSize()) {
            const auto& drive = ephemeralConfig.GetHostConfigs(0).GetDrive(0);
            diskType = drive.GetType();
            diskTypeLower = diskType;
            diskTypeLower->to_lower();
            drivePath = drive.GetPath();
        }

        if (!ephemeralConfig.HasStaticErasure()) {
            ephemeralConfig.SetStaticErasure(erasureName);
        }

        auto& domainsConfig = *config.MutableDomainsConfig();

        if (!domainsConfig.DomainSize()) {
            NKikimrBlobStorage::EPDiskType dtEnum;
            Y_ENSURE_BT(TryFromString<NKikimrBlobStorage::EPDiskType>(diskType.value(), dtEnum), "incorrect enum: " << diskType.value());

            auto& domain = *domainsConfig.AddDomain();
            domain.SetName("Root"); // TODO: allow override
            auto& storagePoolType =  *domain.AddStoragePoolTypes();
            storagePoolType.SetKind(diskTypeLower.value());
            auto& poolConfig = *storagePoolType.MutablePoolConfig();
            poolConfig.SetBoxId(1);
            poolConfig.SetErasureSpecies(erasureName);
            poolConfig.AddPDiskFilter()->AddProperty()->SetType(dtEnum);
            poolConfig.SetVDiskKind("Default");
        }

        if (!domainsConfig.StateStorageSize()) {
            auto& stateStorage = *domainsConfig.AddStateStorage();
            stateStorage.SetSSId(1);
            auto& ring = *stateStorage.MutableRing();
            ring.AddNode(1);
            ring.SetNToSelect(1);
        }

        if (!config.HasBlobStorageConfig()) {
            auto& bsConfig = *config.MutableBlobStorageConfig();
            auto& serviceSet = *bsConfig.MutableServiceSet();
            auto& group = *serviceSet.AddGroups();
            ctx.GroupErasureSpecies[0] = erasureName;

            auto& ring = *group.AddRings();
            auto& failDomain = *ring.AddFailDomains();
            failDomain.AddVDiskLocations();

            auto& vdiskLoc = ctx.CombinedDiskInfo[TCombinedDiskInfoKey{}];

            vdiskLoc.SetNodeID("1");
            vdiskLoc.SetPath(drivePath.value());
            vdiskLoc.SetPDiskCategory(diskType.value());
        }

        if (!config.HasChannelProfileConfig()) {
            auto& channelProfile = *config.MutableChannelProfileConfig()->AddProfile();
            channelProfile.SetProfileId(0);
            for (size_t i = 0; i < 3; ++i) {
                auto& channel = *channelProfile.AddChannel();
                channel.SetErasureSpecies(erasureName);
                channel.SetPDiskCategory(1);
                channel.SetStoragePoolKind(diskTypeLower.value());
            };
        }
    }

    void ApplyMultiNodeDefaults(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        TMaybe<TString> erasureName;
        TMaybe<TString> defaultDiskType;
        TMaybe<TString> defaultDiskTypeLower;
        TMaybe<NKikimrBlobStorage::EPDiskType> dtEnum;

        const NKikimrConfig::TBlobStorageConfig::TAutoconfigSettings *autoconf = nullptr;
        if (config.HasBlobStorageConfig()) {
            if (const auto& bsConfig = config.GetBlobStorageConfig(); bsConfig.HasAutoconfigSettings()) {
                autoconf = &bsConfig.GetAutoconfigSettings();
            }
        }

        if (ephemeralConfig.HasErasure()) {
            erasureName = ephemeralConfig.GetErasure();
        } else if (autoconf && autoconf->HasErasureSpecies()) {
            erasureName = autoconf->GetErasureSpecies();
        }

        if (ephemeralConfig.HasDefaultDiskType()) {
            defaultDiskType = ephemeralConfig.GetDefaultDiskType();
            Y_ENSURE_BT(NKikimrBlobStorage::EPDiskType_Parse(*defaultDiskType, &dtEnum.ConstructInPlace()),
                "incorrect enum: " << defaultDiskType);
        } else if (autoconf) {
            THashSet<TMaybe<NKikimrBlobStorage::EPDiskType>> options;
            bool error = false;

            if (autoconf->HasPDiskType()) {
                options.insert(autoconf->GetPDiskType());
            }

            for (const auto& filter : autoconf->GetPDiskFilter()) {
                TMaybe<NKikimrBlobStorage::EPDiskType> type;
                for (const auto& prop : filter.GetProperty()) {
                    if (prop.HasType()) {
                        if (type) { // two Type values in single filter
                            error = true;
                        } else {
                            type = prop.GetType();
                        }
                    }
                }
                options.insert(type);
            }

            if (options.size() == 1 && !error) {
                dtEnum = *options.begin();
                defaultDiskType = NKikimrBlobStorage::EPDiskType_Name(*dtEnum);
            }
        }

        if (defaultDiskType) {
            defaultDiskTypeLower = *defaultDiskType.Get();
            defaultDiskTypeLower.Get()->to_lower();
        }

        if (erasureName && !ephemeralConfig.HasStaticErasure()) {
            ephemeralConfig.SetStaticErasure(*erasureName);
        }

        if (!config.HasDomainsConfig()) {
            auto& domainsConfig = *config.MutableDomainsConfig();
            auto& domain = *domainsConfig.AddDomain();
            domain.SetName("Root");

            if (erasureName && defaultDiskTypeLower && dtEnum) {
                auto& poolType = *domain.AddStoragePoolTypes();
                poolType.SetKind(*defaultDiskTypeLower);
                auto& poolConfig = *poolType.MutablePoolConfig();
                poolConfig.SetErasureSpecies(*erasureName);
                poolConfig.SetVDiskKind("Default");
                auto& filter = *poolConfig.AddPDiskFilter();
                auto& prop = *filter.AddProperty();
                prop.SetType(*dtEnum);
            }
        } else {
            auto& domainsConfig = *config.MutableDomainsConfig();

            Y_ENSURE_BT(domainsConfig.DomainSize() <= 1, "Only a single domain is currently supported");
            if (domainsConfig.DomainSize() == 1) {
                auto& domain = *domainsConfig.MutableDomain(0);
                ui32 storagePoolTypeId = 0;
                for (auto& storagePoolType : *domain.MutableStoragePoolTypes()) {
                    if (defaultDiskTypeLower && !storagePoolType.HasKind()) {
                        storagePoolType.SetKind(*defaultDiskTypeLower);
                    }

                    if (storagePoolType.HasPoolConfig()) {
                        auto& poolConfig = *storagePoolType.MutablePoolConfig();
                        auto& info = ctx.PoolConfigInfo[{0, storagePoolTypeId}];

                        if (erasureName && !info.HasErasureSpecies) {
                            poolConfig.SetErasureSpecies(erasureName.GetRef());
                        }

                        if (defaultDiskTypeLower && !info.HasKind) {
                            poolConfig.SetKind(*defaultDiskTypeLower);
                        }

                        if (defaultDiskType && !poolConfig.PDiskFilterSize()) {
                            poolConfig.AddPDiskFilter()->AddProperty()->SetType(*dtEnum);
                        }

                        if (!info.HasVDiskKind) {
                            poolConfig.SetVDiskKind("Default");
                        }
                    }
                    ++storagePoolTypeId;
                }
            }
        }

        if (!config.HasGRpcConfig()) {
            auto& grpc = *config.MutableGRpcConfig();
            grpc.SetPort(2135);
        }

        if (config.HasBlobStorageConfig()) {
            auto& bsConfig = *config.MutableBlobStorageConfig();
            if (bsConfig.HasServiceSet()) {
                auto& serviceSet = *bsConfig.MutableServiceSet();
                for (ui32 groupID = 0; groupID < serviceSet.GroupsSize(); ++groupID) {
                    if (erasureName && !ctx.GroupErasureSpecies.contains(groupID)) {
                        ctx.GroupErasureSpecies[groupID] = erasureName.GetRef();
                    }
                }
            }
            if (defaultDiskType) {
                for (auto& [_, loc] : ctx.CombinedDiskInfo) {
                    if (!loc.HasPDiskCategory()) {
                        loc.SetPDiskCategory(defaultDiskType.GetRef());
                    }
                }
            }
        }

        if (config.HasChannelProfileConfig()) {
            auto& cpConfig = *config.MutableChannelProfileConfig();
            for (auto& profile : *cpConfig.MutableProfile()) {
                for (auto& channel : *profile.MutableChannel()) {
                    if (erasureName && !channel.HasErasureSpecies()) {
                        channel.SetErasureSpecies(erasureName.GetRef());
                    }

                    if (defaultDiskTypeLower && !channel.HasStoragePoolKind()) {
                        channel.SetStoragePoolKind(defaultDiskTypeLower.GetRef());
                    }
                }
            }
        } else if (erasureName && defaultDiskTypeLower) {
            auto& channelProfile = *config.MutableChannelProfileConfig()->AddProfile();
            channelProfile.SetProfileId(0);

            for (size_t i = 0; i < 3; ++i) {
                auto& channel = *channelProfile.AddChannel();
                channel.SetErasureSpecies(erasureName.GetRef());
                channel.SetPDiskCategory(TPDiskCategory(PDiskTypeToPDiskType(*dtEnum), 0));
                channel.SetStoragePoolKind(defaultDiskTypeLower.GetRef());
            };
        }
    }

    void ApplyDefaultConfigs(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        if (ephemeralConfig.HostsSize() == 1) {
            ApplySingleNodeDefaults(ctx, config, ephemeralConfig);
        } else if (ephemeralConfig.HostsSize() > 1) {
            ApplyMultiNodeDefaults(ctx, config, ephemeralConfig);
        }
    }

    void PrepareNameserviceConfig(NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        if (config.HasNameserviceConfig() && config.GetNameserviceConfig().NodeSize()) {
            return;
        }

        // make expliti empty ?
        if (!ephemeralConfig.HostsSize()) {
            return;
        }

        auto* nsConfig = config.MutableNameserviceConfig();

        for(const auto& host : ephemeralConfig.GetHosts()) {
            auto* node = nsConfig->AddNode();
            /* TODO: add optional reflection layout check */
            /* or even better additional copy method generated by special annotation */

            if (host.HasNodeId()) {
                node->SetNodeId(host.GetNodeId());
            }

            if (host.HasAddress()) {
                node->SetAddress(host.GetAddress());
            }

            if (host.HasPort()) {
                node->SetPort(host.GetPort());
            }

            if (host.HasHost()) {
                node->SetHost(host.GetHost());
            }

            if (host.HasLocation()) {
                node->MutableLocation()->CopyFrom(host.GetLocation());
            }

            if (host.EndpointSize()) {
                for (const auto& endpoint : host.GetEndpoint()) {
                    node->AddEndpoint()->CopyFrom(endpoint);
                }
            }

            if (host.HasWalleLocation()) {
                node->MutableWalleLocation()->CopyFrom(host.GetWalleLocation());
            }

            if (host.HasInterconnectHost()) {
                node->SetInterconnectHost(host.GetInterconnectHost());
            } else {
                node->SetInterconnectHost(host.GetHost());
            }
        }
    }

    TString HostAndICPort(const NKikimrConfig::TStaticNameserviceConfig::TNode& host) {
        TString hostname = host.GetHost();
        ui32 interconnectPort = DEFAULT_INTERCONNECT_PORT;
        if (host.HasPort()) {
            interconnectPort = host.GetPort();
        }

        return TStringBuilder() << hostname << ":" << interconnectPort;
    }

    NKikimrConfig::TStaticNameserviceConfig::TNode FindNodeByString(NKikimrConfig::TAppConfig& config, const TString& data) {
        ui32 foundCandidates = 0;
        NKikimrConfig::TStaticNameserviceConfig::TNode result;

        // TODO ensure?
        auto& nsConfig = config.GetNameserviceConfig();

        for(auto& host : nsConfig.GetNode()) {
            if (data == host.GetHost()) {
                result = host;
                ++foundCandidates;
            }
        }

        if (foundCandidates == 1) {
            return result;
        }

        foundCandidates = 0;
        for(auto& host : nsConfig.GetNode()) {
            if (data == HostAndICPort(host)) {
                result = host;
                ++foundCandidates;
            }
        }

        Y_ENSURE_BT(foundCandidates == 1, "Cannot find node_id for " << data);
        return result;
    }

    ui32 FindNodeId(NKikimrConfig::TAppConfig& config, const TString& host) {
        ui32 result = 0;
        if (TryFromString(host, result)) {
            return result;
        }

        auto node = FindNodeByString(config, host);
        return node.GetNodeId();
    }

    void PrepareStaticGroup(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        Y_UNUSED(ephemeralConfig);
        if (!config.HasBlobStorageConfig()) {
            return;
        }

        auto* bsConfig = config.MutableBlobStorageConfig();
        if (!bsConfig->HasServiceSet()) {
            return;
        }

        auto* serviceSet = bsConfig->MutableServiceSet();
        if (!serviceSet->AvailabilityDomainsSize()) {
            serviceSet->AddAvailabilityDomains(1);
        }

        if (serviceSet->GroupsSize()) {
            bool shouldFillVdisks = !serviceSet->VDisksSize();

            bool shouldFillPdisks = !serviceSet->PDisksSize();

            ui32 groupID = 0;
            for(auto& group : *serviceSet->MutableGroups()) {
                if (!group.HasGroupGeneration()) {
                    group.SetGroupGeneration(1);
                }

                if (!group.HasGroupID()) {
                    group.SetGroupID(groupID);
                }

                ui32 groupGeneration = group.GetGroupGeneration();
                ui32 realGroupID = group.GetGroupID();

                Y_ENSURE_BT(ctx.GroupErasureSpecies.contains(groupID), "erasure species are not specified for group, id " << groupID);
                group.SetErasureSpecies(ErasureStrToNum(ctx.GroupErasureSpecies[groupID]));

                std::map<ui32, std::set<ui32>> UniquePdiskIds;
                std::map<ui32, std::set<ui32>> UniquePdiskGuids;

                ui32 ringID = 0;
                for(auto& ring : *group.MutableRings()) {

                    ui32 failDomainID = 0;
                    for(auto& failDomain : *ring.MutableFailDomains()) {

                        ui32 vdiskLocationID = 0;
                        for(auto& vdiskLocation : *failDomain.MutableVDiskLocations()) {
                            TCombinedDiskInfoKey key{
                                .Group = groupID,
                                .Ring = ringID,
                                .FailDomain = failDomainID,
                                .VDiskLocation = vdiskLocationID,
                            };
                            Y_ENSURE_BT(ctx.CombinedDiskInfo.contains(key), "Can't find key: " << key);
                            auto& info = ctx.CombinedDiskInfo.at(key);
                            Y_ENSURE_BT(info.HasNodeID());

                            ui32 myNodeId = FindNodeId(config, info.GetNodeID());

                            if (!info.HasVDiskSlotID()) {
                                info.SetVDiskSlotID(0);
                            }

                            if (!info.HasPDiskGuid()) {
                                for(ui32 pdiskGuid = 1; ; pdiskGuid++) {
                                    if (UniquePdiskGuids[myNodeId].find(pdiskGuid) == UniquePdiskGuids[myNodeId].end()) {
                                        info.SetPDiskGuid(pdiskGuid);
                                        break;
                                    }
                                }
                            }

                            {
                                ui64 guid = info.GetPDiskGuid();
                                auto [it, success] = UniquePdiskGuids[myNodeId].insert(guid);
                                Y_ENSURE_BT(success, "pdisk guids should be unique, non-unique guid is " << guid);
                            }

                            if (!info.HasPDiskID()) {
                                for(ui32 pdiskID = 1; ; pdiskID++) {
                                    if (UniquePdiskIds[myNodeId].find(pdiskID) == UniquePdiskIds[myNodeId].end()) {
                                        info.SetPDiskID(pdiskID);
                                        break;
                                    }
                                }
                            }

                            {
                                ui64 pdiskId = info.GetPDiskID();
                                auto [it, success] = UniquePdiskIds[myNodeId].insert(pdiskId);
                                Y_ENSURE_BT(success, "pdisk ids should be unique, non unique pdisk_id : " << pdiskId);
                            }

                            std::optional<ui64> pDiskCategoryId;
                            if (info.HasPDiskCategory()) {
                                pDiskCategoryId = PdiskCategoryFromString(info.GetPDiskCategory());
                            }

                            info.CopyToTVDiskLocation(vdiskLocation);
                            vdiskLocation.SetNodeID(myNodeId);

                            if (shouldFillPdisks) {
                                auto* pdiskInfo = serviceSet->AddPDisks();
                                info.CopyToTPDisk(*pdiskInfo);

                                pdiskInfo->SetNodeID(myNodeId);

                                if (pDiskCategoryId) {
                                    pdiskInfo->SetPDiskCategory(pDiskCategoryId.value());
                                }
                            }

                            if (shouldFillVdisks) {
                                auto* vdiskInfo = serviceSet->AddVDisks();
                                info.CopyToTVDisk(*vdiskInfo);
                                vdiskInfo->MutableVDiskLocation()->CopyFrom(vdiskLocation);
                                auto* vdiskID = vdiskInfo->MutableVDiskID();
                                vdiskID->SetDomain(failDomainID);
                                vdiskID->SetRing(ringID);
                                vdiskID->SetVDisk(0);
                                vdiskID->SetGroupID(realGroupID);
                                vdiskID->SetGroupGeneration(groupGeneration);

                                vdiskInfo->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);
                            }

                            ++vdiskLocationID;
                        }
                        ++failDomainID;
                    }
                    ++ringID;
                }
                ++groupID;
            }
        }
    }

    void PrepareBlobStorageConfig(NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        if (!config.HasBlobStorageConfig()) {
            return;
        }
        auto* bsConfig = config.MutableBlobStorageConfig();

        if (!bsConfig->HasAutoconfigSettings()) {
            return;
        }
        auto* autoconfigSettings = bsConfig->MutableAutoconfigSettings();

        autoconfigSettings->ClearDefineHostConfig();
        autoconfigSettings->ClearDefineBox();

        bool hostConfigIdAssigned = false;
        bool hostConfigIdProvided = false;
        constexpr ui64 defaultHostConfigId = 1;
        THashSet<ui64> validHostConfigIds;

        for (const auto& hostConfig : ephemeralConfig.GetHostConfigs()) {
            auto *hostconf = autoconfigSettings->AddDefineHostConfig();
            hostConfig.CopyToTDefineHostConfig(*hostconf);
            if (hostConfig.HasHostConfigId()) {
                hostConfigIdProvided = true;
            } else if (!hostConfigIdAssigned) {
                hostConfigIdAssigned = true;
                hostconf->SetHostConfigId(defaultHostConfigId);
            } else {
                Y_ENSURE_BT(false, "multiple host configs without explicit id");
            }
            Y_ENSURE_BT(validHostConfigIds.insert(hostconf->GetHostConfigId()).second, "duplicate host config id "
                << hostconf->GetHostConfigId());
        }

        Y_ENSURE_BT(!hostConfigIdProvided || !hostConfigIdAssigned, "mixed host configs with explicit id and without one");
        Y_ENSURE_BT(!validHostConfigIds.empty(), "autoconfiguration is enabled, but no host configs provided");

        TMap<std::tuple<TString, ui32>, ui32> hostNodeMap; // (.nameservice_config.node[].interconnect_host, .nameservice_config.node[].port) -> .nameservice_config.node[].node_id
        Y_ENSURE_BT(config.HasNameserviceConfig());
        const auto& nsConfig = config.GetNameserviceConfig();
        Y_ENSURE_BT(nsConfig.NodeSize());
        for (const auto& item : nsConfig.GetNode()) {
            const auto key = std::make_tuple(item.GetInterconnectHost(), item.GetPort());
            hostNodeMap[key] = item.GetNodeId();
        }

        NKikimrBlobStorage::TDefineBox* defineBox = nullptr;
        for (const auto& host : ephemeralConfig.GetHosts()) {
            if (!host.HasHostConfigId() && !hostConfigIdAssigned) {
                continue;
            }

            if (!defineBox) {
                defineBox = autoconfigSettings->MutableDefineBox();
                defineBox->SetBoxId(1);
            }

            TString fqdn;
            if (host.HasInterconnectHost()) {
                fqdn = host.GetInterconnectHost();
            } else {
                fqdn = host.GetHost();
            }
            ui32 port = 19001;
            if (host.HasPort()) {
                port = host.GetPort();
            }
            const auto key = std::make_tuple(fqdn, port);
            Y_ENSURE_BT(hostNodeMap.contains(key));

            auto* dbHost = defineBox->AddHost();
            dbHost->SetHostConfigId(host.HasHostConfigId() ? host.GetHostConfigId() : defaultHostConfigId);
            dbHost->SetEnforcedNodeId(hostNodeMap[key]);

            Y_ENSURE_BT(validHostConfigIds.contains(dbHost->GetHostConfigId()), "invalid host config id "
                << dbHost->GetHostConfigId() << " for host " << fqdn << " and port " << port);
        }
    }

    void PrepareSystemTabletsInfo(NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig, bool relaxed)  {
        if (relaxed && (!config.HasNameserviceConfig() || !config.GetNameserviceConfig().NodeSize())) {
            return;
        }

        auto* sysTablets = ephemeralConfig.MutableSystemTablets();

        if (!sysTablets->DefaultNodeSize()) {
            for(const auto& node: config.GetNameserviceConfig().GetNode()) {
                Y_ENSURE_BT(node.HasNodeId(), "node_id must be specified");
                auto nodeId = node.GetNodeId();
                sysTablets->AddDefaultNode(nodeId);
            }
        }
    }

    const NProtoBuf::RepeatedPtrField<NKikimrConfig::TBootstrap::TTablet>& GetTabletIdsFor(NKikimrConfig::TEphemeralInputFields& ephemeralConfig, TString type) {
        auto* systemTabletsConfig = ephemeralConfig.MutableSystemTablets();
        TString enumName = type;
        NProtobufJson::ToSnakeCaseDense(&enumName);
        enumName = to_upper(enumName);

        if (!systemTabletsConfig->TabletsSize(type)) {
            for(ui32 idx = 0; idx < GetDefaultTabletCount(type); ++idx) {
                auto* tablet = systemTabletsConfig->AddTablets(type);
                NKikimrConfig::TBootstrap_ETabletType res;
                Y_ENSURE_BT(TryFromString<NKikimrConfig::TBootstrap_ETabletType>(enumName, res), "incorrect enum: " << enumName);
                tablet->SetType(res);
            }
        }

        ui32 idx = 0;
        for (auto& tablet : *systemTabletsConfig->MutableTablets(type)) {
            ++idx;

            auto* tabletInfo = tablet.MutableInfo();

            if (!tabletInfo->HasTabletID()) {
                Y_ENSURE_BT(idx <= GetDefaultTabletCount(type));
                tabletInfo->SetTabletID(GetNextTabletID(type, idx));
            }
        }

        return systemTabletsConfig->GetTablets(type);
    }

    void PrepareDomainsConfig(NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig, bool relaxed) {
        if (relaxed && !config.HasDomainsConfig()) {
            return;
        }

        Y_ENSURE_BT(config.HasDomainsConfig());
        auto* domainsConfig = config.MutableDomainsConfig();

        Y_ENSURE_BT(domainsConfig->DomainSize() == 1);

        if (!domainsConfig->HiveConfigSize()) {
            auto* hiveConfig = domainsConfig->AddHiveConfig();
            hiveConfig->SetHive(72057594037968897);
        }

        for (auto& domain : *domainsConfig->MutableDomain()) {
            Y_ENSURE_BT(domain.HasName());

            if (domain.HasDomainId()) {
                Y_ENSURE_BT(domain.GetDomainId() == 1);
            } else {
                domain.SetDomainId(1);
            }

            if (!domain.HasSchemeRoot()) {
                domain.SetSchemeRoot(72057594046678944);
            }

            if (!domain.HasPlanResolution()) {
                domain.SetPlanResolution(10);
            }

            const auto& exps = EXPLICIT_TABLETS;
            const auto* descriptor = domain.GetDescriptor();
            const auto* reflection = domain.GetReflection();
            std::vector<const NProtoBuf::FieldDescriptor *> fields;
            reflection->ListFields(domain, &fields);
            std::map<TString, const NProtoBuf::FieldDescriptor *> fieldsByName;
            for (auto* field : fields) {
                fieldsByName[field->name()] = field;
            }
            for (const auto& [field, type] : exps) {
                if (relaxed && fieldsByName.contains(field)) {
                    continue;
                }
                Y_ENSURE_BT(!fieldsByName.contains(field));

                for (const auto& tablet : GetTabletIdsFor(ephemeralConfig, type)) {
                    Y_ENSURE_BT(tablet.HasInfo() && tablet.GetInfo().HasTabletID());
                    if (auto* fieldDescriptor = descriptor->FindFieldByName(field)) {
                        reflection->AddUInt64(&domain, fieldDescriptor, tablet.GetInfo().GetTabletID());
                    } else {
                        Y_ENSURE_BT(false, "unknown explicit tablet type " << field);
                    }
                }
            }
        }
    }

    static NProtoBuf::RepeatedPtrField<NKikimrTabletBase::TTabletChannelInfo> BuildDefaultChannels(NKikimrConfig::TEphemeralInputFields& ephemeralConfig) {
        const TString& erasureName = ephemeralConfig.GetStaticErasure();
        NProtoBuf::RepeatedPtrField<NKikimrTabletBase::TTabletChannelInfo> channelsInfo;

        for(ui32 channelId = 0; channelId < 3; ++channelId) {
            auto* channelInfo = channelsInfo.Add();

            channelInfo->SetChannel(channelId);
            channelInfo->SetChannelErasureName(erasureName);

            auto* history = channelInfo->AddHistory();

            history->SetFromGeneration(0);
            history->SetGroupID(0);
        }

        return channelsInfo;
    }


    const NProtoBuf::RepeatedPtrField<NKikimrConfig::TBootstrap::TTablet>& GetTabletsFor(NKikimrConfig::TEphemeralInputFields& ephemeralConfig, TString type) {
        auto* systemTabletsConfig = ephemeralConfig.MutableSystemTablets();
        TString enumName = type;
        NProtobufJson::ToSnakeCaseDense(&enumName);
        enumName = to_upper(enumName);

        if (!systemTabletsConfig->TabletsSize(type)) {
            for(ui32 idx = 0; idx < GetDefaultTabletCount(type); ++idx) {
                auto* tablet = systemTabletsConfig->AddTablets(type);
                NKikimrConfig::TBootstrap_ETabletType res;
                Y_ENSURE_BT(TryFromString<NKikimrConfig::TBootstrap_ETabletType>(enumName, res), "incorrect enum: " << enumName);
                tablet->SetType(res);
            }
        }

        ui32 idx = 0;
        for (auto& tablet : *systemTabletsConfig->MutableTablets(type)) {
            ++idx;

            if (!tablet.NodeSize()) {
                for (const auto& node : systemTabletsConfig->GetDefaultNode()) {
                    tablet.AddNode(node);
                }
            }

            if (!tablet.HasType()) {
                NKikimrConfig::TBootstrap_ETabletType res;
                Y_ENSURE_BT(TryFromString<NKikimrConfig::TBootstrap_ETabletType>(enumName, res), "incorrect enum: " << enumName);
                tablet.SetType(res);
            }

            auto* tabletInfo = tablet.MutableInfo();

            if (!tabletInfo->HasTabletID()) {
                Y_ENSURE_BT(idx <= GetDefaultTabletCount(type));
                tabletInfo->SetTabletID(GetNextTabletID(type, idx));
            }

            if (!tabletInfo->ChannelsSize()) {
                tabletInfo->MutableChannels()->CopyFrom(BuildDefaultChannels(ephemeralConfig));
            }
        }

        return systemTabletsConfig->GetTablets(type);
    }

    void PrepareBootstrapConfig(NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig, bool relaxed) {
        if (config.HasBootstrapConfig() && config.GetBootstrapConfig().TabletSize()) {
            return;
        }

        if (relaxed && (!ephemeralConfig.HasSystemTablets() || !ephemeralConfig.HasStaticErasure())) {
            return;
        }

        auto* bootConfig = config.MutableBootstrapConfig();
        for(const auto& type : GetTabletTypes()) {
            for(const auto& tablet : GetTabletsFor(ephemeralConfig, type)) {
                bootConfig->AddTablet()->CopyFrom(tablet);
            }
        }
    }

    void TransformProtoConfig(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig, bool relaxed) {
        PrepareHosts(ephemeralConfig);
        ApplyDefaultConfigs(ctx, config, ephemeralConfig);
        PrepareNameserviceConfig(config, ephemeralConfig);
        PrepareStaticGroup(ctx, config, ephemeralConfig);
        PrepareBlobStorageConfig(config, ephemeralConfig);
        PrepareSystemTabletsInfo(config, ephemeralConfig, relaxed);
        PrepareDomainsConfig(config, ephemeralConfig, relaxed);
        PrepareBootstrapConfig(config, ephemeralConfig, relaxed);
        PrepareIcConfig(config, ephemeralConfig);
        PrepareGrpcConfig(config, ephemeralConfig);
        PrepareSecurityConfig(ctx, config, relaxed);
        PrepareActorSystemConfig(config);
        PrepareLogConfig(config);
    }

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue json = Yaml2Json(yamlNode, true);

        NJson::TJsonValue ephemeralJsonNode = json;
        for (const auto& field : ListNonEphemeralFields()) {
            ephemeralJsonNode.EraseValue(field);
        }
        NKikimrConfig::TEphemeralInputFields ephemeralConfig;
        NProtobufJson::MergeJson2Proto(ephemeralJsonNode, ephemeralConfig, GetJsonToProtoConfig());

        NKikimrConfig::TAppConfig config;
        PrepareHosts(ephemeralConfig);
        PrepareNameserviceConfig(config, ephemeralConfig);

        NKikimrBlobStorage::TConfigRequest result;

        const auto itemConfigGeneration = ephemeralConfig.HasStorageConfigGeneration() ?
            ephemeralConfig.GetStorageConfigGeneration() : 0;

        for(const auto& hostConfig : ephemeralConfig.GetHostConfigs()) {
            auto *hostConfigProto = result.AddCommand()->MutableDefineHostConfig();
            hostConfig.CopyToTDefineHostConfig(*hostConfigProto);
            // KIKIMR-16712
            // Avoid checking the version number for "host_config" configuration items.
            // This allows to add new host configuration items after the initial cluster setup.
            hostConfigProto->SetItemConfigGeneration(Max<ui64>());
        }

        auto *defineBox = result.AddCommand()->MutableDefineBox();
        defineBox->SetBoxId(1);
        defineBox->SetItemConfigGeneration(itemConfigGeneration);

        for(const auto& host : ephemeralConfig.GetHosts()) {
            auto* dbHost = defineBox->AddHost();
            auto* hostKey = dbHost->MutableKey();
            hostKey->SetNodeId(host.GetNodeId());
            hostKey->SetFqdn(host.GetHost());
            hostKey->SetIcPort(host.GetPort());
            dbHost->SetHostConfigId(host.GetHostConfigId());
        }

        return result;
    }

    void Parse(const NJson::TJsonValue& json, NProtobufJson::TJson2ProtoConfig convertConfig, NKikimrConfig::TAppConfig& config, bool transform, bool relaxed) {
        auto jsonNode = json;
        TTransformContext ctx;
        NKikimrConfig::TEphemeralInputFields ephemeralConfig;

        if (transform) {
            ExtractExtraFields(jsonNode, ctx);

            NJson::TJsonValue ephemeralJsonNode = jsonNode;
            ClearNonEphemeralFields(ephemeralJsonNode);
            NProtobufJson::MergeJson2Proto(ephemeralJsonNode, ephemeralConfig, convertConfig);
            ClearEphemeralFields(jsonNode);
        }

        NProtobufJson::MergeJson2Proto(jsonNode, config, convertConfig);

        if (transform) {
            TransformProtoConfig(ctx, config, ephemeralConfig, relaxed);
        }
    }

    NKikimrConfig::TAppConfig Parse(const TString& data, bool transform) {
        auto yamlNode = YAML::Load(data);
        NJson::TJsonValue jsonNode = Yaml2Json(yamlNode, true);

        NKikimrConfig::TAppConfig config;
        Parse(jsonNode, GetJsonToProtoConfig(), config, transform);

        return config;
    }

} // NKikimr::NYaml

template <>
NKikimrBlobStorage::EPDiskType
NKikimrConfig::TExtendedHostConfigDrive::TransformTypeToTypeForTHostConfigDrive<const TString, NKikimrBlobStorage::EPDiskType>(const TString* const in) {
    NKikimrBlobStorage::EPDiskType res{};
    if (!in || TryFromString(*in, res)) {
        return res;
    }
    Y_ENSURE_BT(false, "Unknown EPDiskType enum value: " << *in);
}
