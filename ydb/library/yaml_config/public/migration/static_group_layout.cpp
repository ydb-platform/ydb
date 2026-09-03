#include "config_migration.h"
#include "yaml_helpers.h"

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <optional>
#include <tuple>
#include <utility>

namespace NKikimr::NYamlConfig {
namespace {

    using NMigrationDetail::AsMap;
    using NMigrationDetail::AsSequence;
    using NMigrationDetail::FindMap;
    using NMigrationDetail::FindScalar;
    using NMigrationDetail::FindSequence;

    constexpr ui32 DefaultInterconnectPort = 19001;

    enum class EDomainType {
        Rack,
        Disk,
    };

    enum class EErasureSpecies {
        Mirror3dc,
        Block42,
    };

    struct TGroupShape {
        ui32 NumFailRealms = 0;
        ui32 NumFailDomainsPerFailRealm = 0;
        ui32 NumVDisksPerFailDomain = 0;

        bool operator==(const TGroupShape&) const = default;
    };

    // Defaults used by TGroupGeometryInfo when the shape is omitted.
    constexpr TGroupShape DefaultMirror3dcShape{3, 3, 1};
    constexpr TGroupShape DefaultBlock42Shape{1, 8, 1};

    struct TStaticGroupGeometry {
        EDomainType DomainType;
        std::optional<TGroupShape> Shape;

        bool operator==(const TStaticGroupGeometry&) const = default;
    };

    const TGroupShape& GetDefaultShape(EErasureSpecies erasureSpecies) {
        return erasureSpecies == EErasureSpecies::Mirror3dc ? DefaultMirror3dcShape : DefaultBlock42Shape;
    }

    struct TNodeLocation {
        TString BridgePile;
        TString DataCenter;
        TString Module;
        TString Rack;
    };

    struct TNodes {
        TMap<ui32, TNodeLocation> Locations;
        TMap<TString, std::optional<ui32>> Aliases;
    };

    struct TPhysicalRealm {
        TString BridgePile;
        TString DataCenter;

        bool operator==(const TPhysicalRealm&) const = default;

        auto Fields() const {
            return std::tie(BridgePile, DataCenter);
        }

        bool operator<(const TPhysicalRealm& other) const {
            return Fields() < other.Fields();
        }
    };

    template <typename T>
    bool KeepSame(std::optional<T>& expected, const T& value) {
        if (!expected) {
            expected = value;
            return true;
        }
        return *expected == value;
    }

    struct TPhysicalDomain {
        TString BridgePile;
        TString DataCenter;
        TString Module;
        TString Rack;
        ui32 NodeId = 0;
        ui32 PDiskId = 0;

        bool operator==(const TPhysicalDomain&) const = default;

        auto Fields() const {
            return std::tie(BridgePile, DataCenter, Module, Rack, NodeId, PDiskId);
        }

        bool operator<(const TPhysicalDomain& other) const {
            return Fields() < other.Fields();
        }
    };

    std::optional<ui32> FindUi32(const NFyaml::TMapping& map, TStringBuf key) {
        const auto scalar = FindScalar(map, key);
        if (!scalar) {
            return std::nullopt;
        }
        ui32 value;
        Y_ENSURE_EX(TryFromString(*scalar, value), TYamlConfigEx() << "'" << key << "' must be an unsigned integer");
        return value;
    }

    TString FindScalarOrEmpty(const NFyaml::TMapping& map, TStringBuf key) {
        return FindScalar(map, key).value_or(TString());
    }

    std::optional<TStaticGroupGeometry> GetGeometry(const NFyaml::TMapping& geometry) {
        const ui32 realmBegin = FindUi32(geometry, "realm_level_begin").value_or(0);
        const ui32 realmEnd = FindUi32(geometry, "realm_level_end").value_or(0);
        const ui32 domainBegin = FindUi32(geometry, "domain_level_begin").value_or(0);
        const ui32 domainEnd = FindUi32(geometry, "domain_level_end").value_or(0);
        std::optional<EDomainType> domainType;
        if (!realmBegin && !realmEnd && !domainBegin && !domainEnd) {
            domainType = EDomainType::Rack;
        } else if (realmBegin != 10 || realmEnd != 20 || domainBegin != 10) {
            return std::nullopt;
        } else if (domainEnd == 40) {
            domainType = EDomainType::Rack;
        } else if (domainEnd == 256) {
            domainType = EDomainType::Disk;
        } else {
            return std::nullopt;
        }

        TGroupShape shape{
            .NumFailRealms = FindUi32(geometry, "num_fail_realms").value_or(0),
            .NumFailDomainsPerFailRealm = FindUi32(geometry, "num_fail_domains_per_fail_realm").value_or(0),
            .NumVDisksPerFailDomain = FindUi32(geometry, "num_vdisks_per_fail_domain").value_or(0),
        };
        std::optional<TGroupShape> configuredShape;
        if (shape.NumFailRealms || shape.NumFailDomainsPerFailRealm || shape.NumVDisksPerFailDomain) {
            configuredShape = shape;
        }
        return TStaticGroupGeometry{
            .DomainType = *domainType,
            .Shape = std::move(configuredShape),
        };
    }

    std::optional<TVector<TStaticGroupGeometry>> GetStoragePoolGeometries(const NFyaml::TMapping& config) {
        const auto domainsConfig = FindMap(config, "domains_config");
        const auto domains = domainsConfig ? FindSequence(*domainsConfig, "domain") : std::nullopt;
        if (!domains) {
            return std::nullopt;
        }

        TVector<TStaticGroupGeometry> result;
        for (const auto& domainNode : *domains) {
            const auto domain = AsMap(domainNode);
            if (!domain) {
                return std::nullopt;
            }
            const auto storagePoolTypes = FindSequence(*domain, "storage_pool_types");
            if (!storagePoolTypes) {
                return std::nullopt;
            }
            for (const auto& storagePoolTypeNode : *storagePoolTypes) {
                const auto storagePoolType = AsMap(storagePoolTypeNode);
                const auto poolConfig = storagePoolType ? FindMap(*storagePoolType, "pool_config") : std::nullopt;
                if (!poolConfig) {
                    return std::nullopt;
                }
                const auto geometry = FindMap(*poolConfig, "geometry");
                if (poolConfig->Has("geometry") && !geometry) {
                    return std::nullopt;
                }
                const auto current = geometry
                                     ? GetGeometry(*geometry)
                                     : std::make_optional(TStaticGroupGeometry{.DomainType = EDomainType::Rack});
                if (!current) {
                    return std::nullopt;
                }
                result.push_back(*current);
            }
        }
        if (result.empty()) {
            return std::nullopt;
        }
        return std::make_optional(std::move(result));
    }

    std::optional<TStaticGroupGeometry> GetStaticGroupGeometry(const TVector<TStaticGroupGeometry>& storagePoolGeometries,
                                                               EErasureSpecies erasureSpecies) {
        const auto& defaultShape = GetDefaultShape(erasureSpecies);
        std::optional<TStaticGroupGeometry> result;
        for (const auto& geometry : storagePoolGeometries) {
            if (geometry.Shape && *geometry.Shape != defaultShape) {
                return std::nullopt;
            }
            TStaticGroupGeometry current{
                .DomainType = geometry.DomainType,
                .Shape = defaultShape,
            };
            if (result && *result != current) {
                return std::nullopt;
            }
            result = std::move(current);
        }
        return result;
    }

    std::optional<EErasureSpecies> ParseErasureSpecies(TStringBuf value) {
        if (AsciiEqualsIgnoreCase(value, "mirror-3-dc")) {
            return EErasureSpecies::Mirror3dc;
        }
        if (AsciiEqualsIgnoreCase(value, "block-4-2")) {
            return EErasureSpecies::Block42;
        }
        return std::nullopt;
    }

    EStaticGroupLayoutCheckResult GetMirror3dcLayout(EDomainType domainType) {
        return domainType == EDomainType::Disk
               ? EStaticGroupLayoutCheckResult::Mirror3dc3Nodes
               : EStaticGroupLayoutCheckResult::Mirror3dc;
    }

    TNodeLocation ReadLocation(const NFyaml::TMapping& location) {
        return {
            .BridgePile = FindScalarOrEmpty(location, "bridge_pile_name"),
            .DataCenter = FindScalarOrEmpty(location, "data_center"),
            .Module = FindScalarOrEmpty(location, "module"),
            .Rack = FindScalarOrEmpty(location, "rack"),
        };
    }

    void AddNode(TNodes& nodes, ui32 nodeId, TNodeLocation location, const NFyaml::TMapping& node) {
        nodes.Locations.emplace(nodeId, std::move(location));
        const auto host = FindScalar(node, "host");
        if (!host) {
            return;
        }

        const auto addAlias = [&](TString alias) {
            const auto [it, inserted] = nodes.Aliases.emplace(std::move(alias), nodeId);
            if (!inserted) {
                it->second.reset();
            }
        };
        addAlias(*host);
        addAlias(TStringBuilder() << *host << ':' << FindUi32(node, "port").value_or(DefaultInterconnectPort));
    }

    // Mirrors simplified-host defaults from NYaml::PrepareHosts without introducing core dependencies.
    TNodes ReadHosts(const NFyaml::TSequence& hosts) {
        ui32 nextBodyId = 1;
        for (const auto& hostNode : hosts) {
            if (const auto host = AsMap(hostNode)) {
                auto location = FindMap(*host, "walle_location");
                if (!location) {
                    location = FindMap(*host, "location");
                }
                if (location) {
                    nextBodyId = std::max(nextBodyId, FindUi32(*location, "body").value_or(0) + 1);
                }
            }
        }

        TNodes result;
        for (size_t index = 0; index < hosts.size(); ++index) {
            const auto host = AsMap(hosts.at(index));
            Y_ENSURE_EX(host, TYamlConfigEx() << "'hosts[" << index << "]' must be a mapping");

            const ui32 nodeId = FindUi32(*host, "node_id").value_or(static_cast<ui32>(index + 1));
            if (const auto walleLocation = FindMap(*host, "walle_location")) {
                AddNode(result, nodeId, ReadLocation(*walleLocation), *host);
                continue;
            }

            TNodeLocation location;
            const auto yamlLocation = FindMap(*host, "location");
            if (yamlLocation) {
                location = ReadLocation(*yamlLocation);
            }
            const auto body = yamlLocation ? FindUi32(*yamlLocation, "body") : std::nullopt;
            const ui32 bodyId = body.value_or(nextBodyId);
            if (!body) {
                ++nextBodyId;
            }
            if (!location.DataCenter) {
                location.DataCenter = "default";
            }
            if (!location.Rack) {
                location.Rack = TStringBuilder() << "generated-rack-" << bodyId;
            }
            AddNode(result, nodeId, std::move(location), *host);
        }
        return result;
    }

    TNodes ReadNodes(const NFyaml::TMapping& config) {
        const auto nameservice = FindMap(config, "nameservice_config");
        const auto nodes = nameservice ? FindSequence(*nameservice, "node") : std::nullopt;
        if (!nodes || nodes->empty()) {
            const auto hosts = FindSequence(config, "hosts");
            return hosts ? ReadHosts(*hosts) : TNodes();
        }

        TNodes result;
        for (size_t index = 0; index < nodes->size(); ++index) {
            const auto node = AsMap(nodes->at(index));
            Y_ENSURE_EX(node, TYamlConfigEx() << "'nameservice_config.node[" << index << "]' must be a mapping");
            const auto nodeId = FindUi32(*node, "node_id");
            Y_ENSURE_EX(nodeId, TYamlConfigEx() << "'nameservice_config.node[" << index << "].node_id' is required");
            auto location = FindMap(*node, "location");
            if (!location) {
                location = FindMap(*node, "walle_location");
            }
            AddNode(result, *nodeId, location ? ReadLocation(*location) : TNodeLocation(), *node);
        }
        return result;
    }

    ui32 ResolveNodeId(const TNodes& nodes, TStringBuf value) {
        ui32 nodeId;
        if (TryFromString(value, nodeId)) {
            return nodeId;
        }
        const auto alias = nodes.Aliases.find(TString(value));
        Y_ENSURE_EX(alias != nodes.Aliases.end() && alias->second,
                    TYamlConfigEx() << "Cannot find node_id for " << value);
        return *alias->second;
    }

    // Mirrors per-group PDisk ID inference from NYaml::PrepareStaticGroup.
    std::optional<ui32> ResolvePDiskId(const NFyaml::TMapping& vdisk, ui32 nodeId,
                                       TMap<ui32, TSet<ui32>>& usedPDiskIds) {
        auto& used = usedPDiskIds[nodeId];
        const auto configuredPDiskId = FindUi32(vdisk, "pdisk_id");
        ui32 pdiskId = configuredPDiskId.value_or(1);
        while (!configuredPDiskId && used.contains(pdiskId)) {
            ++pdiskId;
        }
        return used.insert(pdiskId).second ? std::make_optional(pdiskId) : std::nullopt;
    }

    TPhysicalRealm GetRealm(const TNodeLocation& location) {
        return {
            .BridgePile = location.BridgePile,
            .DataCenter = location.DataCenter,
        };
    }

    TPhysicalDomain GetDomain(const TNodeLocation& location, ui32 nodeId, ui32 pdiskId, EDomainType type) {
        if (type == EDomainType::Rack) {
            return {
                .BridgePile = location.BridgePile,
                .DataCenter = location.DataCenter,
                .Module = location.Module,
                .Rack = location.Rack,
            };
        }
        return {
            .NodeId = nodeId,
            .PDiskId = pdiskId,
        };
    }

    bool IsGroupLayoutCorrect(const NFyaml::TMapping& group, const TNodes& nodes,
                              const TStaticGroupGeometry& geometry) {
        Y_ABORT_UNLESS(geometry.Shape);
        const auto& shape = *geometry.Shape;
        const auto rings = FindSequence(group, "rings");
        if (!rings || rings->size() != shape.NumFailRealms) {
            return false;
        }

        std::optional<TString> realmGroup;
        TSet<TPhysicalRealm> usedRealms;
        TSet<TPhysicalDomain> usedDomains;
        TMap<ui32, TSet<ui32>> usedPDiskIds;

        for (const auto& ringNode : *rings) {
            const auto ring = AsMap(ringNode);
            if (!ring) {
                return false;
            }
            const auto failDomains = FindSequence(*ring, "fail_domains");
            if (!failDomains || failDomains->size() != shape.NumFailDomainsPerFailRealm) {
                return false;
            }

            std::optional<TPhysicalRealm> realm;
            for (const auto& failDomainNode : *failDomains) {
                const auto failDomain = AsMap(failDomainNode);
                const auto vdisks = failDomain ? FindSequence(*failDomain, "vdisk_locations") : std::nullopt;
                if (!vdisks || vdisks->size() != shape.NumVDisksPerFailDomain) {
                    return false;
                }

                std::optional<TPhysicalDomain> domain;
                for (const auto& vdiskNode : *vdisks) {
                    const auto vdisk = AsMap(vdiskNode);
                    if (!vdisk) {
                        return false;
                    }
                    const auto node = FindScalar(*vdisk, "node_id");
                    if (!node) {
                        return false;
                    }
                    const ui32 nodeId = ResolveNodeId(nodes, *node);
                    const auto location = nodes.Locations.find(nodeId);
                    Y_ENSURE_EX(location != nodes.Locations.end(),
                                TYamlConfigEx() << "Static group references unknown node " << nodeId);
                    const auto pdiskId = ResolvePDiskId(*vdisk, nodeId, usedPDiskIds);
                    if (!pdiskId) {
                        return false;
                    }

                    if (!KeepSame(realmGroup, location->second.BridgePile)
                        || !KeepSame(realm, GetRealm(location->second))
                        || !KeepSame(domain,
                                    GetDomain(location->second, nodeId, *pdiskId, geometry.DomainType))) {
                        return false;
                    }
                }
                if (!domain || !usedDomains.insert(*domain).second) {
                    return false;
                }
            }
            if (!realm || !usedRealms.insert(*realm).second) {
                return false;
            }
        }
        return true;
    }

    EStaticGroupLayoutCheckResult CheckStaticGroupLayout(const NFyaml::TMapping& config) {
        const auto blobStorage = FindMap(config, "blob_storage_config");
        const auto serviceSet = blobStorage ? FindMap(*blobStorage, "service_set") : std::nullopt;
        if (!serviceSet) {
            return EStaticGroupLayoutCheckResult::NotApplicable;
        }

        const auto groups = FindSequence(*serviceSet, "groups");
        if (!groups) {
            if (serviceSet->Has("groups")) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
            return EStaticGroupLayoutCheckResult::NotApplicable;
        }

        auto defaultErasureSpecies = FindScalar(config, "static_erasure");
        if (!defaultErasureSpecies) {
            defaultErasureSpecies = FindScalar(config, "erasure");
        }
        TVector<NFyaml::TMapping> supportedGroups;
        std::optional<EErasureSpecies> commonErasureSpecies;
        for (const auto& groupNode : *groups) {
            const auto group = AsMap(groupNode);
            if (!group) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
            if (group->Has("bridge_group_state")) {
                continue;
            }
            auto erasureSpecies = FindScalar(*group, "erasure_species");
            if (!erasureSpecies) {
                erasureSpecies = defaultErasureSpecies;
            }
            if (!erasureSpecies) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
            const auto supportedErasure = ParseErasureSpecies(*erasureSpecies);
            if (!supportedErasure) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
            if (commonErasureSpecies && *commonErasureSpecies != *supportedErasure) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
            commonErasureSpecies = *supportedErasure;
            supportedGroups.push_back(*group);
        }
        if (supportedGroups.empty()) {
            return EStaticGroupLayoutCheckResult::NotApplicable;
        }

        const auto storagePoolGeometries = GetStoragePoolGeometries(config);
        if (!storagePoolGeometries) {
            return EStaticGroupLayoutCheckResult::Incorrect;
        }

        Y_ABORT_UNLESS(commonErasureSpecies);
        const auto geometry = GetStaticGroupGeometry(*storagePoolGeometries, *commonErasureSpecies);
        if (!geometry
            || (*commonErasureSpecies == EErasureSpecies::Block42
                && geometry->DomainType != EDomainType::Rack)) {
            return EStaticGroupLayoutCheckResult::Incorrect;
        }

        const auto nodes = ReadNodes(config);
        for (const auto& group : supportedGroups) {
            if (!IsGroupLayoutCorrect(group, nodes, *geometry)) {
                return EStaticGroupLayoutCheckResult::Incorrect;
            }
        }
        return *commonErasureSpecies == EErasureSpecies::Mirror3dc
               ? GetMirror3dcLayout(geometry->DomainType)
               : EStaticGroupLayoutCheckResult::Block42;
    }

} // anonymous namespace

EStaticGroupLayoutCheckResult CheckStaticGroupLayout(NFyaml::TDocument& document) {
    const auto root = AsMap(document.Root());
    const auto config = root ? FindMap(*root, "config") : std::nullopt;
    Y_ENSURE_EX(config, TYamlConfigEx() << "Config must have a 'config' mapping");
    return CheckStaticGroupLayout(*config);
}

} // namespace NKikimr::NYamlConfig
