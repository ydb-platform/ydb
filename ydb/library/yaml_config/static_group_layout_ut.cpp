#include <ydb/library/yaml_config/public/migration/config_migration.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <optional>

using namespace NKikimr;

namespace {

    enum class EPlacement {
        OneNodePerRealm,
        OneNodePerVDisk,
        SplitRealms,
        SharedRealms,
    };

    enum class EPoolGeometry {
        Missing,
        Rack,
        Disk,
    };

    struct TGroupShape {
        ui32 Rings = 3;
        ui32 FailDomains = 3;
        ui32 VDisks = 1;
    };

    struct TConfigOptions {
        EPlacement Placement = EPlacement::OneNodePerVDisk;
        EPoolGeometry PoolGeometry = EPoolGeometry::Rack;
        bool DuplicatePDisk = false;
        bool DuplicateRack = false;
        bool OmitPDiskIds = false;
        bool SplitRealmGroups = false;
        TStringBuf PoolErasure = "mirror-3-dc";
        TStringBuf GroupErasure = "mirror-3-dc";
        TGroupShape GroupShape;
        std::optional<TGroupShape> GeometryShape;
    };

    TString MakeConfig(const TConfigOptions& options) {
        TStringBuilder config;
        config << "config:\n"
               << "  hosts:\n";

        const bool oneNodePerRealm = options.Placement == EPlacement::OneNodePerRealm;
        const ui32 nodesPerRealm = options.GroupShape.FailDomains * options.GroupShape.VDisks;
        const ui32 nodeCount = oneNodePerRealm
                               ? options.GroupShape.Rings
                               : options.GroupShape.Rings * nodesPerRealm;
        for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            const ui32 dataCenter = oneNodePerRealm || options.Placement == EPlacement::SplitRealms
                                    ? nodeId
                                    : options.Placement == EPlacement::SharedRealms
                                      ? (nodeId - 1) % nodesPerRealm + 1
                                      : (nodeId - 1) / nodesPerRealm + 1;
            const ui32 rack = options.DuplicateRack && nodeId == 2 ? 1 : nodeId;
            config << "  - node_id: " << nodeId << '\n'
                   << "    host: host-" << nodeId << '\n'
                   << "    port: " << 19000 + nodeId << '\n'
                   << "    location:\n";
            if (options.SplitRealmGroups) {
                config << "      bridge_pile_name: pile-" << (nodeId - 1) / nodesPerRealm + 1 << '\n';
            }
            config << "      data_center: dc-" << dataCenter << '\n'
                   << "      rack: rack-" << rack << '\n';
        }

        config << "  domains_config:\n"
               << "    domain:\n"
               << "    - storage_pool_types:\n"
               << "      - pool_config:\n"
               << "          erasure_species: " << options.PoolErasure << '\n'
               << "          kind: ssd\n";
        if (options.PoolGeometry != EPoolGeometry::Missing) {
            config << "          geometry:\n"
                   << "            realm_level_begin: 10\n"
                   << "            realm_level_end: 20\n"
                   << "            domain_level_begin: 10\n"
                   << "            domain_level_end: "
                   << (options.PoolGeometry == EPoolGeometry::Disk ? 256 : 40) << '\n';
            if (options.GeometryShape) {
                config << "            num_fail_realms: " << options.GeometryShape->Rings << '\n'
                       << "            num_fail_domains_per_fail_realm: " << options.GeometryShape->FailDomains << '\n'
                       << "            num_vdisks_per_fail_domain: " << options.GeometryShape->VDisks << '\n';
            }
        }

        config << "  self_management_config:\n"
               << "    enabled: false\n"
               << "  blob_storage_config:\n"
               << "    service_set:\n"
               << "      groups:\n"
               << "      - erasure_species: " << options.GroupErasure << '\n'
               << "        rings:\n";
        for (ui32 realm = 0; realm < options.GroupShape.Rings; ++realm) {
            config << "        - fail_domains:\n";
            for (ui32 domain = 0; domain < options.GroupShape.FailDomains; ++domain) {
                config << "          - vdisk_locations:\n";
                for (ui32 vdisk = 0; vdisk < options.GroupShape.VDisks; ++vdisk) {
                    const ui32 nodeId = oneNodePerRealm
                                        ? realm + 1
                                        : realm * nodesPerRealm
                                          + (options.DuplicatePDisk
                                             ? 1
                                             : domain * options.GroupShape.VDisks + vdisk + 1);
                    const ui32 pdiskId = options.DuplicatePDisk
                                         ? 1
                                         : oneNodePerRealm
                                           ? domain * options.GroupShape.VDisks + vdisk + 1
                                           : 1;
                    config << "            - node_id: host-" << nodeId << ':' << 19000 + nodeId << '\n';
                    if (!options.OmitPDiskIds) {
                        config << "              pdisk_id: " << pdiskId << '\n';
                    }
                }
            }
        }
        return config;
    }

    NFyaml::TDocument MakeDocument(TConfigOptions options = {}) {
        return NFyaml::TDocument::Parse(MakeConfig(options));
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(StaticGroupLayout) {

    Y_UNIT_TEST(ThreeNodePlacementWithoutGeometryIsIncorrect) {
        auto config = MakeDocument({
            .Placement = EPlacement::OneNodePerRealm,
            .PoolGeometry = EPoolGeometry::Missing,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(NineNodePlacementWithoutGeometryUsesMirror3dcDefaults) {
        auto config = MakeDocument({.PoolGeometry = EPoolGeometry::Missing});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(FailDomainTypeDoesNotOverrideMissingStoragePoolGeometry) {
        auto config = MakeDocument({
            .Placement = EPlacement::OneNodePerRealm,
            .PoolGeometry = EPoolGeometry::Missing,
        });
        NYamlConfig::SetDiskFailDomainType(config);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(Mirror3dc3NodesConfigurationAccepted) {
        auto config = MakeDocument({
            .Placement = EPlacement::OneNodePerRealm,
            .PoolGeometry = EPoolGeometry::Disk,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc3Nodes);
    }

    Y_UNIT_TEST(Mirror3dc3NodesRejectsDuplicatePDisk) {
        auto config = MakeDocument({
            .Placement = EPlacement::OneNodePerRealm,
            .PoolGeometry = EPoolGeometry::Disk,
            .DuplicatePDisk = true,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(MissingPDiskIdsAreInferred) {
        auto config = MakeDocument({
            .Placement = EPlacement::OneNodePerRealm,
            .PoolGeometry = EPoolGeometry::Disk,
            .OmitPDiskIds = true,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc3Nodes);
    }

    Y_UNIT_TEST(Mirror3dcConfigurationAccepted) {
        auto config = MakeDocument();
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(ExplicitDefaultGeometryShapeAccepted) {
        auto config = MakeDocument({.GeometryShape = TGroupShape{}});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(MultiplePhysicalRealmsPerRingRejected) {
        auto config = MakeDocument({.Placement = EPlacement::SplitRealms});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(PhysicalRealmCannotBeSharedByDifferentRings) {
        auto config = MakeDocument({.Placement = EPlacement::SharedRealms});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(GroupCannotSpanMultipleRealmGroups) {
        auto config = MakeDocument({.SplitRealmGroups = true});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(DiskFailDomainTypeDoesNotOverrideDefaultStoragePoolGeometry) {
        auto config = MakeDocument({.PoolGeometry = EPoolGeometry::Missing});
        NYamlConfig::SetDiskFailDomainType(config);
        UNIT_ASSERT(NYamlConfig::HasDiskFailDomainType(config));
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(GeometryMustMatchPlacement) {
        auto config = MakeDocument({.Placement = EPlacement::OneNodePerRealm});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(RequiresThreeRings) {
        auto config = MakeDocument({.GroupShape = {.Rings = 1}});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(RequiresThreeFailDomainsPerRing) {
        auto config = MakeDocument({.GroupShape = {.FailDomains = 1}});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(RequiresOneVDiskPerFailDomain) {
        auto config = MakeDocument({.GroupShape = {.VDisks = 2}});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(GeometryShapeMustMatchGroup) {
        auto config = MakeDocument({.GeometryShape = TGroupShape{.Rings = 4}});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(NonDefaultGeometryShapeIsNotMigrationSafe) {
        const TGroupShape shape{.Rings = 4, .VDisks = 2};
        auto config = MakeDocument({
            .GroupShape = shape,
            .GeometryShape = shape,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(StoragePoolErasureDoesNotSelectStaticGroupLayout) {
        auto config = MakeDocument({.PoolErasure = "block-4-2"});
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(Block42ConfigurationAccepted) {
        auto config = MakeDocument({
            .PoolGeometry = EPoolGeometry::Missing,
            .PoolErasure = "block-4-2",
            .GroupErasure = "block-4-2",
            .GroupShape = {.Rings = 1, .FailDomains = 8},
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Block42);
    }

    Y_UNIT_TEST(Block42ExplicitDefaultGeometryAccepted) {
        const TGroupShape shape{.Rings = 1, .FailDomains = 8};
        auto config = MakeDocument({
            .PoolErasure = "block-4-2",
            .GroupErasure = "block-4-2",
            .GroupShape = shape,
            .GeometryShape = shape,
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Block42);
    }

    Y_UNIT_TEST(Block42RejectsSharedRack) {
        auto config = MakeDocument({
            .PoolGeometry = EPoolGeometry::Missing,
            .DuplicateRack = true,
            .PoolErasure = "block-4-2",
            .GroupErasure = "block-4-2",
            .GroupShape = {.Rings = 1, .FailDomains = 8},
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(Block42RejectsWrongShape) {
        auto config = MakeDocument({
            .PoolGeometry = EPoolGeometry::Missing,
            .PoolErasure = "block-4-2",
            .GroupErasure = "block-4-2",
            .GroupShape = {.Rings = 1, .FailDomains = 7},
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(Block42DiskGeometryRequiresManualDecision) {
        auto config = MakeDocument({
            .PoolGeometry = EPoolGeometry::Disk,
            .PoolErasure = "block-4-2",
            .GroupErasure = "block-4-2",
            .GroupShape = {.Rings = 1, .FailDomains = 8},
            .GeometryShape = TGroupShape{.Rings = 1, .FailDomains = 8},
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(UnsupportedErasureRequiresManualDecision) {
        auto config = MakeDocument({
            .PoolErasure = "mirror-3of4",
            .GroupErasure = "mirror-3of4",
        });
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

}
