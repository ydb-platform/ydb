#include <ydb/library/yaml_config/public/migration/config_migration.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NKikimr;

namespace {

    enum class EMirror3dcPlacement {
        ThreeNodes,
        NineNodes,
    };

    enum class EPoolGeometry {
        Missing,
        Mirror3dc,
        Mirror3dc3Nodes,
    };

    TString MakeConfig(EMirror3dcPlacement placement, EPoolGeometry poolGeometry, bool duplicatePDisk = false,
                       TStringBuf poolErasure = "mirror-3-dc", TStringBuf groupErasure = "mirror-3-dc") {
        TStringBuilder config;
        config << "config:\n"
               << "  hosts:\n";

        const ui32 nodeCount = placement == EMirror3dcPlacement::ThreeNodes ? 3 : 9;
        for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            const ui32 dataCenter = placement == EMirror3dcPlacement::ThreeNodes
                                    ? nodeId
                                    : (nodeId - 1) / 3 + 1;
            config << "  - node_id: " << nodeId << '\n'
                   << "    host: host-" << nodeId << '\n'
                   << "    port: " << 19000 + nodeId << '\n'
                   << "    location:\n"
                   << "      data_center: dc-" << dataCenter << '\n'
                   << "      rack: rack-" << nodeId << '\n';
        }

        config << "  domains_config:\n"
               << "    domain:\n"
               << "    - storage_pool_types:\n"
               << "      - pool_config:\n"
               << "          erasure_species: " << poolErasure << '\n'
               << "          kind: ssd\n";
        if (poolGeometry != EPoolGeometry::Missing) {
            config << "          geometry:\n"
                   << "            realm_level_begin: 10\n"
                   << "            realm_level_end: 20\n"
                   << "            domain_level_begin: 10\n"
                   << "            domain_level_end: "
                   << (poolGeometry == EPoolGeometry::Mirror3dc3Nodes ? 256 : 40) << '\n';
        }

        config << "  self_management_config:\n"
               << "    enabled: false\n"
               << "  blob_storage_config:\n"
               << "    service_set:\n"
               << "      groups:\n"
               << "      - erasure_species: " << groupErasure << '\n'
               << "        rings:\n";
        for (ui32 realm = 0; realm < 3; ++realm) {
            config << "        - fail_domains:\n";
            for (ui32 domain = 0; domain < 3; ++domain) {
                const ui32 nodeId = placement == EMirror3dcPlacement::ThreeNodes
                                    ? realm + 1
                                    : realm * 3 + (duplicatePDisk ? 1 : domain + 1);
                const ui32 pdiskId = placement == EMirror3dcPlacement::ThreeNodes && !duplicatePDisk
                                     ? domain + 1
                                     : 1;
                config << "          - vdisk_locations:\n"
                       << "            - node_id: host-" << nodeId << ':' << 19000 + nodeId << '\n'
                       << "              pdisk_id: " << pdiskId << '\n';
            }
        }
        return config;
    }

    NFyaml::TDocument MakeDocument(EMirror3dcPlacement placement, EPoolGeometry poolGeometry,
                                  bool duplicatePDisk = false,
                                  TStringBuf poolErasure = "mirror-3-dc",
                                  TStringBuf groupErasure = "mirror-3-dc") {
        return NFyaml::TDocument::Parse(MakeConfig(placement, poolGeometry, duplicatePDisk, poolErasure,
                                                  groupErasure));
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(StaticGroupLayout) {

    Y_UNIT_TEST(ThreeNodePlacementWithoutGeometryIsIncorrect) {
        auto config = MakeDocument(EMirror3dcPlacement::ThreeNodes, EPoolGeometry::Missing);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(NineNodePlacementWithoutGeometryIsIncorrect) {
        auto config = MakeDocument(EMirror3dcPlacement::NineNodes, EPoolGeometry::Missing);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(FailDomainTypeDoesNotOverrideMissingStoragePoolGeometry) {
        auto config = MakeDocument(EMirror3dcPlacement::ThreeNodes, EPoolGeometry::Missing);
        NYamlConfig::SetDiskFailDomainType(config);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(Mirror3dc3NodesConfigurationAccepted) {
        auto config = MakeDocument(EMirror3dcPlacement::ThreeNodes, EPoolGeometry::Mirror3dc3Nodes);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc3Nodes);
    }

    Y_UNIT_TEST(Mirror3dc3NodesRejectsDuplicatePDisk) {
        auto config = MakeDocument(EMirror3dcPlacement::ThreeNodes, EPoolGeometry::Mirror3dc3Nodes, true);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(Mirror3dcConfigurationAccepted) {
        auto config = MakeDocument(EMirror3dcPlacement::NineNodes, EPoolGeometry::Mirror3dc);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(GeometryMustMatchPlacement) {
        auto config = MakeDocument(EMirror3dcPlacement::ThreeNodes, EPoolGeometry::Mirror3dc);
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Incorrect);
    }

    Y_UNIT_TEST(StoragePoolErasureDoesNotSelectStaticGroupLayout) {
        auto config = MakeDocument(
            EMirror3dcPlacement::NineNodes, EPoolGeometry::Mirror3dc, false, "block-4-2", "mirror-3-dc");
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::Mirror3dc);
    }

    Y_UNIT_TEST(OtherErasureIsNotApplicable) {
        auto config = MakeDocument(
            EMirror3dcPlacement::NineNodes, EPoolGeometry::Mirror3dc, false, "block-4-2", "block-4-2");
        UNIT_ASSERT(NYamlConfig::CheckStaticGroupLayout(config)
                    == NYamlConfig::EStaticGroupLayoutCheckResult::NotApplicable);
    }

}
