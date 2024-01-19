#include <ydb/library/yaml_config/static_validator/builders.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yaml_config/validator/validator.h>
#include <ydb/library/yaml_config/validator/validator_builder.h>

namespace NKikimr {

using namespace NYamlConfig::NValidator;
using TIssue = TValidationResult::TIssue;

bool HasOnlyThisIssues(TValidationResult result, TVector<TIssue> issues) {
    if (result.Issues.size() != issues.size()) {
        Cerr << "Issue counts are differend. List of actul issues:" << Endl;
        Cerr << result;
        Cerr << "------------- List of Expected Issues: " << Endl;
        Cerr << TValidationResult(issues);
        Cerr << "------------- End of issue List" << Endl;
        return false;
    }
    Sort(result.Issues);
    Sort(issues);
    for (size_t i = 0; i < issues.size(); ++i) {
        if (result.Issues[i] != issues[i]) {
            Cerr << "Issues are differend. List of actul issues:" << Endl;
            Cerr << result;
            Cerr << "------------- List of Expected Issues: " << Endl;
            Cerr << TValidationResult(issues);
            Cerr << "------------- End of issue List" << Endl;
            return false;
        }
    }
    return true;
}

bool Valid(TValidationResult result) {
    if (result.Ok()) return true;

    Cerr << "List of issues:" << Endl;
    Cerr << result;
    Cerr << "------------- End of issue list: " << Endl;
    return false;
}

Y_UNIT_TEST_SUITE(StaticValidator) {
    Y_UNIT_TEST(HostConfigs) {
        auto v = 
        TMapBuilder()
        .Field("host_configs", HostConfigBuilder())
        .CreateValidator();

        auto yaml =
        "host_configs:\n"
        "- host_config_id: 1\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n";
        
        Y_ENSURE(Valid(v.Validate(yaml)));

        yaml =
        "host_configs:\n"
        "- host_config_id: 1\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "- host_config_id: 2\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03\n"
        "    type: SSD\n";

        Y_ENSURE(Valid(v.Validate(yaml)));

        yaml =
        "host_configs:\n"
        "- host_config_id: 1\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "- host_config_id: 1\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03\n"
        "    type: SSD\n";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/host_configs", "Check \"All array items, that located in \"host_config_id\" must be unique\" failed: items with indexes 0 and 1 are conflicting"}
        }));

        yaml =
        "host_configs:\n"
        "- host_config_id: 1\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "- host_config_id: 2\n"
        "  drive:\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02\n"
        "    type: SSD\n"
        "  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01\n"
        "    type: SSD\n";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/host_configs/1/drive", "Check \"All array items, that located in \"path\" must be unique\" failed: items with indexes 0 and 2 are conflicting"}
        }));
    }
    
    Y_UNIT_TEST(Hosts) {
        auto v = 
        TMapBuilder()
        .Field("hosts", HostsBuilder())
        .CreateValidator();

        auto yaml =
        "hosts:\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n"
        "- host: hostname2\n"
        "  host_config_id: 1\n"
        "  node_id: 2\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n";
        
        Y_ENSURE(Valid(v.Validate(yaml)));

        yaml =
        "hosts:\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 2\n"
        "  port: 19002\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n";
        
        Y_ENSURE(Valid(v.Validate(yaml)));

        yaml =
        "hosts:\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 2\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n";
        
        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/hosts", "Check \"Must not have two hosts with same host name and port\" failed: items with indexes 0 and 1 are conflicting"}
        }));

        yaml =
        "hosts:\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/hosts", "Check \"All array items, that located in \"node_id\" must be unique\" failed: items with indexes 0 and 1 are conflicting"},
            {"/hosts", "Check \"Must not have two hosts with same host name and port\" failed: items with indexes 0 and 1 are conflicting"}
        }));

        yaml =
        "hosts:\n"
        "- host: hostname1\n"
        "  host_config_id: 1\n"
        "  node_id: 1\n"
        "  port: 19001\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n"
        "- host: hostname2\n"
        "  host_config_id: 1\n"
        "  node_id: 2\n"
        "  port: 65536\n"
        "  location:\n"
        "    unit: '1'\n"
        "    data_center: '1'\n"
        "    rack: '1'\n";
        
        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/hosts/1/port", "Value must be less or equal to max value(i.e <= 65535)"}
        }));
    }

    Y_UNIT_TEST(DomainsConfig) {
        auto v = 
        TMapBuilder()
        .Field("domains_config", DomainsConfigBuilder())
        .CreateValidator();

        auto yaml =
        "domains_config:\n"
        "  domain:\n"
        "  - name: Root\n"
        "    storage_pool_types:\n"
        "    - kind: ssd\n"
        "      pool_config:\n"
        "        box_id: 1\n"
        "        erasure_species: block-4-2\n"
        "        kind: ssd\n"
        "        pdisk_filter:\n"
        "        - property:\n"
        "          - type: SSD\n"
        "        vdisk_kind: Default\n"
        "  state_storage:\n"
        "  - ring:\n"
        "      node: [1, 2, 3, 4, 5, 6, 7, 8, 9]\n"
        "      nto_select: 9\n"
        "    ssid: 1\n"
        "  security_config:\n"
        "    enforce_user_token_requirement: true\n";
        
        Y_ENSURE(Valid(v.Validate(yaml)));
        
        yaml =
        "domains_config:\n"
        "  domain:\n"
        "  - name: Root\n"
        "    storage_pool_types:\n"
        "    - kind: ssd\n"
        "      pool_config:\n"
        "        box_id: 1\n"
        "        erasure_species: block-4-2\n"
        "        kind: aaaaaaaaa\n"
        "        pdisk_filter:\n"
        "        - property:\n"
        "          - type: SSD\n"
        "        vdisk_kind: Default\n"
        "  state_storage:\n"
        "  - ring:\n"
        "      node: [1, 2, 3, 4, 5, 6, 7, 8, 9]\n"
        "      nto_select: 9\n"
        "    ssid: 1\n";
        
        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/domains_config/domain/0/storage_pool_types/0", "Fields with paths kind and pool_config/kind must be equal"}
        }));
        
        yaml =
        "domains_config:\n"
        "  domain:\n"
        "  - name: Root\n"
        "    storage_pool_types:\n"
        "    - kind: ssd\n"
        "      pool_config:\n"
        "        box_id: 1\n"
        "        erasure_species: block-4-2\n"
        "        kind: ssd\n"
        "        pdisk_filter:\n"
        "        - property:\n"
        "          - type: SSD\n"
        "        vdisk_kind: Default\n"
        "  state_storage:\n"
        "  - ring:\n"
        "      node: [1, 2, 3, 4, 5, 6, 7, 8]\n"
        "      nto_select: 9\n"
        "    ssid: 1\n";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/domains_config/state_storage/0/ring", "nto_select must not be greater, than node array size"}
        }));
    }
}

} // namesapce NKikimr
