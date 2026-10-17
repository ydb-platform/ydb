#include <ydb/library/yaml_config/static_validator/builders.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yaml_config/validator/validator.h>
#include <ydb/library/yaml_config/validator/validator_builder.h>
#include <util/string/builder.h>

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

    Y_UNIT_TEST(HarmonizerNeedyCpuWindowSeconds) {
        auto validator = TMapBuilder()
            .Field("actor_system_config", ActorSystemConfigBuilder())
            .CreateValidator();
        auto makeConfig = [](ui32 windowSeconds, TStringBuf executorType) {
            return ::TStringBuilder()
                << "actor_system_config:\n"
                << "  executor:\n"
                << "  - name: User\n"
                << "    threads: 1\n"
                << "    max_threads: 2\n"
                << "    harmonizer_needy_cpu_window_seconds: " << windowSeconds << "\n"
                << "    type: " << executorType << "\n"
                << "  scheduler:\n"
                << "    progress_threshold: 10000\n"
                << "    resolution: 64\n"
                << "    spin_threshold: 0\n";
        };

        for (ui32 windowSeconds : {1, 30, 32}) {
            UNIT_ASSERT_C(Valid(validator.Validate(makeConfig(windowSeconds, "BASIC"))), "window: " << windowSeconds);
        }
        for (ui32 windowSeconds : {0, 33}) {
            UNIT_ASSERT_C(!validator.Validate(makeConfig(windowSeconds, "BASIC")).Ok(), "window: " << windowSeconds);
        }
        UNIT_ASSERT(!validator.Validate(makeConfig(30, "IO")).Ok());
    }

    Y_UNIT_TEST(ExecutorReferences) {
        auto validator = TMapBuilder()
            .Field("actor_system_config", ActorSystemConfigBuilder())
            .CreateValidator();

        auto makeConfig = [](TStringBuf executorReferences) {
            return ::TStringBuilder()
                << "actor_system_config:\n"
                << "  executor:\n"
                << "  - name: System\n"
                << "    threads: 1\n"
                << "    type: BASIC\n"
                << executorReferences
                << "  scheduler:\n"
                << "    progress_threshold: 10000\n"
                << "    resolution: 64\n"
                << "    spin_threshold: 0\n";
        };

        UNIT_ASSERT(Valid(validator.Validate(makeConfig(
            "  sys_executor: 0\n"
            "  user_executor: 0\n"
            "  io_executor: 0\n"
            "  batch_executor: 0\n"
            "  service_executor:\n"
            "  - service_name: Interconnect\n"
            "    executor_id: 0\n"))));

        for (TStringBuf field : {"sys_executor", "user_executor", "io_executor", "batch_executor"}) {
            UNIT_ASSERT_C(!validator.Validate(makeConfig(
                ::TStringBuilder() << "  " << field << ": 1\n")).Ok(), field);
        }

        UNIT_ASSERT(HasOnlyThisIssues(validator.Validate(makeConfig(
            "  service_executor:\n"
            "  - service_name: Interconnect\n"
            "    executor_id: 1\n")), {{
                "/actor_system_config",
                "Check \"Executor references\" failed: service_executor[0].executor_id "
                "must refer to an existing executor (got 1, executor count is 1)"
            }}));

        UNIT_ASSERT(HasOnlyThisIssues(validator.Validate(makeConfig(
            "  sys_executor: 256\n")), {{
                "/actor_system_config/sys_executor",
                "Value must be less or equal to max value(i.e <= 255)"
            }}));

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "  service_executor:\n"
            "  - service_name: Interconnect\n"
            "    executor_id: 256\n")).Ok());
    }

    Y_UNIT_TEST(EnabledSharedThreadsRequiresAutoConfig) {
        auto validator = TMapBuilder()
            .Field("actor_system_config", ActorSystemConfigBuilder())
            .CreateValidator();

        auto autoConfig =
            "actor_system_config:\n"
            "  use_auto_config: true\n"
            "  use_shared_threads: true\n"
            "  node_type: COMPUTE\n"
            "  cpu_count: 2\n";
        UNIT_ASSERT(Valid(validator.Validate(autoConfig)));

        auto manualConfigWithSharedThreadsDisabled =
            "actor_system_config:\n"
            "  use_shared_threads: false\n"
            "  executor:\n"
            "  - name: System\n"
            "    threads: 1\n"
            "    type: BASIC\n"
            "  scheduler:\n"
            "    progress_threshold: 10000\n"
            "    resolution: 64\n"
            "    spin_threshold: 0\n";
        UNIT_ASSERT(Valid(validator.Validate(manualConfigWithSharedThreadsDisabled)));

        auto manualConfig =
            "actor_system_config:\n"
            "  use_shared_threads: true\n"
            "  executor:\n"
            "  - name: System\n"
            "    threads: 1\n"
            "    type: BASIC\n"
            "  scheduler:\n"
            "    progress_threshold: 10000\n"
            "    resolution: 64\n"
            "    spin_threshold: 0\n";
        UNIT_ASSERT(HasOnlyThisIssues(validator.Validate(manualConfig), {{
            "/actor_system_config",
            "Check \"Must either be auto config or manual config\" failed: "
            "use_shared_threads must not be enabled when not using auto config"
        }}));
    }

    Y_UNIT_TEST(ExecutorPlacementAndBlobStorageSelection) {
        auto validator = TMapBuilder()
            .Field("actor_system_config", ActorSystemConfigBuilder())
            .CreateValidator();
        auto makeConfig = [](TStringBuf name, TStringBuf executorFields, TStringBuf actorSystemFields = {}) {
            return ::TStringBuilder()
                << "actor_system_config:\n"
                << "  executor:\n"
                << "  - name: " << name << "\n"
                << executorFields
                << actorSystemFields
                << "  scheduler:\n"
                << "    progress_threshold: 10000\n"
                << "    resolution: 64\n"
                << "    spin_threshold: 0\n";
        };

        UNIT_ASSERT(Valid(validator.Validate(makeConfig(
            "BS0",
            "    threads: 3\n"
            "    placement: 1\n"
            "    type: BASIC\n",
            "  blob_storage_executor: [0]\n"))));

        UNIT_ASSERT(Valid(validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    affinity:\n"
            "      cpu_list: 0-1\n"
            "      exclude_cpu_list: 1\n"
            "    type: BASIC\n"))));

        UNIT_ASSERT(Valid(validator.Validate(makeConfig(
            "IO",
            "    threads: 1\n"
            "    type: IO\n",
            "  blob_storage_executor: [0]\n"))));

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    type: BASIC\n",
            "  blob_storage_executor: 0\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    type: BASIC\n",
            "  blob_storage_executor: [1]\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    type: BASIC\n",
            "  blob_storage_executor: [0, 0]\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "IO",
            "    threads: 1\n"
            "    placement: 0\n"
            "    type: IO\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "BS",
            "    placement: 0\n"
            "    type: BASIC\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    placement: -1\n"
            "    type: BASIC\n")).Ok());

        UNIT_ASSERT(HasOnlyThisIssues(validator.Validate(makeConfig(
            "BS",
            "    threads: 1\n"
            "    placement: 0\n"
            "    affinity:\n"
            "      cpu_list: 0-1\n"
            "    type: BASIC\n")), {{
                "/actor_system_config/executor/0",
                "Check \"Executor placement settings\" failed: executor must not define both affinity and placement"
            }}));

    }

    Y_UNIT_TEST(InterconnectSessionExecutor) {
        auto validator = TMapBuilder()
            .Field("actor_system_config", ActorSystemConfigBuilder())
            .CreateValidator();
        auto makeManualConfig = [](TStringBuf actorSystemFields) {
            return ::TStringBuilder()
                << "actor_system_config:\n"
                << actorSystemFields
                << "  scheduler:\n"
                << "    progress_threshold: 10000\n"
                << "    resolution: 64\n"
                << "    spin_threshold: 0\n";
        };

        UNIT_ASSERT(Valid(validator.Validate(makeManualConfig(
            "  executor:\n"
            "  - name: System\n"
            "    threads: 1\n"
            "    type: BASIC\n"
            "  - name: ICSession0\n"
            "    threads: 1\n"
            "    placement: 0\n"
            "    type: BASIC\n"
            "  - name: ICSession1\n"
            "    threads: 1\n"
            "    placement: 1\n"
            "    type: BASIC\n"
            "  sys_executor: 0\n"
            "  use_shared_threads: false\n"
            "  interconnect_session_executor: [1, 2]\n"))));

        UNIT_ASSERT(!validator.Validate(makeManualConfig(
            "  executor:\n"
            "  - name: System\n"
            "    threads: 1\n"
            "    type: BASIC\n"
            "  interconnect_session_executor: [1]\n")).Ok());

        UNIT_ASSERT(!validator.Validate(makeManualConfig(
            "  executor:\n"
            "  - name: System\n"
            "    threads: 1\n"
            "    type: BASIC\n"
            "  interconnect_session_executor: [0, 0]\n")).Ok());

        UNIT_ASSERT(!validator.Validate(
            "actor_system_config:\n"
            "  use_auto_config: true\n"
            "  node_type: STORAGE\n"
            "  cpu_count: 4\n"
            "  interconnect_session_executor: [0]\n").Ok());
    }
}

} // namesapce NKikimr
