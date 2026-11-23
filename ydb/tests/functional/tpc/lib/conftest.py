import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper


class FunctionalTestBase:
    cluster = None

    @classmethod
    def setup_cluster(cls, table_service_config: dict = {}, memory_controller_config: dict = {}) -> None:
        cls.setup_cluster_ext(
            table_service_config=table_service_config,
            memory_controller_config=memory_controller_config,
        )

    @classmethod
    def setup_cluster_ext(cls,
                          table_service_config: dict = {},
                          memory_controller_config: dict = {},
                          extra_feature_flags: list[str] | None = None,
                          query_service_config: dict | None = None,
                          actor_system_config: dict | None = None,
                          static_nodes: int | None = None,
                          dynamic_nodes: int = 1,
                          erasure=None,
                          use_in_memory_pdisks: bool = True,
                          binary_paths: list[str] | None = None,
                          pdisk_store_path: str | None = None,
                          pdisk_size: int | None = None,
                          static_pdisk_config: dict | None = None,
                          storage_pool_erasure=None) -> None:
        flags = [
            "enable_resource_pools",
            "enable_resource_pools_counters",
            "enable_table_pg_types",
            "enable_forced_compactions",
        ]
        if extra_feature_flags:
            flags.extend(extra_feature_flags)
        disk_options = {} if pdisk_size is None else {"static_pdisk_size": pdisk_size}
        config_generator = KikimrConfigGenerator(
            domain_name='local',
            extra_feature_flags=flags,
            nodes=static_nodes,
            erasure=erasure,
            binary_paths=binary_paths,
            pdisk_store_path=pdisk_store_path,
            static_pdisk_config=static_pdisk_config,
            use_in_memory_pdisks=use_in_memory_pdisks,
            column_shard_config={
                "alter_object_enabled": True,
            },
            overrided_actor_system_config=actor_system_config,
            **disk_options,
        )
        # Override every pool kind, including those not requested by the current database.
        if storage_pool_erasure is not None:
            for domain in config_generator.yaml_config["domains_config"]["domain"]:
                for pool in domain.get("storage_pool_types", []):
                    pool["pool_config"]["erasure_species"] = str(storage_pool_erasure)
        if table_service_config:
            config_generator.yaml_config["table_service_config"] = table_service_config

        if memory_controller_config:
            config_generator.yaml_config["memory_controller_config"] = memory_controller_config

        if query_service_config:
            config_generator.yaml_config["query_service_config"] = query_service_config

        cls.cluster = KiKiMR(configurator=config_generator)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        YdbCluster.reset(
            ydb_endpoint=f'grpc://{node.host}:{node.grpc_port}',
            ydb_database=f'{cls.cluster.domain_name}/test_db',
            ydb_mon_port=node.mon_port,
            dyn_nodes_count=dynamic_nodes
        )
        db = f'/{YdbCluster.ydb_database}'
        cls.cluster.create_database(
            db,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(db, count=YdbCluster.get_dyn_nodes_count())
        cls.cluster.wait_tenant_up(db)

    def setup_method(self, method):
        for node in self.cluster.nodes.values():
            node.start()
        for slot in self.cluster.slots.values():
            slot.start()
        self.cluster.wait_tenant_up(f'/{YdbCluster.ydb_database}')

    def teardown_method(self, method):
        for node in self.cluster.nodes.values():
            if not node.is_alive():
                node.stop()
        for slot in self.cluster.slots.values():
            if not slot.is_alive():
                slot.stop()

    @classmethod
    def run_cli(cls, argv: list[str]) -> yatest.common.process._Execution:
        return yatest.common.execute(YdbCliHelper.get_cli_command() + argv)

    @classmethod
    def do_teardown_class(cls):
        cls.cluster.stop()
